"""Tests for SQL pipeline assembly and isolation."""
import pytest
from j_perm import ExecutionContext, build_default_engine

from j_perm_sql import RenderOptions, install_sql, install_sql_write
from j_perm_sql.pipeline import (
    SQL_PIPELINE_NAME,
    SQL_WRITE_PIPELINE_NAME,
    build_sql_pipeline,
    build_sql_write_pipeline,
)


class TestBuildPipeline:
    def test_default_opts(self):
        pipeline = build_sql_pipeline()
        # dispatches a $select to a fragment
        engine = build_default_engine()
        engine.register_pipeline(SQL_PIPELINE_NAME, pipeline)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        frag = engine.run_pipeline(SQL_PIPELINE_NAME, {"$select": {"from": {"table": "t"}}}, ctx).dest
        assert frag["sql"] == 'SELECT * FROM "t"'

    def test_custom_opts(self):
        pipeline = build_sql_pipeline(RenderOptions(identifier_quote="`"))
        engine = build_default_engine()
        engine.register_pipeline(SQL_PIPELINE_NAME, pipeline)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        frag = engine.run_pipeline(SQL_PIPELINE_NAME, {"$col": "id"}, ctx).dest
        assert frag["sql"] == "`id`"

    def test_identity_passthrough_for_literals(self):
        # a bare string is not a construct → identity returns it unchanged
        engine = build_default_engine()
        engine.register_pipeline(SQL_PIPELINE_NAME, build_sql_pipeline())
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        assert engine.run_pipeline(SQL_PIPELINE_NAME, "plain", ctx).dest == "plain"


class TestBuildWritePipeline:
    def test_dispatches_insert_to_fragment(self):
        engine = build_default_engine()
        engine.register_pipeline(SQL_WRITE_PIPELINE_NAME, build_sql_write_pipeline())
        # recursion (the $val) must stay in the write pipeline
        ctx = ExecutionContext(source={}, dest={}, engine=engine,
                               metadata={"_sql_pipeline": SQL_WRITE_PIPELINE_NAME})
        node = {"$insert": {"into": "t", "values": [[{"$val": 1}]]}}
        frag = engine.run_pipeline(SQL_WRITE_PIPELINE_NAME, node, ctx).dest
        assert frag["sql"] == 'INSERT INTO "t" VALUES (?)'

    def test_custom_opts(self):
        engine = build_default_engine()
        engine.register_pipeline(SQL_WRITE_PIPELINE_NAME,
                                 build_sql_write_pipeline(RenderOptions(identifier_quote="`")))
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        frag = engine.run_pipeline(SQL_WRITE_PIPELINE_NAME, {"$col": "id"}, ctx).dest
        assert frag["sql"] == "`id`"


class TestIsolation:
    def test_sql_keys_inert_in_value_pipeline(self):
        """$select outside op:sql is just a plain dict (no SQL meaning)."""
        engine = build_default_engine()
        install_sql(engine, lambda sql, params: None)
        result = engine.apply({"/x": {"$select": {"columns": ["id"]}}}, source={}, dest={})
        assert result == {"x": {"$select": {"columns": ["id"]}}}

    def test_dml_keys_inert_in_value_pipeline(self):
        """$insert outside a write op is just a plain dict (no SQL meaning)."""
        engine = build_default_engine()
        install_sql_write(engine, lambda sql, params: None)
        node = {"$insert": {"into": "t", "values": [[1]]}}
        result = engine.apply({"/x": node}, source={}, dest={})
        assert result == {"x": node}

    def test_read_op_rejects_dml(self):
        """op: sql stays read-only — a DML tree is not a SQL construct there."""
        engine = build_default_engine()
        install_sql(engine, lambda sql, params: None)
        install_sql_write(engine, lambda sql, params: None)
        with pytest.raises(ValueError, match="must be a SQL construct"):
            engine.apply(
                {"op": "sql", "query": {"$insert": {"into": "t", "values": [[{"$val": 1}]]}}},
                source={}, dest={},
            )
