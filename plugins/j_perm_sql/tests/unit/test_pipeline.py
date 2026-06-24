"""Tests for SQL pipeline assembly and isolation."""
from j_perm import ExecutionContext, build_default_engine

from j_perm_sql import RenderOptions, install_sql
from j_perm_sql.pipeline import SQL_PIPELINE_NAME, build_sql_pipeline


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


class TestIsolation:
    def test_sql_keys_inert_in_value_pipeline(self):
        """$select outside op:sql is just a plain dict (no SQL meaning)."""
        engine = build_default_engine()
        install_sql(engine, lambda sql, params: None)
        result = engine.apply({"/x": {"$select": {"columns": ["id"]}}}, source={}, dest={})
        assert result == {"x": {"$select": {"columns": ["id"]}}}
