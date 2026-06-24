"""Tests for install_sql: handler selection, op name, dialect/paramstyle."""
import pytest
from j_perm import build_default_engine

from j_perm_sql import RenderOptions, install_sql
from j_perm_sql.handler import AsyncSqlHandler, SqlHandler


def _registered_handler(engine, op="sql"):
    node = engine.main_pipeline.registry.resolve({"op": op})[0]
    return node


class TestHandlerSelection:
    def test_sync_executor_registers_sync_handler(self):
        engine = build_default_engine()
        install_sql(engine, lambda sql, params: None)
        assert isinstance(_registered_handler(engine), SqlHandler)

    def test_async_executor_registers_async_handler(self):
        engine = build_default_engine()

        async def executor(sql, params):
            return None

        install_sql(engine, executor)
        assert isinstance(_registered_handler(engine), AsyncSqlHandler)

    def test_returns_engine(self):
        engine = build_default_engine()
        assert install_sql(engine, lambda s, p: None) is engine


class TestEndToEnd:
    def test_sync_apply(self):
        engine = build_default_engine()
        calls = []
        install_sql(engine, lambda s, p: calls.append((s, p)) or [{"id": 1}])
        out = engine.apply(
            {"op": "sql", "to": "/rows", "query": {"$select": {"from": {"table": "t"}}}},
            source={}, dest={},
        )
        assert out == {"rows": [{"id": 1}]}
        assert calls[0] == ('SELECT * FROM "t"', [])

    @pytest.mark.asyncio
    async def test_async_apply(self):
        engine = build_default_engine()
        calls = []

        async def executor(sql, params):
            calls.append((sql, params))
            return [{"id": 2}]

        install_sql(engine, executor)
        out = await engine.apply_async(
            {"op": "sql", "to": "/rows", "query": {"$select": {"from": {"table": "t"}}}},
            source={}, dest={},
        )
        assert out == {"rows": [{"id": 2}]}

    def test_custom_op_name(self):
        engine = build_default_engine()
        install_sql(engine, lambda s, p: "ok", op="query")
        assert isinstance(_registered_handler(engine, "query"), SqlHandler)


class TestDialectAndParamstyle:
    def test_paramstyle(self):
        engine = build_default_engine()
        calls = []
        install_sql(engine, lambda s, p: calls.append((s, p)), paramstyle="numeric")
        engine.apply(
            {"op": "sql", "query": {"$select": {"from": {"table": "t"},
                                                "where": {"$eq": [{"$col": "id"}, {"$val": 1}]}}}},
            source={}, dest={},
        )
        assert calls[0][0] == 'SELECT * FROM "t" WHERE "id" = $1'

    def test_dialect_overrides_paramstyle(self):
        engine = build_default_engine()
        calls = []
        install_sql(engine, lambda s, p: calls.append((s, p)),
                    paramstyle="qmark", dialect=RenderOptions(identifier_quote="`"))
        engine.apply(
            {"op": "sql", "query": {"$select": {"columns": ["id"], "from": {"table": "t"}}}},
            source={}, dest={},
        )
        assert calls[0][0] == "SELECT `id` FROM `t`"
