"""Tests for the SqlRenderer and the sync/async op:sql handlers."""
import pytest
from j_perm import ExecutionContext, build_default_engine

from j_perm_sql import RenderOptions, SqlRenderer
from j_perm_sql.handler import AsyncSqlHandler, SqlHandler
from j_perm_sql.pipeline import SQL_PIPELINE_NAME, build_sql_pipeline


def _engine(opts=None):
    engine = build_default_engine()
    engine.register_pipeline(SQL_PIPELINE_NAME, build_sql_pipeline(opts))
    return engine


class TestSqlRenderer:
    def test_render_returns_sql_and_params(self):
        engine = _engine()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        sql, params = SqlRenderer(RenderOptions()).render(
            {"$select": {"columns": ["id"], "from": {"table": "t"},
                         "where": {"$eq": [{"$col": "id"}, {"$val": 5}]}}},
            ctx,
        )
        assert sql == 'SELECT "id" FROM "t" WHERE "id" = ?'
        assert params == [5]

    def test_non_construct_query_raises(self):
        engine = _engine()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        with pytest.raises(ValueError, match="must be a SQL construct"):
            SqlRenderer(RenderOptions()).render("not a construct", ctx)


class TestSyncHandler:
    def test_writes_result_to_path(self):
        engine = _engine()
        calls = []
        handler = SqlHandler(lambda s, p: calls.append((s, p)) or [{"id": 1}], SqlRenderer(RenderOptions()))
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        step = {"op": "sql", "to": "/rows", "query": {"$select": {"from": {"table": "t"}}}}
        dest = handler.execute(step, ctx)
        assert dest == {"rows": [{"id": 1}]}
        assert calls[0][0] == 'SELECT * FROM "t"'

    def test_without_to_discards_result(self):
        engine = _engine()
        handler = SqlHandler(lambda s, p: "ignored", SqlRenderer(RenderOptions()))
        ctx = ExecutionContext(source={}, dest={"keep": 1}, engine=engine)
        dest = handler.execute({"op": "sql", "query": {"$select": {"from": {"table": "t"}}}}, ctx)
        assert dest == {"keep": 1}


class TestAsyncHandler:
    @pytest.mark.asyncio
    async def test_awaits_executor_and_writes(self):
        engine = _engine()
        calls = []

        async def executor(sql, params):
            calls.append((sql, params))
            return [{"id": 2}]

        handler = AsyncSqlHandler(executor, SqlRenderer(RenderOptions()))
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        step = {"op": "sql", "to": "/rows", "query": {"$select": {"from": {"table": "t"}}}}
        dest = await handler.execute(step, ctx)
        assert dest == {"rows": [{"id": 2}]}
        assert calls[0][0] == 'SELECT * FROM "t"'

    @pytest.mark.asyncio
    async def test_without_to(self):
        engine = _engine()

        async def executor(sql, params):
            return "ignored"

        handler = AsyncSqlHandler(executor, SqlRenderer(RenderOptions()))
        ctx = ExecutionContext(source={}, dest={"keep": 1}, engine=engine)
        dest = await handler.execute({"op": "sql", "query": {"$select": {"from": {"table": "t"}}}}, ctx)
        assert dest == {"keep": 1}
