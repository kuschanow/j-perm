"""End-to-end integration tests through engine.apply / apply_async."""
import pytest
from j_perm import build_default_engine, build_default_async_engine

from j_perm_sql import RenderOptions, install_sql


def _engine(**kw):
    engine = build_default_engine()
    calls = []
    install_sql(engine, lambda sql, params: calls.append((sql, params)) or [{"ok": True}], **kw)
    return engine, calls


class TestComplexQuery:
    def test_full_select(self):
        engine, calls = _engine()
        query = {"$select": {
            "columns": [
                {"$col": {"table": "u", "name": "id"}},
                {"$col": {"table": "u", "name": "name", "as": "user_name"}},
                {"$func": {"name": "COUNT", "args": [{"$col": {"table": "o", "name": "id"}}], "as": "order_count"}},
            ],
            "from": {"table": "users", "as": "u"},
            "joins": [
                {"$join": {"type": "left", "table": "orders", "as": "o",
                           "on": {"$eq": [{"$col": {"table": "o", "name": "user_id"}},
                                          {"$col": {"table": "u", "name": "id"}}]}}}
            ],
            "where": {"$and": [
                {"$gte": [{"$col": {"name": "age"}}, {"$val": 18}]},
                {"$in": [{"$col": {"name": "country"}}, ["US", "GB"]]},
            ]},
            "group_by": [{"$col": {"table": "u", "name": "id"}}, {"$col": {"table": "u", "name": "name"}}],
            "having": {"$gt": [{"$func": {"name": "COUNT", "args": [{"$col": {"table": "o", "name": "id"}}]}}, {"$val": 5}]},
            "order_by": [{"expr": {"$col": {"name": "order_count"}}, "dir": "desc"}],
            "limit": 10, "offset": 0,
        }}
        out = engine.apply({"op": "sql", "to": "/result", "query": query}, source={}, dest={})
        sql, params = calls[0]
        assert sql == (
            'SELECT "u"."id", "u"."name" AS "user_name", COUNT("o"."id") AS "order_count" '
            'FROM "users" AS "u" LEFT JOIN "orders" AS "o" ON "o"."user_id" = "u"."id" '
            'WHERE ("age" >= ? AND "country" IN (?, ?)) '
            'GROUP BY "u"."id", "u"."name" HAVING COUNT("o"."id") > ? '
            'ORDER BY "order_count" DESC LIMIT 10 OFFSET 0'
        )
        assert params == [18, "US", "GB", 5]
        assert out == {"result": [{"ok": True}]}

    def test_values_from_source_are_bound_not_interpolated(self):
        """Injection safety: a malicious value lands in params, never in the SQL."""
        engine, calls = _engine()
        engine.apply(
            {"op": "sql", "query": {"$select": {
                "from": {"table": "t"},
                "where": {"$eq": [{"$col": "name"}, {"$val": {"$ref": "/inp"}}]},
            }}},
            source={"inp": "x'; DROP TABLE t; --"},
            dest={},
        )
        sql, params = calls[0]
        assert "DROP TABLE" not in sql
        assert sql == 'SELECT * FROM "t" WHERE "name" = ?'
        assert params == ["x'; DROP TABLE t; --"]

    def test_cte_with_window_and_subquery(self):
        engine, calls = _engine()
        query = {"$select": {
            "with": [{"name": "ranked", "query": {"$select": {
                "columns": [
                    {"$col": "id"},
                    {"$func": {"name": "ROW_NUMBER", "args": [],
                               "over": {"partition_by": [{"$col": "dept"}],
                                        "order_by": [{"expr": {"$col": "salary"}, "dir": "desc"}]},
                               "as": "rn"}},
                ],
                "from": {"table": "emp"},
            }}}],
            "columns": ["id"],
            "from": {"table": "ranked"},
            "where": {"$eq": [{"$col": "rn"}, {"$val": 1}]},
        }}
        engine.apply({"op": "sql", "query": query}, source={}, dest={})
        sql, params = calls[0]
        assert sql == (
            'WITH "ranked" AS (SELECT "id", ROW_NUMBER() OVER (PARTITION BY "dept" '
            'ORDER BY "salary" DESC) AS "rn" FROM "emp") '
            'SELECT "id" FROM "ranked" WHERE "rn" = ?'
        )
        assert params == [1]


class TestAsyncIntegration:
    @pytest.mark.asyncio
    async def test_async_executor(self):
        engine = build_default_engine()
        calls = []

        async def executor(sql, params):
            calls.append((sql, params))
            return [{"n": 1}]

        install_sql(engine, executor, paramstyle="format")
        out = await engine.apply_async(
            {"op": "sql", "to": "/rows", "query": {"$select": {
                "from": {"table": "t"},
                "where": {"$eq": [{"$col": "id"}, {"$val": 7}]},
            }}},
            source={}, dest={},
        )
        assert out == {"rows": [{"n": 1}]}
        assert calls[0] == ('SELECT * FROM "t" WHERE "id" = %s', [7])

    @pytest.mark.asyncio
    async def test_async_engine_embedded_ref_in_foreach(self):
        """On the async engine the value pipeline is async; an embedded ``$ref``
        inside a ``$val`` must resolve via the restart protocol (no leaked
        coroutine), including inside an async ``foreach`` body."""
        engine = build_default_async_engine()
        calls = []

        async def executor(sql, params):
            calls.append((sql, list(params)))
            return [{"id": params[0]}]

        install_sql(engine, executor)
        query = {"$select": {
            "columns": [{"$col": {"name": "id"}}],
            "from": {"table": "u"},
            "where": {"$eq": [{"$col": {"name": "id"}}, {"$val": {"$ref": "&:/id"}}]},
        }}
        out = await engine.apply_async(
            {"op": "foreach", "in": "/ids", "as": "id", "do": [
                {"op": "sql", "query": query, "to": "/rows/-"}]},
            source={"ids": [10, 20]}, dest={"rows": []},
        )
        assert [p for _, p in calls] == [[10], [20]]
        assert out == {"rows": [[{"id": 10}], [{"id": 20}]]}

    @pytest.mark.asyncio
    async def test_async_engine_parallel_foreach(self):
        engine = build_default_async_engine()

        async def executor(sql, params):
            return [{"id": params[0]}]

        install_sql(engine, executor)
        query = {"$select": {
            "columns": [{"$col": {"name": "id"}}],
            "from": {"table": "u"},
            "where": {"$in": [{"$col": {"name": "id"}}, {"$ref": "&:/ids"}]},
        }}
        # parallel iterations run on isolated fresh dests; results fold in via
        # ordered deep_merge (list-concat), so each writes its own /out list.
        out = await engine.apply_async(
            {"op": "foreach", "in_value": [[1, 2], [3]], "as": "ids", "parallel": True,
             "do": [{"op": "sql", "query": query, "to": "/out"}]},
            source={}, dest={},
        )
        assert out == {"out": [{"id": 1}, {"id": 3}]}
