"""End-to-end integration tests for DML through engine.apply / apply_async."""
import pytest
from j_perm import build_default_engine

from j_perm_sql import install_sql_write


def _engine(**kw):
    engine = build_default_engine()
    calls = []
    install_sql_write(engine, lambda sql, params: calls.append((sql, params)) or {"rowcount": 1}, **kw)
    return engine, calls


class TestInsert:
    def test_multi_row_insert(self):
        engine, calls = _engine()
        out = engine.apply(
            {"op": "sql_write", "to": "/res", "query": {"$insert": {
                "into": {"table": "users", "schema": "app"},
                "columns": ["name", "age"],
                "values": [[{"$val": "Ann"}, {"$val": 30}],
                           [{"$val": "Bob"}, {"$val": 41}]],
            }}},
            source={}, dest={},
        )
        sql, params = calls[0]
        assert sql == 'INSERT INTO "app"."users" ("name", "age") VALUES (?, ?), (?, ?)'
        assert params == ["Ann", 30, "Bob", 41]
        assert out == {"res": {"rowcount": 1}}

    def test_insert_select(self):
        engine, calls = _engine()
        engine.apply(
            {"op": "sql_write", "query": {"$insert": {
                "into": "archive",
                "columns": ["id"],
                "query": {"$select": {"columns": ["id"], "from": {"table": "users"},
                                      "where": {"$lt": [{"$col": "age"}, {"$val": 18}]}}},
            }}},
            source={}, dest={},
        )
        sql, params = calls[0]
        assert sql == 'INSERT INTO "archive" ("id") SELECT "id" FROM "users" WHERE "age" < ?'
        assert params == [18]


class TestUpdate:
    def test_update_with_where_and_expression(self):
        engine, calls = _engine()
        engine.apply(
            {"op": "sql_write", "query": {"$update": {
                "table": "users",
                "set": {"visits": {"$add": [{"$col": "visits"}, {"$val": 1}]},
                        "name": {"$val": "Bob"}},
                "where": {"$eq": [{"$col": "id"}, {"$val": 5}]},
            }}},
            source={}, dest={},
        )
        sql, params = calls[0]
        assert sql == 'UPDATE "users" SET "visits" = ("visits" + ?), "name" = ? WHERE "id" = ?'
        assert params == [1, "Bob", 5]


class TestDelete:
    def test_delete_with_where(self):
        engine, calls = _engine()
        engine.apply(
            {"op": "sql_write", "query": {"$delete": {
                "from": "sessions",
                "where": {"$lt": [{"$col": "last_seen"}, {"$val": "2020-01-01"}]},
            }}},
            source={}, dest={},
        )
        sql, params = calls[0]
        assert sql == 'DELETE FROM "sessions" WHERE "last_seen" < ?'
        assert params == ["2020-01-01"]


class TestSafety:
    def test_values_are_bound_not_interpolated(self):
        """Injection safety: a malicious value lands in params, never in the SQL."""
        engine, calls = _engine()
        engine.apply(
            {"op": "sql_write", "query": {"$update": {
                "table": "t",
                "set": {"note": {"$val": {"$ref": "/inp"}}},
                "where": {"$eq": [{"$col": "id"}, {"$val": 1}]},
            }}},
            source={"inp": "x'; DROP TABLE t; --"}, dest={},
        )
        sql, params = calls[0]
        assert "DROP TABLE" not in sql
        assert sql == 'UPDATE "t" SET "note" = ? WHERE "id" = ?'
        assert params == ["x'; DROP TABLE t; --", 1]

    def test_value_from_source_ref_lands_in_params(self):
        """Parameters are sourced from inside j-perm (no external param source)."""
        engine, calls = _engine()
        engine.apply(
            {"op": "sql_write", "query": {"$insert": {
                "into": "t", "values": [[{"$val": {"$ref": "/payload/value"}}]]}}},
            source={"payload": {"value": 99}}, dest={},
        )
        assert calls[0] == ('INSERT INTO "t" VALUES (?)', [99])

    def test_update_without_where_or_all_rejected(self):
        engine, calls = _engine()
        with pytest.raises(ValueError, match='"all": true'):
            engine.apply(
                {"op": "sql_write", "query": {"$update": {
                    "table": "t", "set": {"x": {"$val": 1}}}}},
                source={}, dest={},
            )
        assert calls == []


class TestAsyncIntegration:
    @pytest.mark.asyncio
    async def test_async_delete(self):
        engine = build_default_engine()
        calls = []

        async def executor(sql, params):
            calls.append((sql, params))
            return {"rowcount": 3}

        install_sql_write(engine, executor, paramstyle="named")
        out = await engine.apply_async(
            {"op": "sql_write", "to": "/res", "query": {"$delete": {
                "from": "t", "where": {"$eq": [{"$col": "id"}, {"$val": 7}]}}}},
            source={}, dest={},
        )
        assert out == {"res": {"rowcount": 3}}
        assert calls[0] == ('DELETE FROM "t" WHERE "id" = :p1', {"p1": 7})
