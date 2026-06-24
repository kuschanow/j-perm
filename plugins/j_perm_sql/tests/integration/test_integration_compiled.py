"""End-to-end tests for the *compiled* SQL path (engine.compile / apply_compiled).

The ``op: sql`` / ``op: sql_write`` handlers are :class:`~j_perm.core.Compound`,
so their ``query`` subtree is compiled against the isolated SQL pipeline and the
whole tree is rendered through the compiled path (with per-node memoisation).
"""
import pytest
from j_perm import build_default_engine

from j_perm_sql import install_sql, install_sql_write


def _engine(**kw):
    engine = build_default_engine()
    calls = []
    install_sql(engine, lambda sql, params: calls.append((sql, params)) or [{"ok": True}], **kw)
    return engine, calls


_QUERY = {"$select": {
    "columns": [{"$col": {"name": "id"}}, {"$col": {"name": "name"}}],
    "from": {"table": "users", "as": "u"},
    "where": {"$and": [
        {"$gte": [{"$col": {"name": "age"}}, {"$val": 18}]},
        {"$in": [{"$col": {"name": "country"}}, ["US", "GB"]]},
    ]},
    "order_by": [{"expr": {"$col": {"name": "id"}}, "dir": "asc"}],
    "limit": 10,
}}

_EXPECTED_SQL = (
    'SELECT "id", "name" FROM "users" AS "u" '
    'WHERE ("age" >= ? AND "country" IN (?, ?)) '
    'ORDER BY "id" ASC LIMIT 10'
)


class TestCompiledSelect:
    def test_compiled_matches_interpreted(self):
        engine, calls = _engine()
        compiled = engine.compile([{"op": "sql", "to": "/rows", "query": _QUERY}])
        out = compiled.apply(source={}, dest={})
        assert out == {"rows": [{"ok": True}]}
        assert calls[0] == (_EXPECTED_SQL, [18, "US", "GB"])

    def test_query_compiled_against_sql_pipeline(self):
        engine, _ = _engine()
        compiled = engine.compile([{"op": "sql", "query": _QUERY}])
        nested = compiled.steps[0].nested["query"]
        assert nested._pipeline is engine.get_pipeline("sql")

    def test_reuse_hits_node_cache(self):
        """Applying the same compiled spec twice reuses memoised node specs."""
        engine, calls = _engine()
        compiled = engine.compile([{"op": "sql", "query": _QUERY}])
        compiled.apply(source={}, dest={})
        nested = compiled.steps[0].nested["query"]
        cache = nested._sql_node_cache
        assert cache  # first run populated the per-node cache
        cache_size = len(cache)
        # second run must reuse the cache (no growth), and render identically
        compiled.apply(source={}, dest={})
        assert len(cache) == cache_size
        assert calls[0] == calls[1] == (_EXPECTED_SQL, [18, "US", "GB"])

    def test_compiled_binds_runtime_values(self):
        """Values still resolve per-run; only node structure is compiled."""
        engine, calls = _engine()
        query = {"$select": {
            "from": {"table": "t"},
            "where": {"$eq": [{"$col": "id"}, {"$val": {"$ref": "/wanted"}}]},
        }}
        compiled = engine.compile([{"op": "sql", "query": query}])
        compiled.apply(source={"wanted": 1}, dest={})
        compiled.apply(source={"wanted": 99}, dest={})
        assert calls[0] == ('SELECT * FROM "t" WHERE "id" = ?', [1])
        assert calls[1] == ('SELECT * FROM "t" WHERE "id" = ?', [99])


class TestCompiledWrite:
    def test_compiled_insert(self):
        engine = build_default_engine()
        calls = []
        install_sql_write(engine, lambda sql, params: calls.append((sql, params)) or {"rowcount": 1})
        query = {"$insert": {"into": "users", "columns": ["name"],
                             "values": [[{"$val": "Ann"}]]}}
        compiled = engine.compile([{"op": "sql_write", "to": "/r", "query": query}])
        out = compiled.apply(source={}, dest={})
        assert out == {"r": {"rowcount": 1}}
        assert calls[0] == ('INSERT INTO "users" ("name") VALUES (?)', ["Ann"])
        # write query compiled against the write pipeline
        assert compiled.steps[0].nested["query"]._pipeline is engine.get_pipeline("sql_write")


class TestCompiledAsync:
    @pytest.mark.asyncio
    async def test_async_compiled(self):
        engine = build_default_engine()
        calls = []

        async def executor(sql, params):
            calls.append((sql, params))
            return [{"n": 1}]

        install_sql(engine, executor, paramstyle="format")
        compiled = engine.compile([{"op": "sql", "to": "/rows", "query": {"$select": {
            "from": {"table": "t"},
            "where": {"$eq": [{"$col": "id"}, {"$val": 7}]},
        }}}])
        out = await compiled.apply_async(source={}, dest={})
        assert out == {"rows": [{"n": 1}]}
        assert calls[0] == ('SELECT * FROM "t" WHERE "id" = %s', [7])
