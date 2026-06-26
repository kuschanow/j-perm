"""Compiled-path coverage for the async engine.

Compiles specs with ``engine.compile`` and runs them through
``apply_compiled_async`` so every ``execute_compiled_async`` (foreach/while/if/
try/def) and ``CompiledSpec.run_async`` is exercised.
"""

import pytest

from j_perm import build_default_async_engine


@pytest.fixture
def aeng():
    return build_default_async_engine()


async def run_compiled(aeng, spec, source=None, dest=None):
    compiled = aeng.compile(spec)
    return await aeng.apply_compiled_async(compiled, source=source or {}, dest=dest if dest is not None else {})


class TestCompiledAsync:
    async def test_foreach_seq_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "foreach", "in_value": [1, 2, 3], "as": "it",
                                      "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]})
        assert r == {"o": [1, 2, 3]}

    async def test_foreach_break_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "foreach", "in_value": [1, 2, 3], "as": "it", "do": [
            {"op": "if", "cond": {"$eq": [{"$ref": "&:/it"}, 2]}, "then": [{"$break": None}]},
            {"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}},
        ]})
        assert r == {"o": [1]}

    async def test_foreach_parallel_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "foreach", "in_value": [10, 20, 30], "as": "it",
                                      "parallel": True, "concurrency": 2,
                                      "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]},
                               dest={"o": []})
        assert r == {"o": [10, 20, 30]}

    async def test_while_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "while", "cond": {"$lt": [{"$ref": "@:/n"}, 3]}, "do": [
            {"op": "set", "path": "/n", "value": {"$add": [{"$ref": "@:/n"}, 1]}}]}, dest={"n": 0})
        assert r == {"n": 3}

    async def test_while_break_continue_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "while", "cond": {"$lt": [{"$ref": "@:/n"}, 5]}, "do": [
            {"op": "set", "path": "/n", "value": {"$add": [{"$ref": "@:/n"}, 1]}},
            {"op": "if", "cond": {"$eq": [{"$ref": "@:/n"}, 2]}, "then": [{"$continue": None}]},
            {"op": "if", "cond": {"$eq": [{"$ref": "@:/n"}, 4]}, "then": [{"$break": None}]},
        ]}, dest={"n": 0})
        assert r == {"n": 4}

    async def test_if_compiled(self, aeng):
        assert await run_compiled(aeng, {"op": "if", "cond": True,
                                         "then": [{"op": "set", "path": "/x", "value": 1}]}) == {"x": 1}
        assert await run_compiled(aeng, {"op": "if", "cond": False,
                                         "else": [{"op": "set", "path": "/x", "value": 2}]}) == {"x": 2}
        assert await run_compiled(aeng, {"op": "if", "cond": False}) == {}

    async def test_if_signal_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "foreach", "in_value": [1, 2], "as": "it", "do": [
            {"op": "if", "cond": True, "then": [{"$break": None}]},
            {"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}},
        ]})
        assert r == {}

    async def test_try_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "try",
                                      "do": [{"op": "copy", "from": "/missing", "path": "/x"}],
                                      "except": [{"op": "set", "path": "/err", "value": "caught"}],
                                      "finally": [{"op": "set", "path": "/f", "value": 1}]})
        assert r == {"err": "caught", "f": 1}

    async def test_try_success_finally_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "try", "do": [{"op": "set", "path": "/x", "value": 1}],
                                      "finally": [{"op": "set", "path": "/f", "value": 1}]})
        assert r == {"x": 1, "f": 1}

    async def test_try_no_except_finally_reraise_compiled(self, aeng):
        with pytest.raises(Exception):
            await run_compiled(aeng, {"op": "try", "do": [{"op": "copy", "from": "/m", "path": "/x"}],
                                      "finally": [{"op": "set", "path": "/f", "value": 1}]})

    async def test_try_signal_finally_compiled(self, aeng):
        r = await run_compiled(aeng, {"op": "foreach", "in_value": [1, 2], "as": "it", "do": [
            {"op": "try", "do": [{"$break": None}], "finally": [{"op": "set", "path": "/f", "value": 1}]},
        ]})
        assert r == {"f": 1}

    async def test_def_func_compiled(self, aeng):
        spec = [
            {"$def": "f", "params": ["x"], "body": [
                {"$return": {"$mul": [{"$ref": "&:/x"}, 2]}}]},
            {"op": "set", "path": "/r", "value": {"$func": "f", "args": [5]}},
        ]
        assert await run_compiled(aeng, spec) == {"r": 10}

    async def test_def_return_path_compiled(self, aeng):
        spec = [
            {"$def": "f", "params": ["x"], "body": [
                {"op": "set", "path": "/out", "value": {"$ref": "&:/x"}}], "return": "/out"},
            {"op": "set", "path": "/r", "value": {"$func": "f", "args": [7]}},
        ]
        assert await run_compiled(aeng, spec) == {"r": 7}

    async def test_def_on_failure_compiled(self, aeng):
        spec = [
            {"$def": "f", "params": [], "body": [{"$raise": "boom"}],
             "on_failure": [{"op": "set", "path": "/handled", "value": True}]},
            {"op": "set", "path": "/r", "value": {"$func": "f"}},
        ]
        r = await run_compiled(aeng, spec)
        assert r["r"] == {"handled": True}
