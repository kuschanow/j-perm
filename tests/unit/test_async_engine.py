"""End-to-end tests for the async engine (``build_default_async_engine``).

Exercises every async operation, construct, the function path, container/special
value dispatch, and the parallel ``foreach`` mode — driving them through
``apply_async`` so the async twins in ``handlers/*_async.py`` are covered.
"""

import asyncio

import pytest

from j_perm import build_default_async_engine, build_default_engine
from j_perm.handlers.function import JPermError


@pytest.fixture
def aeng():
    return build_default_async_engine()


async def run(aeng, spec, source=None, dest=None):
    return await aeng.apply_async(spec=spec, source=source or {}, dest=dest if dest is not None else {})


# ─────────────────────────────────────────────────────────────────────────────
# leaf ops
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncLeafOps:
    async def test_set_normal_and_append(self, aeng):
        r = await run(aeng, [
            {"op": "set", "path": "/a", "value": 1},
            {"op": "set", "path": "/list/-", "value": 2},
            {"op": "set", "path": "/list/-", "value": [3, 4]},
        ])
        assert r == {"a": 1, "list": [2, 3, 4]}

    async def test_set_append_no_create_raises(self, aeng):
        with pytest.raises(Exception):
            await run(aeng, {"op": "set", "path": "/missing/-", "value": 1, "create": False})

    async def test_copy_from_default_ignore(self, aeng):
        r = await run(aeng, {"op": "copy", "from": "/x", "path": "/y"}, source={"x": 5})
        assert r == {"y": 5}
        r2 = await run(aeng, {"op": "copy", "from": "/missing", "path": "/y", "default": 9})
        assert r2 == {"y": 9}
        r3 = await run(aeng, {"op": "copy", "from": "/missing", "path": "/y", "ignore_missing": True})
        assert r3 == {}
        with pytest.raises(Exception):
            await run(aeng, {"op": "copy", "from": "/missing", "path": "/y"})

    async def test_delete(self, aeng):
        r = await run(aeng, {"op": "delete", "path": "/a"}, dest={"a": 1, "b": 2})
        assert r == {"b": 2}
        r2 = await run(aeng, {"op": "delete", "path": "/missing"}, dest={"b": 2})
        assert r2 == {"b": 2}
        with pytest.raises((KeyError, IndexError)):
            await run(aeng, {"op": "delete", "path": "/missing", "ignore_missing": False}, dest={"b": 2})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "delete", "path": "/a/-"}, dest={"a": [1]})

    async def test_update(self, aeng):
        r = await run(aeng, {"op": "update", "path": "/a", "value": {"y": 2}}, dest={"a": {"x": 1}})
        assert r == {"a": {"x": 1, "y": 2}}
        r2 = await run(aeng, {"op": "update", "path": "/a", "from": "/src", "deep": True},
                       source={"src": {"n": {"b": 2}}}, dest={"a": {"n": {"a": 1}}})
        assert r2 == {"a": {"n": {"a": 1, "b": 2}}}
        r3 = await run(aeng, {"op": "update", "path": "/new", "from": "/missing", "default": {"k": 1}})
        assert r3 == {"new": {"k": 1}}
        with pytest.raises(TypeError):
            await run(aeng, {"op": "update", "path": "/a", "value": 5}, dest={"a": {}})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "update", "path": "/a"}, dest={"a": {}})
        with pytest.raises(KeyError):
            await run(aeng, {"op": "update", "path": "/a", "value": {"x": 1}, "create": False})

    async def test_update_from_missing_raises(self, aeng):
        with pytest.raises(Exception):
            await run(aeng, {"op": "update", "path": "/a", "from": "/missing"}, dest={"a": {}})

    async def test_distinct(self, aeng):
        r = await run(aeng, {"op": "distinct", "path": "/a"}, dest={"a": [1, 2, 2, 3, 1]})
        assert r == {"a": [1, 2, 3]}
        r2 = await run(aeng, {"op": "distinct", "path": "/a", "key": "/id"},
                       dest={"a": [{"id": 1}, {"id": 1}, {"id": 2}]})
        assert r2 == {"a": [{"id": 1}, {"id": 2}]}
        with pytest.raises(TypeError):
            await run(aeng, {"op": "distinct", "path": "/a"}, dest={"a": 5})

    async def test_distinct_unhashable(self, aeng):
        r = await run(aeng, {"op": "distinct", "path": "/a"}, dest={"a": [[1], [1], [2]]})
        assert r == {"a": [[1], [1], [2]]}

    async def test_assert(self, aeng):
        assert await run(aeng, {"op": "assert", "path": "/x"}, source={"x": 1}) == {}
        with pytest.raises(AssertionError):
            await run(aeng, {"op": "assert", "path": "/missing"}, source={})
        with pytest.raises(AssertionError):
            await run(aeng, {"op": "assert", "value": 1, "equals": 2})
        r = await run(aeng, {"op": "assert", "value": 1, "equals": 2, "return": True, "to_path": "/ok"})
        assert r == {"ok": False}
        r2 = await run(aeng, {"op": "assert", "path": "/missing", "return": True, "to_path": "/ok"})
        assert r2 == {"ok": False}
        r3 = await run(aeng, {"op": "assert", "value": 7, "return": True, "to_path": "/v"})
        assert r3 == {"v": 7}
        r4 = await run(aeng, {"op": "assert", "value": 7, "return": True})
        assert r4 == 7
        with pytest.raises(ValueError):
            await run(aeng, {"op": "assert", "path": "/x", "value": 1})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "assert"})

    async def test_deserialize(self, aeng):
        r = await run(aeng, {"op": "deserialize", "value": '{"a": 1}', "path": "/out"})
        assert r == {"out": {"a": 1}}
        r2 = await run(aeng, {"op": "deserialize", "from": "/raw", "format": "yaml", "path": "/out"},
                       source={"raw": "a: 2"})
        assert r2 == {"out": {"a": 2}}
        r3 = await run(aeng, {"op": "deserialize", "from": "/missing", "path": "/out", "default": {"d": 1}})
        assert r3 == {"out": {"d": 1}}
        r4 = await run(aeng, {"op": "deserialize", "value": "not json", "path": "/out", "default": {"d": 2}})
        assert r4 == {"out": {"d": 2}}
        with pytest.raises(ValueError):
            await run(aeng, {"op": "deserialize", "value": "not json", "path": "/out"})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "deserialize", "value": "x", "from": "/y", "path": "/out"})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "deserialize", "path": "/out"})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "deserialize", "value": "x", "format": "xml", "path": "/out"})
        with pytest.raises(Exception):
            await run(aeng, {"op": "deserialize", "from": "/missing", "path": "/out"})


# ─────────────────────────────────────────────────────────────────────────────
# foreach
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncForeach:
    async def test_seq_basic_and_dict_and_in_value(self, aeng):
        r = await run(aeng, {"op": "foreach", "in_value": [1, 2], "as": "it",
                             "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]})
        assert r == {"o": [1, 2]}
        r2 = await run(aeng, {"op": "foreach", "in": "/m", "as": "kv",
                              "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/kv"}, "extend": False}]},
                       source={"m": {"a": 1}})
        assert r2 == {"o": [["a", 1]]}

    async def test_skip_empty_and_default_and_size(self, aeng):
        assert await run(aeng, {"op": "foreach", "in": "/missing", "do": [{"op": "set", "path": "/x", "value": 1}]}) == {}
        r = await run(aeng, {"op": "foreach", "in": "/missing", "default": [1], "as": "it",
                             "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]})
        assert r == {"o": [1]}
        # skip_empty=False over empty array → body runs zero times, returns dest as-is
        assert await run(aeng, {"op": "foreach", "in_value": [], "skip_empty": False,
                                "do": [{"op": "set", "path": "/x", "value": 1}]}) == {}

    async def test_break_continue(self, aeng):
        r = await run(aeng, {"op": "foreach", "in_value": [1, 2, 3, 4], "as": "it", "do": [
            {"op": "if", "cond": {"$eq": [{"$ref": "&:/it"}, 3]}, "then": [{"$break": None}]},
            {"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}},
        ]})
        assert r == {"o": [1, 2]}
        r2 = await run(aeng, {"op": "foreach", "in_value": [1, 2, 3], "as": "it", "do": [
            {"op": "if", "cond": {"$eq": [{"$ref": "&:/it"}, 2]}, "then": [{"$continue": None}]},
            {"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}},
        ]})
        assert r2 == {"o": [1, 3]}

    async def test_validation_and_rollback(self, aeng):
        with pytest.raises(ValueError):
            await run(aeng, {"op": "foreach", "in": "/a", "in_value": [], "do": []})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "foreach", "do": []})
        with pytest.raises(ZeroDivisionError):
            await run(aeng, {"op": "foreach", "in_value": [1], "as": "it", "do": [
                {"op": "set", "path": "/x", "value": {"$div": [1, 0]}}]}, dest={"keep": 1})

    async def test_size_limit(self):
        aeng = build_default_async_engine(max_foreach_items=2)
        with pytest.raises(ValueError):
            await run(aeng, {"op": "foreach", "in_value": [1, 2, 3], "do": []})

    async def test_return_propagates(self, aeng):
        spec = [
            {"$def": "f", "params": [], "body": [
                {"op": "foreach", "in_value": [1, 2], "as": "it", "do": [{"$return": {"$ref": "&:/it"}}]},
                {"$return": "end"},
            ]},
            {"op": "set", "path": "/r", "value": {"$func": "f"}},
        ]
        assert await run(aeng, spec) == {"r": 1}

    async def test_parallel_ordered_merge(self, aeng):
        r = await run(aeng, {"op": "foreach", "in_value": [10, 20, 30], "as": "it", "parallel": True,
                             "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]},
                      dest={"o": []})
        assert r == {"o": [10, 20, 30]}

    async def test_parallel_concurrency_limit(self, aeng):
        r = await run(aeng, {"op": "foreach", "in_value": [1, 2, 3, 4], "as": "it",
                             "parallel": True, "concurrency": 2,
                             "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]},
                      dest={"o": []})
        assert r == {"o": [1, 2, 3, 4]}

    async def test_parallel_bad_concurrency(self, aeng):
        with pytest.raises(ValueError):
            await run(aeng, {"op": "foreach", "in_value": [1], "parallel": True, "concurrency": 0,
                             "do": [{"op": "set", "path": "/x", "value": 1}]})

    async def test_parallel_rejects_break(self, aeng):
        with pytest.raises(ValueError):
            await run(aeng, {"op": "foreach", "in_value": [1, 2], "parallel": True,
                             "do": [{"$break": None}]})

    async def test_parallel_rollback_on_error(self, aeng):
        with pytest.raises(ZeroDivisionError):
            await run(aeng, {"op": "foreach", "in_value": [1], "as": "it", "parallel": True,
                             "do": [{"op": "set", "path": "/x", "value": {"$div": [1, 0]}}]},
                      dest={"keep": 1})

    async def test_parallel_return_propagates(self, aeng):
        spec = [
            {"$def": "f", "params": [], "body": [
                {"op": "foreach", "in_value": [1, 2], "as": "it", "parallel": True,
                 "do": [{"$return": {"$ref": "&:/it"}}]},
                {"$return": "end"},
            ]},
            {"op": "set", "path": "/r", "value": {"$func": "f"}},
        ]
        assert await run(aeng, spec) in ({"r": 1}, {"r": 2})


# ─────────────────────────────────────────────────────────────────────────────
# while / if / exec / try
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncControl:
    async def test_while_cond(self, aeng):
        r = await run(aeng, {"op": "while", "cond": {"$lt": [{"$ref": "@:/n"}, 3]}, "do": [
            {"op": "set", "path": "/n", "value": {"$add": [{"$ref": "@:/n"}, 1]}}]}, dest={"n": 0})
        assert r == {"n": 3}

    async def test_while_path_variants(self, aeng):
        r = await run(aeng, {"op": "while", "path": "@:/flag", "do": [
            {"op": "set", "path": "/flag", "value": False}]}, dest={"flag": True})
        assert r == {"flag": False}
        r2 = await run(aeng, {"op": "while", "path": "/x", "equals": 1, "do_while": True, "do": [
            {"op": "set", "path": "/x", "value": 2}]}, dest={"x": 1})
        assert r2 == {"x": 2}
        r3 = await run(aeng, {"op": "while", "path": "/missing", "exists": True, "do": [
            {"op": "set", "path": "/y", "value": 1}]}, dest={})
        assert r3 == {}

    async def test_while_break_continue_maxiter_rollback(self, aeng):
        r = await run(aeng, {"op": "while", "cond": True, "do": [{"$break": None}]}, dest={"k": 1})
        assert r == {"k": 1}
        with pytest.raises(ValueError):
            await run(aeng, {"op": "while", "do": [{"op": "set", "path": "/x", "value": 1}]})
        aeng2 = build_default_async_engine(max_loop_iterations=2)
        with pytest.raises(RuntimeError):
            await run(aeng2, {"op": "while", "cond": True, "do": [
                {"op": "set", "path": "/n", "value": 1}]})

    async def test_while_continue(self, aeng):
        r = await run(aeng, {"op": "while", "cond": {"$lt": [{"$ref": "@:/n"}, 2]}, "do": [
            {"op": "set", "path": "/n", "value": {"$add": [{"$ref": "@:/n"}, 1]}},
            {"$continue": None},
        ]}, dest={"n": 0})
        assert r == {"n": 2}

    async def test_while_rollback(self, aeng):
        with pytest.raises(ZeroDivisionError):
            await run(aeng, {"op": "while", "cond": True, "do": [
                {"op": "set", "path": "/x", "value": {"$div": [1, 0]}}]}, dest={"keep": 1})

    async def test_if_then_else_do(self, aeng):
        assert await run(aeng, {"op": "if", "cond": True, "then": [{"op": "set", "path": "/x", "value": 1}]}) == {"x": 1}
        assert await run(aeng, {"op": "if", "cond": False, "else": [{"op": "set", "path": "/x", "value": 2}]}) == {"x": 2}
        assert await run(aeng, {"op": "if", "cond": True, "do": [{"op": "set", "path": "/x", "value": 3}]}) == {"x": 3}
        assert await run(aeng, {"op": "if", "cond": False}) == {}

    async def test_if_path_variants_and_rollback(self, aeng):
        assert await run(aeng, {"op": "if", "path": "/x", "equals": 1,
                                "then": [{"op": "set", "path": "/ok", "value": 1}]},
                         source={"x": 1}) == {"ok": 1}
        assert await run(aeng, {"op": "if", "path": "/x", "exists": True,
                                "then": [{"op": "set", "path": "/ok", "value": 1}]},
                         source={"x": 9}) == {"ok": 1}
        assert await run(aeng, {"op": "if", "path": "/x",
                                "then": [{"op": "set", "path": "/ok", "value": 1}]},
                         source={"x": True}) == {"ok": 1}
        with pytest.raises(ZeroDivisionError):
            await run(aeng, {"op": "if", "cond": True, "then": [
                {"op": "set", "path": "/x", "value": {"$div": [1, 0]}}]}, dest={"keep": 1})

    async def test_if_signal_propagates(self, aeng):
        r = await run(aeng, {"op": "foreach", "in_value": [1, 2], "as": "it", "do": [
            {"op": "if", "cond": True, "then": [{"$break": None}]},
            {"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}},
        ]})
        assert r == {}

    async def test_exec(self, aeng):
        r = await run(aeng, {"op": "exec", "actions": [{"op": "set", "path": "/x", "value": 1}]})
        assert r == {"x": 1}
        r2 = await run(aeng, {"op": "exec", "actions": [{"op": "set", "path": "/x", "value": 1}], "merge": True},
                       dest={"y": 2})
        assert r2 == {"y": 2, "x": 1}
        r3 = await run(aeng, {"op": "exec", "from": "/acts"},
                       source={"acts": [{"op": "set", "path": "/x", "value": 5}]})
        assert r3 == {"x": 5}
        r4 = await run(aeng, {"op": "exec", "from": "/missing",
                              "default": [{"op": "set", "path": "/x", "value": 6}]})
        assert r4 == {"x": 6}
        with pytest.raises(ValueError):
            await run(aeng, {"op": "exec", "from": "/missing"})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "exec", "from": "/a", "actions": []})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "exec"})

    async def test_try(self, aeng):
        r = await run(aeng, {"op": "try",
                             "do": [{"op": "copy", "from": "/missing", "path": "/x"}],
                             "except": [{"op": "set", "path": "/err", "value": "caught"}]})
        assert r["err"] == "caught"
        r2 = await run(aeng, {"op": "try", "do": [{"op": "set", "path": "/x", "value": 1}],
                              "finally": [{"op": "set", "path": "/f", "value": 1}]})
        assert r2 == {"x": 1, "f": 1}
        with pytest.raises(Exception):
            await run(aeng, {"op": "try", "do": [{"op": "copy", "from": "/m", "path": "/x"}],
                             "finally": [{"op": "set", "path": "/f", "value": 1}]})
        with pytest.raises(ValueError):
            await run(aeng, {"op": "try", "except": []})

    async def test_try_signal_with_finally(self, aeng):
        r = await run(aeng, {"op": "foreach", "in_value": [1, 2], "as": "it", "do": [
            {"op": "try", "do": [{"$break": None}], "finally": [{"op": "set", "path": "/f", "value": 1}]},
        ]})
        assert r == {"f": 1}


# ─────────────────────────────────────────────────────────────────────────────
# functions
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncFunctions:
    async def test_def_call_params_return(self, aeng):
        spec = [
            {"$def": "add", "params": ["a", "b"], "body": [
                {"op": "set", "path": "/r", "value": {"$add": [{"$ref": "&:/a"}, {"$ref": "&:/b"}]}}],
             "return": "/r"},
            {"op": "set", "path": "/out", "value": {"$func": "add", "args": [2, 3]}},
        ]
        assert await run(aeng, spec) == {"out": 5}

    async def test_context_new_shared(self, aeng):
        spec = [
            {"$def": "f", "params": [], "context": "new", "body": [
                {"op": "set", "path": "/inner", "value": 1}]},
            {"op": "set", "path": "/x", "value": {"$func": "f"}},
        ]
        assert await run(aeng, spec) == {"x": {"inner": 1}}

    async def test_on_failure(self, aeng):
        spec = [
            {"$def": "f", "params": [], "body": [{"$raise": "boom"}],
             "on_failure": [{"op": "set", "path": "/handled", "value": True}]},
            {"op": "set", "path": "/x", "value": {"$func": "f"}},
        ]
        # on_failure shares the caller dest (no deepcopy), matching the sync handler.
        assert await run(aeng, spec) == {"handled": True, "x": {"handled": True}}

    async def test_invalid_context(self, aeng):
        spec = [
            {"$def": "f", "params": [], "context": "bogus", "body": []},
            {"op": "set", "path": "/x", "value": {"$func": "f"}},
        ]
        with pytest.raises(ValueError):
            await run(aeng, spec)

    async def test_arg_count_mismatch(self, aeng):
        spec = [
            {"$def": "f", "params": ["a"], "body": [{"$return": 1}]},
            {"op": "set", "path": "/x", "value": {"$func": "f", "args": []}},
        ]
        with pytest.raises(ValueError):
            await run(aeng, spec)

    async def test_undefined_function(self, aeng):
        with pytest.raises(ValueError):
            await run(aeng, {"op": "set", "path": "/x", "value": {"$func": "nope"}})

    async def test_recursion_limit(self):
        aeng = build_default_async_engine(max_function_recursion_depth=5)
        spec = [
            {"$def": "loop", "body": [{"$func": "loop"}]},
            {"$func": "loop"},
        ]
        with pytest.raises(RecursionError, match="recursion depth.*exceeded maximum"):
            await run(aeng, spec)

    async def test_raise_uncaught(self, aeng):
        with pytest.raises(JPermError):
            await run(aeng, {"$raise": "boom"})

    async def test_return_at_top(self, aeng):
        # $return raises ReturnSignal; at top-level it surfaces as a signal
        from j_perm.handlers.signals import ReturnSignal
        with pytest.raises(ReturnSignal):
            await run(aeng, {"$return": 5})


# ─────────────────────────────────────────────────────────────────────────────
# container / special / raw
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncValueDispatch:
    async def test_container_list_and_dict(self, aeng):
        r = await run(aeng, {"op": "set", "path": "/x",
                             "value": {"a": {"$ref": "/n"}, "b": [{"$ref": "/n"}, 2]}},
                      source={"n": 7})
        assert r == {"x": {"a": 7, "b": [7, 2]}}

    async def test_container_key_collision(self, aeng):
        with pytest.raises(KeyError):
            await run(aeng, {"op": "set", "path": "/x",
                             "value": {"${/a}": 1, "${/b}": 2}}, source={"a": "k", "b": "k"})

    async def test_raw_flag_and_literal(self, aeng):
        r = await run(aeng, {"op": "set", "path": "/x", "value": {"$raw": {"$ref": "/not"}}})
        assert r == {"x": {"$ref": "/not"}}
        r2 = await run(aeng, {"op": "set", "path": "/x", "value": {"$ref": "@:/y", "$raw": True}},
                       dest={"y": {"$ref": "/z"}})
        assert r2["x"] == {"$ref": "/z"}


# ─────────────────────────────────────────────────────────────────────────────
# constructs (async twins)
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncConstructs:
    async def _val(self, aeng, value, source=None):
        r = await run(aeng, {"op": "set", "path": "/r", "value": value}, source=source or {})
        return r["r"]

    async def test_ref_default(self, aeng):
        assert await self._val(aeng, {"$ref": "/missing", "$default": 9}) == 9
        with pytest.raises(Exception):
            await self._val(aeng, {"$ref": "/missing"})

    async def test_eval_select(self, aeng):
        assert await self._val(aeng, {"$eval": {"/a": 1, "/b": 2}, "$select": "/a"}) == 1
        assert await self._val(aeng, {"$eval": {"/a": 1}}) == {"a": 1}

    async def test_logic(self, aeng):
        assert await self._val(aeng, {"$and": [1, 2]}) == 2
        assert await self._val(aeng, {"$and": [0, 2]}) == 0
        assert await self._val(aeng, {"$or": [0, 5]}) == 5
        assert await self._val(aeng, {"$or": [0, ""]}) == ""
        assert await self._val(aeng, {"$not": False}) is True

    async def test_comparisons(self, aeng):
        assert await self._val(aeng, {"$gt": [2, 1]}) is True
        assert await self._val(aeng, {"$gte": [2, 2]}) is True
        assert await self._val(aeng, {"$lt": [1, 2]}) is True
        assert await self._val(aeng, {"$lte": [2, 2]}) is True
        assert await self._val(aeng, {"$eq": [1, 1]}) is True
        assert await self._val(aeng, {"$ne": [1, 2]}) is True
        with pytest.raises(ValueError):
            await self._val(aeng, {"$gt": [1]})

    async def test_in_exists(self, aeng):
        assert await self._val(aeng, {"$in": [2, [1, 2]]}) is True
        with pytest.raises(ValueError):
            await self._val(aeng, {"$in": [1]})
        assert await self._val(aeng, {"$exists": "/x"}, source={"x": 1}) is True
        assert await self._val(aeng, {"$exists": "/missing"}) is False

    async def test_arithmetic(self, aeng):
        assert await self._val(aeng, {"$add": [1, 2, 3]}) == 6
        assert await self._val(aeng, {"$sub": [10, 3]}) == 7
        assert await self._val(aeng, {"$mul": [2, 3]}) == 6
        assert await self._val(aeng, {"$div": [10, 2]}) == 5
        assert await self._val(aeng, {"$pow": [2, 3]}) == 8
        assert await self._val(aeng, {"$mod": [10, 3]}) == 1
        for op in ("$add", "$sub", "$mul", "$div", "$pow", "$mod"):
            with pytest.raises(ValueError):
                await self._val(aeng, {op: "notalist"})

    async def test_arithmetic_limits(self):
        aeng = build_default_async_engine(add_max_number_result=10)
        with pytest.raises(ValueError):
            await self._val(aeng, {"$add": [9, 9]})

    async def test_round(self, aeng):
        assert await self._val(aeng, {"$round": 3.2}) == 3
        assert await self._val(aeng, {"$round": {"value": 3.14159, "ndigits": 2}}) == 3.14
        assert await self._val(aeng, {"$round": {"value": 3.141, "ndigits": 2, "mode": "ceil"}}) == 3.15
        assert await self._val(aeng, {"$round": {"value": 3.149, "ndigits": 2, "mode": "floor"}}) == 3.14
        assert await self._val(aeng, {"$round": {"value": 7, "mode": "ceil"}}) == 7
        with pytest.raises(ValueError):
            await self._val(aeng, {"$round": "x"})
        with pytest.raises(ValueError):
            await self._val(aeng, {"$round": {"value": 1, "mode": "bogus"}})

    async def test_strings(self, aeng):
        assert await self._val(aeng, {"$str_split": {"string": "a,b", "delimiter": ","}}) == ["a", "b"]
        assert await self._val(aeng, {"$str_join": {"array": ["a", "b"], "separator": "-"}}) == "a-b"
        assert await self._val(aeng, {"$str_join": {"array": [], "separator": "-"}}) == ""
        assert await self._val(aeng, {"$str_slice": {"string": "hello", "start": 1, "end": 3}}) == "el"
        assert await self._val(aeng, {"$str_upper": "ab"}) == "AB"
        assert await self._val(aeng, {"$str_lower": "AB"}) == "ab"
        assert await self._val(aeng, {"$str_strip": "  x  "}) == "x"
        assert await self._val(aeng, {"$str_strip": {"string": "**x**", "chars": "*"}}) == "x"
        assert await self._val(aeng, {"$str_lstrip": "  x"}) == "x"
        assert await self._val(aeng, {"$str_lstrip": {"string": "__x", "chars": "_"}}) == "x"
        assert await self._val(aeng, {"$str_rstrip": "x  "}) == "x"
        assert await self._val(aeng, {"$str_rstrip": {"string": "x__", "chars": "_"}}) == "x"
        assert await self._val(aeng, {"$str_replace": {"string": "aa", "old": "a", "new": "b"}}) == "bb"
        assert await self._val(aeng, {"$str_contains": {"string": "abc", "substring": "b"}}) is True
        assert await self._val(aeng, {"$str_startswith": {"string": "abc", "prefix": "ab"}}) is True
        assert await self._val(aeng, {"$str_endswith": {"string": "abc", "suffix": "bc"}}) is True

    async def test_string_errors(self, aeng):
        # dict-required constructs reject a bare string
        for spec in ({"$str_split": "x"}, {"$str_join": "x"}, {"$str_slice": "x"}):
            with pytest.raises(ValueError):
                await self._val(aeng, spec)
        # upper/lower validate the resolved string type
        for spec in ({"$str_upper": 1}, {"$str_lower": 1}):
            with pytest.raises(ValueError):
                await self._val(aeng, spec)
        # strip with a non-str / non-dict spec falls into the dict branch → AttributeError
        with pytest.raises(AttributeError):
            await self._val(aeng, {"$str_strip": 1})
        # dict form with non-string 'string' → ValueError
        for spec in ({"$str_strip": {"string": 1}}, {"$str_lstrip": {"string": 1}},
                     {"$str_rstrip": {"string": 1}},
                     {"$str_contains": {"string": 1, "substring": "x"}}):
            with pytest.raises(ValueError):
                await self._val(aeng, spec)

    async def test_regex(self, aeng):
        assert await self._val(aeng, {"$regex_match": {"pattern": r"\d+", "string": "123"}}) is True
        assert await self._val(aeng, {"$regex_search": {"pattern": r"\d+", "string": "a1"}}) == "1"
        assert await self._val(aeng, {"$regex_search": {"pattern": r"\d+", "string": "ab"}}) is None
        assert await self._val(aeng, {"$regex_findall": {"pattern": r"\d", "string": "a1b2"}}) == ["1", "2"]
        assert await self._val(aeng, {"$regex_replace": {"pattern": r"\d", "replacement": "X", "string": "a1"}}) == "aX"
        assert await self._val(aeng, {"$regex_groups": {"pattern": r"(\d)(\w)", "string": "1a"}}) == ["1", "a"]
        assert await self._val(aeng, {"$regex_groups": {"pattern": r"\d", "string": "ab"}}) == []

    async def test_regex_errors(self, aeng):
        with pytest.raises(ValueError):
            await self._val(aeng, {"$regex_match": {"pattern": r"\d", "string": 1}})

    async def test_cast(self, aeng):
        assert await self._val(aeng, {"$cast": {"value": "42", "type": "int"}}) == 42
        with pytest.raises(ValueError):
            await self._val(aeng, {"$cast": "x"})
        with pytest.raises(ValueError):
            await self._val(aeng, {"$cast": {"value": "1"}})
        with pytest.raises(ValueError):
            await self._val(aeng, {"$cast": {"value": "1", "type": 5}})
        with pytest.raises(KeyError):
            await self._val(aeng, {"$cast": {"value": "1", "type": "nope"}})


# ─────────────────────────────────────────────────────────────────────────────
# builder parity & customisation
# ─────────────────────────────────────────────────────────────────────────────

class TestBuilders:
    async def test_custom_specials_path(self):
        # Passing specials=... skips _default_specials in both builders.
        eng = build_default_async_engine(specials={})
        assert await run(eng, {"op": "set", "path": "/x", "value": 1}) == {"x": 1}

    async def test_custom_casters(self):
        eng = build_default_async_engine(casters={"int": int})
        r = await eng.apply_async(spec={"op": "set", "path": "/x", "value": {"$cast": {"value": "5", "type": "int"}}},
                                  source={}, dest={})
        assert r == {"x": 5}

    async def test_parity_with_sync(self):
        spec = [
            {"$def": "sq", "params": ["x"], "body": [{"$return": {"$mul": [{"$ref": "&:/x"}, {"$ref": "&:/x"}]}}]},
            {"op": "foreach", "in": "/items", "as": "it",
             "do": [{"op": "set", "path": "/out/-", "value": {"$func": "sq", "args": [{"$ref": "&:/it"}]}}]},
        ]
        sync = build_default_engine().apply(spec=spec, source={"items": [1, 2, 3]}, dest={})
        a = await build_default_async_engine().apply_async(spec=spec, source={"items": [1, 2, 3]}, dest={})
        assert sync == a == {"out": [1, 4, 9]}


# ─────────────────────────────────────────────────────────────────────────────
# $exit — early, error-free termination (async path)
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncExit:
    async def test_exit_top_level(self, aeng):
        assert await run(aeng, [{"/a": 1}, {"$exit": None}, {"/b": 2}]) == {"a": 1}

    async def test_exit_from_foreach(self, aeng):
        r = await run(aeng, [
            {"op": "foreach", "in_value": [1, 2, 3, 4], "as": "it", "do": [
                {"op": "if", "cond": {"$eq": [{"$ref": "&:/it"}, 3]}, "then": [{"$exit": None}]},
                {"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}},
            ]},
            {"/after": True},
        ], dest={"o": []})
        assert r == {"o": [1, 2]}

    async def test_exit_from_parallel_foreach(self, aeng):
        # $exit propagates out of a parallel foreach and terminates the whole
        # script; per-iteration deltas are never merged, so dest stays as-is.
        r = await run(aeng, [
            {"op": "foreach", "in_value": [1, 2, 3], "as": "it", "parallel": True,
             "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}, {"$exit": None}]},
            {"/after": True},
        ], dest={"o": []})
        assert r == {"o": []}

    async def test_exit_from_while(self, aeng):
        r = await run(aeng, [
            {"/n": 0},
            {"op": "while", "cond": {"$lt": [{"$ref": "@:/n"}, 10]}, "do": [
                {"op": "if", "cond": {"$eq": [{"$ref": "@:/n"}, 2]}, "then": [{"$exit": None}]},
                {"/n": {"$add": [{"$ref": "@:/n"}, 1]}},
            ]},
            {"/after": True},
        ])
        assert r == {"n": 2}

    async def test_exit_from_if(self, aeng):
        r = await run(aeng, [
            {"/a": 1},
            {"op": "if", "cond": True, "then": [{"$exit": None}]},
            {"/after": True},
        ])
        assert r == {"a": 1}

    async def test_exit_runs_finally_not_except(self, aeng):
        r = await run(aeng, [
            {"op": "try", "do": [{"/x": 1}, {"$exit": None}],
             "except": [{"/caught": True}], "finally": [{"/cleaned": True}]},
            {"/after": True},
        ])
        assert r == {"x": 1, "cleaned": True}

    async def test_exit_from_function_bypasses_on_failure(self, aeng):
        r = await run(aeng, [
            {"$def": "f", "params": [], "context": "shared",
             "body": [{"/in_func": True}, {"$exit": None}],
             "on_failure": [{"/failed": True}]},
            {"/before": 1},
            {"$func": "f"},
            {"/after": 2},
        ])
        assert r == {"before": 1, "in_func": True}

    async def test_exit_compiled_async(self, aeng):
        compiled = aeng.compile([
            {"/a": 1},
            {"op": "foreach", "in_value": [1, 2, 3], "as": "it", "do": [
                {"op": "if", "cond": {"$eq": [{"$ref": "&:/it"}, 2]}, "then": [{"$exit": None}]},
                {"/seen[]": "&:/it"},
            ]},
            {"/after": True},
        ])
        result = await compiled.apply_async(source={}, dest={})
        assert result == {"a": 1, "seen": [1]}
