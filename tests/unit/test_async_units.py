"""Direct unit tests for async helpers and edge branches not reachable through
the normal engine dispatch (matcher-guarded fallthroughs, merge utilities)."""

import pytest

from j_perm import build_default_async_engine
from j_perm.core import ExecutionContext
from j_perm.handlers.merge import deep_merge, deep_update
from j_perm.handlers.container_async import AsyncRecursiveDescentHandler
from j_perm.handlers.special_async import AsyncSpecialResolveHandler


class TestMergeUtils:
    def test_deep_update_recurses_and_replaces(self):
        dst = {"a": {"x": 1}, "b": [1]}
        deep_update(dst, {"a": {"y": 2}, "b": [9]})
        assert dst == {"a": {"x": 1, "y": 2}, "b": [9]}

    def test_deep_merge_dict_list_scalar(self):
        # dict recurse + list concat
        acc = {"o": [1], "m": {"a": 1}}
        acc = deep_merge(acc, {"o": [2], "m": {"b": 2}, "new": 3})
        assert acc == {"o": [1, 2], "m": {"a": 1, "b": 2}, "new": 3}
        # scalar replace (top-level non-mapping)
        assert deep_merge(1, 2) == 2
        # list-vs-list at top level
        assert deep_merge([1], [2]) == [1, 2]


class TestContainerFallthrough:
    async def test_scalar_returns_as_is(self):
        eng = build_default_async_engine()
        ctx = ExecutionContext(source={}, dest={}, engine=eng)
        handler = AsyncRecursiveDescentHandler()
        assert await handler.execute(42, ctx) == 42


class TestSpecialFallthrough:
    async def test_no_special_key_returns_step(self):
        eng = build_default_async_engine()
        ctx = ExecutionContext(source={}, dest={}, engine=eng)
        handler = AsyncSpecialResolveHandler({"$ref": lambda n, c: 1})
        # dict without any registered special key → returned unchanged
        assert await handler.execute({"plain": 1}, ctx) == {"plain": 1}


class TestConstructEdges:
    async def _val(self, eng, value, source=None):
        r = await eng.apply_async(spec={"op": "set", "path": "/r", "value": value},
                                  source=source or {}, dest={})
        return r["r"]

    async def test_str_slice_non_str(self):
        eng = build_default_async_engine()
        with pytest.raises(ValueError):
            await self._val(eng, {"$str_slice": {"string": 1}})

    async def test_str_strip_str_spec_resolves_non_str(self):
        eng = build_default_async_engine()
        with pytest.raises(ValueError):
            await self._val(eng, {"$str_strip": "${int:/n}"}, source={"n": "5"})

    async def test_if_truthy_selects_then(self):
        eng = build_default_async_engine()
        r = await self._val(
            eng, {"$if": {"$ref": "/flag"}, "$then": "a", "$else": "b"},
            source={"flag": True})
        assert r == "a"

    async def test_if_falsy_selects_else(self):
        eng = build_default_async_engine()
        r = await self._val(
            eng, {"$if": {"$ref": "/flag"}, "$then": "a", "$else": "b"},
            source={"flag": 0})
        assert r == "b"

    async def test_if_else_omitted_yields_none(self):
        eng = build_default_async_engine()
        r = await self._val(eng, {"$if": False, "$then": "a"})
        assert r is None

    async def test_if_is_lazy(self):
        eng = build_default_async_engine()
        # untaken $else references a missing pointer and must not be evaluated
        r = await self._val(
            eng, {"$if": True, "$then": 1, "$else": {"$ref": "/missing"}})
        assert r == 1


class TestAsyncEdgeBranches:
    async def test_while_return_propagates(self):
        eng = build_default_async_engine()
        spec = [
            {"$def": "f", "params": [], "body": [
                {"op": "while", "cond": True, "do": [{"$return": 42}]}]},
            {"op": "set", "path": "/r", "value": {"$func": "f"}},
        ]
        r = await eng.apply_async(spec=spec, source={}, dest={})
        assert r == {"r": 42}

    async def test_if_path_missing(self):
        eng = build_default_async_engine()
        # path resolution raises → missing=True → condition false → then-branch skipped
        r = await eng.apply_async(
            spec={"op": "if", "path": "/no/such", "then": [{"op": "set", "path": "/x", "value": 1}]},
            source={}, dest={})
        assert r == {}

    async def test_try_signal_with_failing_finally(self):
        eng = build_default_async_engine()
        spec = {"op": "foreach", "in_value": [1, 2], "as": "it", "do": [
            {"op": "try", "do": [{"$break": None}],
             "finally": [{"op": "copy", "from": "/missing", "path": "/x"}]},
        ]}
        # $break propagates even though finally raises (swallowed)
        r = await eng.apply_async(spec=spec, source={}, dest={})
        assert r == {}

    async def test_try_no_except_failing_finally_reraises(self):
        eng = build_default_async_engine()
        spec = {"op": "try", "do": [{"op": "copy", "from": "/m", "path": "/x"}],
                "finally": [{"op": "copy", "from": "/m2", "path": "/y"}]}
        with pytest.raises(Exception):
            await eng.apply_async(spec=spec, source={}, dest={})

    async def test_recursion_limit_direct(self):
        eng = build_default_async_engine(max_function_recursion_depth=5)
        spec = [
            {"$def": "loop", "body": [{"$func": "loop"}]},
            {"$func": "loop"},
        ]
        with pytest.raises(RecursionError, match="recursion depth.*exceeded maximum"):
            await eng.apply_async(spec=spec, source={}, dest={})


class TestFunctionContextShared:
    async def test_context_shared_mutates_caller_dest(self):
        eng = build_default_async_engine()
        spec = [
            {"$def": "f", "params": [], "context": "shared",
             "body": [{"op": "set", "path": "/inner", "value": 1}]},
            {"op": "set", "path": "/seed", "value": 0},
            {"op": "exec", "actions": [{"$func": "f"}], "merge": True},
        ]
        r = await eng.apply_async(spec=spec, source={}, dest={})
        assert r["inner"] == 1
