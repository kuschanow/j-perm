"""Tests for the Pipeline compilation feature.

Covers CompiledSpec, CompiledStep, Compilable, Pipeline.compile(),
Pipeline.run_compiled(), Pipeline.run_compiled_async(),
Engine.compile(), Engine.apply_compiled(), Engine.apply_compiled_async(),
Engine.apply_compiled_to_context(), Engine.apply_compiled_to_context_async().
"""

import copy
import pickle
import pytest
import logging

from j_perm import (
    build_default_engine,
    Compound,
    CompiledStep,
    CompiledSpec,
    ActionHandler,
    ActionNode,
    ActionTypeRegistry,
    AlwaysMatcher,
    ExecutionContext,
    Pipeline,
    StageNode,
    StageRegistry,
    StageProcessor,
    StageMatcher,
)
from j_perm.handlers.ops import ForeachHandler, WhileHandler, IfHandler, TryHandler
from j_perm.handlers.function import DefHandler


# ─────────────────────────────────────────────────────────────────────────────
# Exports
# ─────────────────────────────────────────────────────────────────────────────

class TestExports:
    def test_compilable_exported(self):
        import j_perm
        assert hasattr(j_perm, "Compound")
        assert j_perm.Compound is Compound

    def test_compiled_step_exported(self):
        import j_perm
        assert hasattr(j_perm, "CompiledStep")

    def test_compiled_spec_exported(self):
        import j_perm
        assert hasattr(j_perm, "CompiledSpec")


# ─────────────────────────────────────────────────────────────────────────────
# context_aware flag defaults
# ─────────────────────────────────────────────────────────────────────────────

class TestContextAwareFlag:
    def test_stage_processor_default_false(self):
        assert StageProcessor.context_aware is False

    def test_stage_matcher_default_false(self):
        assert StageMatcher.context_aware is False

    def test_custom_context_aware_stage_processor(self):
        class MyProcessor(StageProcessor):
            context_aware = True
            def apply(self, steps, ctx):
                return steps
        assert MyProcessor.context_aware is True

    def test_custom_context_aware_stage_matcher(self):
        class MyMatcher(StageMatcher):
            context_aware = True
            def matches(self, steps, ctx):
                return True
        assert MyMatcher.context_aware is True


# ─────────────────────────────────────────────────────────────────────────────
# Basic compilation
# ─────────────────────────────────────────────────────────────────────────────

class TestBasicCompilation:
    def test_compile_returns_compiled_spec(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        assert compiled is not None
        assert isinstance(compiled, CompiledSpec)

    def test_compile_sets_engine_ref(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        assert compiled._engine is engine

    def test_compile_repr(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        assert "1 steps" in repr(compiled)

    def test_compile_empty_spec(self):
        engine = build_default_engine()
        compiled = engine.compile([])
        assert compiled is not None
        assert compiled.steps == []

    def test_compile_single_step_dict(self):
        engine = build_default_engine()
        compiled = engine.compile({"op": "set", "path": "/x", "value": 42})
        assert compiled is not None
        assert len(compiled.steps) == 1

    def test_compile_shorthand_spec(self):
        """Shorthand steps are normalized during compilation."""
        engine = build_default_engine()
        compiled = engine.compile({"/x": 1})
        assert compiled is not None
        # The shorthand was expanded to a 'set' or 'copy' step
        assert len(compiled.steps) == 1
        assert compiled.steps[0].step["op"] in ("set", "copy")

    def test_apply_compiled_matches_apply(self):
        engine = build_default_engine()
        spec = [{"op": "set", "path": "/x", "value": 1}, {"op": "set", "path": "/y", "value": 2}]
        compiled = engine.compile(spec)
        r1 = engine.apply(spec, source={}, dest={})
        r2 = compiled.apply(source={}, dest={})
        assert r1 == r2

    def test_apply_compiled_multiple_runs(self):
        """Compiled spec produces correct results across multiple different inputs."""
        engine = build_default_engine()
        spec = [{"op": "copy", "from": "/val", "path": "/out"}]
        compiled = engine.compile(spec)
        assert compiled.apply(source={"val": "a"}, dest={}) == {"out": "a"}
        assert compiled.apply(source={"val": "b"}, dest={}) == {"out": "b"}
        assert compiled.apply(source={"val": 42}, dest={}) == {"out": 42}

    def test_compile_normalizes_then_resolves(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/k", "value": "v"}])
        step = compiled.steps[0]
        assert step.step["op"] == "set"
        assert len(step.handlers) == 1

    def test_compiled_step_has_no_nested_for_simple_ops(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/k", "value": "v"}])
        assert compiled.steps[0].nested == {}


# ─────────────────────────────────────────────────────────────────────────────
# context_aware blocks compilation
# ─────────────────────────────────────────────────────────────────────────────

class TestContextAwareBlocks:
    def test_compile_returns_none_for_context_aware_processor(self):
        class CtxAwareProcessor(StageProcessor):
            context_aware = True
            def apply(self, steps, ctx):
                return steps

        engine = build_default_engine()
        stage_reg = StageRegistry()
        stage_reg.register(StageNode(
            name="ca", priority=10, processor=CtxAwareProcessor()
        ))
        pipeline = Pipeline(registry=engine.main_pipeline.registry, stages=stage_reg)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = pipeline.compile([{"op": "set", "path": "/x", "value": 1}], ctx)
        assert result is None

    def test_compile_returns_none_for_context_aware_matcher(self):
        class CtxAwareMatcher(StageMatcher):
            context_aware = True
            def matches(self, steps, ctx):
                return False

        class NoOpProcessor(StageProcessor):
            def apply(self, steps, ctx):
                return steps

        engine = build_default_engine()
        stage_reg = StageRegistry()
        stage_reg.register(StageNode(
            name="ca", priority=10,
            matcher=CtxAwareMatcher(),
            processor=NoOpProcessor(),
        ))
        pipeline = Pipeline(registry=engine.main_pipeline.registry, stages=stage_reg)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = pipeline.compile([{"op": "set", "path": "/x", "value": 1}], ctx)
        assert result is None

    def test_engine_compile_returns_none_for_context_aware(self):
        """Engine.compile returns None when pipeline has context-aware stages."""
        class CtxAwareProcessor(StageProcessor):
            context_aware = True
            def apply(self, steps, ctx):
                return steps

        engine = build_default_engine()
        stage_reg = StageRegistry()
        stage_reg.register(StageNode(name="ca", priority=10, processor=CtxAwareProcessor()))
        engine.main_pipeline.stages = stage_reg
        result = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# CompiledSpec methods
# ─────────────────────────────────────────────────────────────────────────────

class TestCompiledSpecMethods:
    def test_apply_without_engine_raises(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        compiled._engine = None
        with pytest.raises(RuntimeError, match="no engine attached"):
            compiled.apply(source={}, dest={})

    def test_apply_async_without_engine_raises(self):
        import asyncio
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        compiled._engine = None
        with pytest.raises(RuntimeError, match="no engine attached"):
            asyncio.get_event_loop().run_until_complete(
                compiled.apply_async(source={}, dest={})
            )

    def test_attach_engine_returns_self(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        compiled._engine = None
        result = compiled.attach_engine(engine)
        assert result is compiled
        assert compiled._engine is engine

    def test_apply_with_engine_kwarg(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 99}])
        compiled._engine = None
        result = compiled.apply(source={}, dest={}, engine=engine)
        assert result == {"x": 99}

    def test_apply_async_with_engine_kwarg(self):
        import asyncio
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 99}])
        compiled._engine = None

        async def run():
            return await compiled.apply_async(source={}, dest={}, engine=engine)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"x": 99}

    def test_run_uses_ctx_engine(self):
        """CompiledSpec.run uses ctx.engine, not self._engine."""
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 7}])
        compiled._engine = None  # remove engine from compiled spec
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = compiled.run(ctx)
        assert result == {"x": 7}


# ─────────────────────────────────────────────────────────────────────────────
# Pickle support
# ─────────────────────────────────────────────────────────────────────────────

class TestPickle:
    def test_pickle_roundtrip(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        data = pickle.dumps(compiled)
        restored = pickle.loads(data)
        assert restored._engine is None
        assert len(restored.steps) == len(compiled.steps)

    def test_pickle_preserves_steps(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/a", "value": "hello"}])
        restored = pickle.loads(pickle.dumps(compiled))
        restored.attach_engine(engine)
        result = restored.apply(source={}, dest={})
        assert result == {"a": "hello"}

    def test_pickle_with_attach_engine(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "copy", "from": "/src", "path": "/dst"}])
        restored = pickle.loads(pickle.dumps(compiled))
        restored.attach_engine(engine)
        result = restored.apply(source={"src": 42}, dest={})
        assert result == {"dst": 42}

    def test_pickle_nested_compiled_spec(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "item",
                 "do": [{"op": "set", "path": "/last", "value": "${&:/item}"}]}]
        compiled = engine.compile(spec)
        restored = pickle.loads(pickle.dumps(compiled))
        restored.attach_engine(engine)
        result = restored.apply(source={"items": [1, 2, 3]}, dest={})
        assert result == {"last": 3}


# ─────────────────────────────────────────────────────────────────────────────
# Nested compiled specs — foreach
# ─────────────────────────────────────────────────────────────────────────────

class TestForeachCompiled:
    def test_foreach_body_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "item",
                 "do": [{"op": "set", "path": "/result/-", "value": "${&:/item}"}]}]
        compiled = engine.compile(spec)
        assert "do" in compiled.steps[0].nested
        result = compiled.apply(source={"items": [1, 2, 3]}, dest={"result": []})
        assert result == {"result": [1, 2, 3]}

    def test_foreach_compiled_matches_dynamic(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "x",
                 "do": [{"op": "set", "path": "/sum", "value": {"$add": ["${@:/sum}", "${&:/x}"]}}]}]
        compiled = engine.compile(spec)
        r_dyn = engine.apply(spec, source={"items": [1, 2, 3]}, dest={"sum": 0})
        r_comp = compiled.apply(source={"items": [1, 2, 3]}, dest={"sum": 0})
        assert r_dyn == r_comp

    def test_foreach_compiled_in_value(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in_value": [10, 20, 30], "as": "x",
                 "do": [{"op": "set", "path": "/last", "value": "${&:/x}"}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={})
        assert result == {"last": 30}

    def test_foreach_compiled_fallback_no_nested(self):
        """When there's no compiled nested (e.g. exec with from), fallback works."""
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "item",
                 "do": [{"op": "set", "path": "/x", "value": 1}]}]
        compiled = engine.compile(spec)
        # Simulate missing nested by clearing it
        compiled.steps[0].nested.clear()
        result = compiled.apply(source={"items": [1]}, dest={})
        assert result == {"x": 1}


# ─────────────────────────────────────────────────────────────────────────────
# Nested compiled specs — while
# ─────────────────────────────────────────────────────────────────────────────

class TestWhileCompiled:
    def test_while_body_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "while", "cond": {"$lt": ["${@:/n}", 3]},
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}}]}]
        compiled = engine.compile(spec)
        assert "do" in compiled.steps[0].nested
        result = compiled.apply(source={}, dest={"n": 0})
        assert result == {"n": 3}

    def test_while_compiled_matches_dynamic(self):
        engine = build_default_engine()
        # Use source for the condition check, dest for tracking changes
        spec = [{"op": "while", "cond": {"$lt": ["${@:/n}", 3]},
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}}]}]
        compiled = engine.compile(spec)
        r_dyn = engine.apply(spec, source={}, dest={"n": 0})
        r_comp = compiled.apply(source={}, dest={"n": 0})
        assert r_dyn == r_comp == {"n": 3}

    def test_while_compiled_fallback_no_nested(self):
        engine = build_default_engine()
        spec = [{"op": "while", "cond": {"$lt": ["${@:/n}", 2]},
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}}]}]
        compiled = engine.compile(spec)
        compiled.steps[0].nested.clear()
        result = compiled.apply(source={}, dest={"n": 0})
        assert result == {"n": 2}


# ─────────────────────────────────────────────────────────────────────────────
# Nested compiled specs — if
# ─────────────────────────────────────────────────────────────────────────────

class TestIfCompiled:
    def test_if_then_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "if", "cond": True, "then": [{"op": "set", "path": "/r", "value": "yes"}],
                 "else": [{"op": "set", "path": "/r", "value": "no"}]}]
        compiled = engine.compile(spec)
        assert "then" in compiled.steps[0].nested
        assert "else" in compiled.steps[0].nested
        result = compiled.apply(source={}, dest={})
        assert result == {"r": "yes"}

    def test_if_else_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "if", "cond": False, "then": [{"op": "set", "path": "/r", "value": "yes"}],
                 "else": [{"op": "set", "path": "/r", "value": "no"}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={})
        assert result == {"r": "no"}

    def test_if_compiled_matches_dynamic(self):
        engine = build_default_engine()
        spec = [{"op": "if", "path": "/x", "equals": 1,
                 "then": [{"op": "set", "path": "/r", "value": "one"}],
                 "else": [{"op": "set", "path": "/r", "value": "other"}]}]
        compiled = engine.compile(spec)
        r_dyn = engine.apply(spec, source={"x": 1}, dest={})
        r_comp = compiled.apply(source={"x": 1}, dest={})
        assert r_dyn == r_comp == {"r": "one"}

    def test_if_compiled_fallback_no_nested(self):
        engine = build_default_engine()
        spec = [{"op": "if", "cond": True, "then": [{"op": "set", "path": "/r", "value": "yes"}]}]
        compiled = engine.compile(spec)
        compiled.steps[0].nested.clear()
        result = compiled.apply(source={}, dest={})
        assert result == {"r": "yes"}


# ─────────────────────────────────────────────────────────────────────────────
# Nested compiled specs — try
# ─────────────────────────────────────────────────────────────────────────────

class TestTryCompiled:
    def test_try_do_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "try",
                 "do": [{"op": "set", "path": "/r", "value": 1}],
                 "finally": [{"op": "set", "path": "/done", "value": True}]}]
        compiled = engine.compile(spec)
        assert "do" in compiled.steps[0].nested
        assert "finally" in compiled.steps[0].nested
        result = compiled.apply(source={}, dest={})
        assert result == {"r": 1, "done": True}

    def test_try_except_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "try",
                 "do": [{"op": "copy", "from": "/missing", "path": "/r", "ignore_missing": False}],
                 "except": [{"op": "set", "path": "/r", "value": "caught"}]}]
        compiled = engine.compile(spec)
        assert "except" in compiled.steps[0].nested
        result = compiled.apply(source={}, dest={})
        assert result == {"r": "caught"}

    def test_try_compiled_matches_dynamic(self):
        engine = build_default_engine()
        spec = [{"op": "try",
                 "do": [{"op": "set", "path": "/ok", "value": True}],
                 "except": [{"op": "set", "path": "/ok", "value": False}],
                 "finally": [{"op": "set", "path": "/ran", "value": True}]}]
        compiled = engine.compile(spec)
        r_dyn = engine.apply(spec, source={}, dest={})
        r_comp = compiled.apply(source={}, dest={})
        assert r_dyn == r_comp


# ─────────────────────────────────────────────────────────────────────────────
# Nested compiled specs — $def / $func
# ─────────────────────────────────────────────────────────────────────────────

class TestDefCompiled:
    def test_def_body_compiled(self):
        engine = build_default_engine()
        spec = [
            {"$def": "greet", "params": ["name"], "body": [
                {"op": "set", "path": "/msg", "value": "hello ${&:/name}"}
            ]},
            {"$func": "greet", "args": ["world"]},
        ]
        compiled = engine.compile(spec)
        assert "body" in compiled.steps[0].nested
        result = compiled.apply(source={}, dest={})
        assert result == {"msg": "hello world"}

    def test_def_compiled_matches_dynamic(self):
        engine = build_default_engine()
        spec = [
            {"$def": "double", "params": ["n"], "body": [
                {"op": "set", "path": "/result", "value": {"$mul": ["${&:/n}", 2]}}
            ]},
            {"$func": "double", "args": [5]},
        ]
        compiled = engine.compile(spec)
        r_dyn = engine.apply(spec, source={}, dest={})
        r_comp = compiled.apply(source={}, dest={})
        assert r_dyn == r_comp == {"result": 10}

    def test_def_compiled_on_failure(self):
        engine = build_default_engine()
        spec = [
            {"$def": "risky", "params": [],
             "body": [{"$raise": "oops"}],
             "on_failure": [{"op": "set", "path": "/caught", "value": True}]},
            {"$func": "risky", "args": []},
        ]
        compiled = engine.compile(spec)
        assert "on_failure" in compiled.steps[0].nested
        result = compiled.apply(source={}, dest={})
        assert result == {"caught": True}

    def test_def_compiled_fallback_no_nested(self):
        """execute_compiled with empty nested falls back to execute path."""
        engine = build_default_engine()
        spec = [
            {"$def": "add1", "params": ["n"], "body": [
                {"op": "set", "path": "/r", "value": {"$add": ["${&:/n}", 1]}}
            ]},
            {"$func": "add1", "args": [9]},
        ]
        compiled = engine.compile(spec)
        compiled.steps[0].nested.clear()
        result = compiled.apply(source={}, dest={})
        assert result == {"r": 10}


# ─────────────────────────────────────────────────────────────────────────────
# exec — not Compilable, but still works
# ─────────────────────────────────────────────────────────────────────────────

class TestExecCompiled:
    def test_exec_from_compiles_step_but_not_body(self):
        engine = build_default_engine()
        spec = [{"op": "exec", "from": "/actions"}]
        compiled = engine.compile(spec)
        assert compiled is not None
        assert compiled.steps[0].nested == {}

    def test_exec_from_works_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "exec", "from": "/actions"}]
        compiled = engine.compile(spec)
        result = compiled.apply(
            source={"actions": [{"op": "set", "path": "/x", "value": 42}]},
            dest={},
        )
        assert result == {"x": 42}

    def test_exec_inline_actions_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "exec", "actions": [{"op": "set", "path": "/y", "value": 7}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={})
        assert result == {"y": 7}


# ─────────────────────────────────────────────────────────────────────────────
# apply_compiled_to_context
# ─────────────────────────────────────────────────────────────────────────────

class TestApplyCompiledToContext:
    def test_apply_compiled_to_context(self):
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 5}])
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = engine.apply_compiled_to_context(compiled, ctx)
        assert result == {"x": 5}

    def test_apply_compiled_to_context_async(self):
        import asyncio
        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 5}])

        async def run():
            ctx = ExecutionContext(source={}, dest={}, engine=engine)
            return await engine.apply_compiled_to_context_async(compiled, ctx)

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"x": 5}


# ─────────────────────────────────────────────────────────────────────────────
# Async compiled execution
# ─────────────────────────────────────────────────────────────────────────────

class TestAsyncCompiled:
    def test_apply_compiled_async(self):
        import asyncio
        engine = build_default_engine()
        spec = [{"op": "set", "path": "/a", "value": 1}, {"op": "set", "path": "/b", "value": 2}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={}, dest={})

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"a": 1, "b": 2}

    def test_apply_compiled_async_with_foreach(self):
        import asyncio
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "i",
                 "do": [{"op": "set", "path": "/last", "value": "${&:/i}"}]}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={"items": [1, 2, 3]}, dest={})

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"last": 3}


# ─────────────────────────────────────────────────────────────────────────────
# Error propagation in run_compiled
# ─────────────────────────────────────────────────────────────────────────────

class TestErrorPropagation:
    def test_error_in_compiled_raises(self):
        engine = build_default_engine()
        spec = [{"op": "copy", "from": "/missing", "path": "/x", "ignore_missing": False}]
        compiled = engine.compile(spec)
        with pytest.raises(Exception):
            compiled.apply(source={}, dest={})

    def test_operation_limit_in_compiled(self):
        engine = build_default_engine(max_operations=1)
        spec = [
            {"op": "set", "path": "/a", "value": 1},
            {"op": "set", "path": "/b", "value": 2},
        ]
        compiled = engine.compile(spec)
        with pytest.raises(RuntimeError, match="Operation limit exceeded"):
            compiled.apply(source={}, dest={})

    def test_lang_stack_annotated_in_compiled(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "x",
                 "do": [{"op": "copy", "from": "/bad_path", "path": "/out", "ignore_missing": False}]}]
        compiled = engine.compile(spec)
        with pytest.raises(Exception) as exc_info:
            compiled.apply(source={"items": [1]}, dest={})
        assert hasattr(exc_info.value, '_j_perm_lang_stack')


# ─────────────────────────────────────────────────────────────────────────────
# Compilable interface
# ─────────────────────────────────────────────────────────────────────────────

class TestCompilableInterface:
    def test_foreach_handler_is_compilable(self):
        assert isinstance(ForeachHandler(), Compound)

    def test_while_handler_is_compilable(self):
        assert isinstance(WhileHandler(), Compound)

    def test_if_handler_is_compilable(self):
        assert isinstance(IfHandler(), Compound)

    def test_try_handler_is_compilable(self):
        assert isinstance(TryHandler(), Compound)

    def test_def_handler_is_compilable(self):
        assert isinstance(DefHandler(), Compound)

    def test_compilable_default_execute_compiled_falls_back(self):
        """Default execute_compiled calls execute()."""
        class SimpleHandler(ActionHandler, Compound):
            def nested_spec_keys(self, step):
                return []
            def execute(self, step, ctx):
                return {"called": True}

        handler = SimpleHandler()
        result = handler.execute_compiled({}, None, {})
        assert result == {"called": True}

    def test_foreach_nested_spec_keys(self):
        h = ForeachHandler()
        step = {"op": "foreach", "in": "/x", "do": []}
        assert h.nested_spec_keys(step) == ["do"]

    def test_while_nested_spec_keys(self):
        h = WhileHandler()
        assert h.nested_spec_keys({}) == ["do"]

    def test_if_nested_spec_keys_all(self):
        h = IfHandler()
        step = {"op": "if", "cond": True, "then": [], "do": [], "else": []}
        keys = h.nested_spec_keys(step)
        assert set(keys) == {"then", "do", "else"}

    def test_if_nested_spec_keys_partial(self):
        h = IfHandler()
        step = {"op": "if", "cond": True, "then": []}
        assert h.nested_spec_keys(step) == ["then"]

    def test_try_nested_spec_keys_all(self):
        h = TryHandler()
        step = {"op": "try", "do": [], "except": [], "finally": []}
        assert set(h.nested_spec_keys(step)) == {"do", "except", "finally"}

    def test_try_nested_spec_keys_partial(self):
        h = TryHandler()
        step = {"op": "try", "do": []}
        assert h.nested_spec_keys(step) == ["do"]

    def test_def_nested_spec_keys_with_on_failure(self):
        h = DefHandler()
        step = {"$def": "f", "body": [], "on_failure": []}
        assert set(h.nested_spec_keys(step)) == {"body", "on_failure"}

    def test_def_nested_spec_keys_without_on_failure(self):
        h = DefHandler()
        step = {"$def": "f", "body": []}
        assert h.nested_spec_keys(step) == ["body"]


# ─────────────────────────────────────────────────────────────────────────────
# run_compiled with trace_logging
# ─────────────────────────────────────────────────────────────────────────────

class TestRunCompiledTrace:
    def test_trace_logging_in_compiled(self, caplog):
        engine = build_default_engine(trace_logging=True)
        spec = [{"op": "set", "path": "/x", "value": 1}]
        compiled = engine.compile(spec)
        with caplog.at_level(logging.DEBUG, logger="j_perm"):
            compiled.apply(source={}, dest={})
        assert any("'op': 'set'" in r.message for r in caplog.records)

    def test_no_trace_without_flag(self):
        engine = build_default_engine(trace_logging=False)
        spec = [{"op": "set", "path": "/x", "value": 1}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={})
        assert result == {"x": 1}


# ─────────────────────────────────────────────────────────────────────────────
# Non-exclusive matchers
# ─────────────────────────────────────────────────────────────────────────────

class TestNonExclusiveMatchers:
    def test_non_exclusive_matchers_compiled(self):
        """Multiple handlers from non-exclusive nodes are all stored and run."""
        results = []

        class HandlerA(ActionHandler):
            def execute(self, step, ctx):
                results.append("A")
                return ctx.dest

        class HandlerB(ActionHandler):
            def execute(self, step, ctx):
                results.append("B")
                return ctx.dest

        reg = ActionTypeRegistry()
        reg.register(ActionNode(
            name="ab", priority=10,
            matcher=AlwaysMatcher(),
            handler=HandlerA(),
            exclusive=False,
        ))
        reg.register(ActionNode(
            name="b", priority=5,
            matcher=AlwaysMatcher(),
            handler=HandlerB(),
            exclusive=True,
        ))

        engine = build_default_engine()
        pipeline = Pipeline(registry=reg, track_execution=True)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        compiled = pipeline.compile({"step": True}, ctx)
        assert len(compiled.steps[0].handlers) == 2

        results.clear()
        pipeline.run_compiled(compiled, ctx)
        assert results == ["A", "B"]


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline.compile with unhandled step
# ─────────────────────────────────────────────────────────────────────────────

class TestCompileUnhandledStep:
    def test_compile_raises_on_unhandled_step(self):
        engine = build_default_engine()
        reg = ActionTypeRegistry()  # empty — nothing matches
        pipeline = Pipeline(registry=reg)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        with pytest.raises(ValueError, match="unhandled step during compilation"):
            pipeline.compile([{"op": "set", "path": "/x", "value": 1}], ctx)


# ─────────────────────────────────────────────────────────────────────────────
# run_compiled edge cases (middleware, op limit, PipelineSignal, ControlFlow)
# ─────────────────────────────────────────────────────────────────────────────

class TestRunCompiledEdgeCases:
    def test_middleware_runs_in_compiled(self):
        from j_perm import Middleware
        called = []

        class RecordMW(Middleware):
            name = "record"
            priority = 0
            def process(self, step, ctx):
                called.append(step)
                return step

        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        engine.main_pipeline.register_middleware(RecordMW())
        compiled.apply(source={}, dest={})
        assert len(called) > 0
        engine.main_pipeline._middlewares.clear()

    def test_operation_limit_in_run_compiled(self):
        engine = build_default_engine(max_operations=0)
        spec = [{"op": "set", "path": "/x", "value": 1}]
        compiled = engine.compile(spec)
        with pytest.raises(RuntimeError, match="Operation limit exceeded"):
            compiled.apply(source={}, dest={})

    def test_pipeline_signal_in_run_compiled(self):
        from j_perm import PipelineSignal, ActionMatcher

        class QuietSignal(PipelineSignal):
            """A PipelineSignal that silently updates dest without re-raising."""
            def __init__(self, value):
                self.value = value
                super().__init__()
            def handle(self, ctx):
                ctx.dest = self.value  # no re-raise → swallowed by Pipeline

        class SignalHandler(ActionHandler):
            def execute(self, step, ctx):
                raise QuietSignal(42)

        class SignalMatcher(ActionMatcher):
            def matches(self, step):
                return isinstance(step, dict) and step.get("op") == "signal_test"

        engine = build_default_engine()
        reg = ActionTypeRegistry()
        reg.register(ActionNode(name="sig", priority=10, matcher=SignalMatcher(), handler=SignalHandler()))
        pipeline = Pipeline(registry=reg, track_execution=True)
        ctx = ExecutionContext(source={}, dest="initial", engine=engine)
        compiled = pipeline.compile({"op": "signal_test"}, ctx)
        pipeline.run_compiled(compiled, ctx)
        assert ctx.dest == 42

    def test_control_flow_signal_propagates_in_run_compiled(self):
        from j_perm.handlers.signals import BreakSignal
        engine = build_default_engine()
        spec = [{"$break": None}]
        compiled = engine.compile(spec)
        with pytest.raises(BreakSignal):
            compiled.apply(source={}, dest={})

    def test_run_compiled_no_track_execution(self):
        """run_compiled with track_execution=False — lang_stack=None branch."""
        engine = build_default_engine()
        reg = ActionTypeRegistry()
        reg.register(ActionNode(
            name="set", priority=10,
            matcher=AlwaysMatcher(),
            handler=build_default_engine().main_pipeline.registry.resolve({"op": "set"})[0],
        ))
        pipeline = Pipeline(registry=reg, track_execution=False)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        compiled = pipeline.compile({"op": "set", "path": "/x", "value": 5}, ctx)
        pipeline.run_compiled(compiled, ctx)
        assert ctx.dest == {"x": 5}

    def test_exception_annotated_with_lang_stack_in_run_compiled(self):
        """Exceptions are annotated with _j_perm_lang_stack in run_compiled."""
        engine = build_default_engine()
        spec = [{"op": "copy", "from": "/missing", "path": "/x", "ignore_missing": False}]
        compiled = engine.compile(spec)
        with pytest.raises(Exception) as exc_info:
            compiled.apply(source={}, dest={})
        assert hasattr(exc_info.value, '_j_perm_lang_stack')


# ─────────────────────────────────────────────────────────────────────────────
# run_compiled_async edge cases
# ─────────────────────────────────────────────────────────────────────────────

class TestRunCompiledAsyncEdgeCases:
    def test_async_middleware_in_run_compiled_async(self):
        import asyncio
        from j_perm import AsyncMiddleware
        called = []

        class AsyncRecordMW(AsyncMiddleware):
            name = "async_record"
            priority = 0
            async def process(self, step, ctx):
                called.append(step)
                return step

        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        engine.main_pipeline.register_middleware(AsyncRecordMW())

        async def run():
            return await compiled.apply_async(source={}, dest={})

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"x": 1}
        assert len(called) > 0
        engine.main_pipeline._middlewares.clear()

    def test_sync_middleware_in_run_compiled_async(self):
        import asyncio
        from j_perm import Middleware
        called = []

        class SyncMW(Middleware):
            name = "sync_mw"
            priority = 0
            def process(self, step, ctx):
                called.append(True)
                return step

        engine = build_default_engine()
        compiled = engine.compile([{"op": "set", "path": "/x", "value": 1}])
        engine.main_pipeline.register_middleware(SyncMW())

        async def run():
            return await compiled.apply_async(source={}, dest={})

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"x": 1}
        assert called
        engine.main_pipeline._middlewares.clear()

    def test_operation_limit_in_run_compiled_async(self):
        import asyncio
        engine = build_default_engine(max_operations=0)
        spec = [{"op": "set", "path": "/x", "value": 1}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={}, dest={})

        with pytest.raises(RuntimeError, match="Operation limit exceeded"):
            asyncio.get_event_loop().run_until_complete(run())

    def test_run_compiled_async_no_track_execution(self):
        import asyncio
        engine = build_default_engine()
        reg = ActionTypeRegistry()
        reg.register(ActionNode(
            name="set", priority=10,
            matcher=AlwaysMatcher(),
            handler=build_default_engine().main_pipeline.registry.resolve({"op": "set"})[0],
        ))
        pipeline = Pipeline(registry=reg, track_execution=False)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        compiled = pipeline.compile({"op": "set", "path": "/x", "value": 5}, ctx)

        async def run():
            await pipeline.run_compiled_async(compiled, ctx)

        asyncio.get_event_loop().run_until_complete(run())
        assert ctx.dest == {"x": 5}

    def test_async_handler_in_run_compiled_async(self):
        """AsyncActionHandler (without Compilable) in run_compiled_async."""
        import asyncio
        from j_perm import AsyncActionHandler, AsyncMiddleware

        class AsyncSetHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest = {"async": True}
                return ctx.dest

        engine = build_default_engine()
        reg = ActionTypeRegistry()
        reg.register(ActionNode(
            name="async_set", priority=10,
            matcher=AlwaysMatcher(),
            handler=AsyncSetHandler(),
        ))
        pipeline = Pipeline(registry=reg, track_execution=True)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        compiled = pipeline.compile({"async": "step"}, ctx)

        async def run():
            await pipeline.run_compiled_async(compiled, ctx)

        asyncio.get_event_loop().run_until_complete(run())
        assert ctx.dest == {"async": True}

    def test_async_compilable_handler_in_run_compiled_async(self):
        """AsyncActionHandler + Compilable in run_compiled_async uses execute() not execute_compiled()."""
        import asyncio
        from j_perm import AsyncActionHandler

        class AsyncCompoundHandler(AsyncActionHandler, Compound):
            def nested_spec_keys(self, step):
                return []
            async def execute(self, step, ctx):
                ctx.dest = {"async_compilable": True}
                return ctx.dest

        engine = build_default_engine()
        reg = ActionTypeRegistry()
        reg.register(ActionNode(
            name="ac", priority=10,
            matcher=AlwaysMatcher(),
            handler=AsyncCompoundHandler(),
        ))
        pipeline = Pipeline(registry=reg, track_execution=True)
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        # compile with empty nested — use a handler that IS Compilable but has no nested specs
        compiled = pipeline.compile({"async": "compilable"}, ctx)
        # Manually add a non-empty nested to force the Compilable+nested branch
        compiled.steps[0].nested["dummy"] = engine.compile([{"op": "set", "path": "/x", "value": 1}])

        async def run():
            await pipeline.run_compiled_async(compiled, ctx)

        asyncio.get_event_loop().run_until_complete(run())
        assert ctx.dest == {"async_compilable": True}

    def test_apply_compiled_async_error_logs_lang_stack(self, caplog):
        import asyncio
        engine = build_default_engine()
        spec = [{"op": "copy", "from": "/missing", "path": "/x", "ignore_missing": False}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={}, dest={})

        with caplog.at_level(logging.ERROR, logger="j_perm"):
            with pytest.raises(Exception):
                asyncio.get_event_loop().run_until_complete(run())
        assert any("j-perm execution failed" in r.message for r in caplog.records)

    def test_control_flow_signal_in_run_compiled_async(self):
        import asyncio
        from j_perm.handlers.signals import BreakSignal
        engine = build_default_engine()
        spec = [{"$break": None}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={}, dest={})

        with pytest.raises(BreakSignal):
            asyncio.get_event_loop().run_until_complete(run())

    def test_exception_annotated_in_run_compiled_async(self):
        import asyncio
        engine = build_default_engine()
        spec = [{"op": "copy", "from": "/missing", "path": "/x", "ignore_missing": False}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={}, dest={})

        with pytest.raises(Exception) as exc_info:
            asyncio.get_event_loop().run_until_complete(run())
        assert hasattr(exc_info.value, '_j_perm_lang_stack')

    def test_trace_logging_in_run_compiled_async(self, caplog):
        """trace_logging branch in run_compiled_async (core.py:1127)."""
        import asyncio
        engine = build_default_engine(trace_logging=True)
        spec = [{"op": "set", "path": "/x", "value": 1}]
        compiled = engine.compile(spec)

        async def run():
            return await compiled.apply_async(source={}, dest={})

        with caplog.at_level(logging.DEBUG, logger="j_perm"):
            result = asyncio.get_event_loop().run_until_complete(run())
        assert result == {"x": 1}
        assert any("'op': 'set'" in r.message for r in caplog.records)

    def test_pipeline_signal_in_run_compiled_async(self):
        """PipelineSignal.handle() called in run_compiled_async (core.py:1143)."""
        import asyncio
        from j_perm import PipelineSignal, ActionMatcher

        class QuietSignal(PipelineSignal):
            def __init__(self, value):
                self.value = value
                super().__init__()
            def handle(self, ctx):
                ctx.dest = self.value

        class SignalHandler(ActionHandler):
            def execute(self, step, ctx):
                raise QuietSignal(99)

        class SignalMatcher(ActionMatcher):
            def matches(self, step):
                return isinstance(step, dict) and step.get("op") == "async_signal"

        engine = build_default_engine()
        reg = ActionTypeRegistry()
        reg.register(ActionNode(name="sig", priority=10, matcher=SignalMatcher(), handler=SignalHandler()))
        pipeline = Pipeline(registry=reg, track_execution=True)
        ctx_compile = ExecutionContext(source={}, dest={}, engine=engine)
        compiled = pipeline.compile({"op": "async_signal"}, ctx_compile)

        async def run():
            ctx = ExecutionContext(source={}, dest="initial", engine=engine)
            await pipeline.run_compiled_async(compiled, ctx)
            return ctx.dest

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == 99


# ─────────────────────────────────────────────────────────────────────────────
# execute_compiled edge cases — ForeachHandler
# ─────────────────────────────────────────────────────────────────────────────

class TestForeachExecuteCompiledEdgeCases:
    def _compile_foreach(self, engine, spec):
        """Compile spec and return the compiled spec with non-empty nested."""
        return engine.compile(spec)

    def test_execute_compiled_both_in_and_in_value_raises(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/x", "as": "i",
                 "do": [{"op": "set", "path": "/r", "value": 1}]}]
        compiled = engine.compile(spec)
        compiled.steps[0].step["in_value"] = [1]  # add in_value to normalized step
        with pytest.raises(ValueError, match="cannot have both"):
            compiled.apply(source={}, dest={})

    def test_execute_compiled_neither_in_nor_in_value_raises(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/x", "as": "i",
                 "do": [{"op": "set", "path": "/r", "value": 1}]}]
        compiled = engine.compile(spec)
        del compiled.steps[0].step["in"]  # remove 'in' from step
        with pytest.raises(ValueError, match="requires either"):
            compiled.apply(source={}, dest={})

    def test_execute_compiled_default_fallback(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/missing", "as": "i", "default": [10, 20],
                 "do": [{"op": "set", "path": "/last", "value": "${&:/i}"}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={})
        assert result == {"last": 20}

    def test_execute_compiled_skip_empty(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/items", "as": "i", "skip_empty": True,
                 "do": [{"op": "set", "path": "/ran", "value": True}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={"items": []}, dest={})
        assert result == {}

    def test_execute_compiled_dict_source(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in": "/mapping", "as": "pair",
                 "do": [{"op": "set", "path": "/last", "value": "${&:/pair}"}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={"mapping": {"a": 1}}, dest={})
        assert result == {"last": ["a", 1]}

    def test_execute_compiled_max_items_exceeded(self):
        engine = build_default_engine(max_foreach_items=2)
        spec = [{"op": "foreach", "in": "/items", "as": "i",
                 "do": [{"op": "set", "path": "/r", "value": 1}]}]
        compiled = engine.compile(spec)
        with pytest.raises(ValueError, match="exceeds maximum"):
            compiled.apply(source={"items": [1, 2, 3]}, dest={})

    def test_execute_compiled_break_signal(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in_value": [1, 2, 3], "as": "i",
                 "do": [{"op": "set", "path": "/last", "value": "${&:/i}"},
                        {"$break": None}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={})
        assert result == {"last": 1}

    def test_execute_compiled_continue_signal(self):
        engine = build_default_engine()
        spec = [{"op": "foreach", "in_value": [1, 2, 3], "as": "i",
                 "do": [{"op": "if", "cond": {"$eq": ["${&:/i}", 2]},
                         "then": [{"$continue": None}]},
                        {"op": "set", "path": "/result/-", "value": "${&:/i}"}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={"result": []})
        assert result == {"result": [1, 3]}

    def test_execute_compiled_return_signal_propagates(self):
        from j_perm.handlers.signals import ReturnSignal
        engine = build_default_engine()
        spec = [{"op": "foreach", "in_value": [1], "as": "i",
                 "do": [{"$return": 99}]}]
        compiled = engine.compile(spec)
        with pytest.raises(ReturnSignal):
            compiled.apply(source={}, dest={})

    def test_execute_compiled_foreach_fallback_no_do_in_nested(self):
        """execute_compiled foreach: nested non-empty but no 'do' key → fallback (line 316)."""
        engine = build_default_engine()
        spec = [{"op": "foreach", "in_value": [1, 2], "as": "i",
                 "do": [{"op": "set", "path": "/last", "value": "${&:/i}"}]}]
        compiled = engine.compile(spec)
        # Replace "do" with another key so nested is non-empty but has no "do"
        do_compiled = compiled.steps[0].nested.pop("do")
        compiled.steps[0].nested["other"] = do_compiled
        # execute_compiled is called (nested non-empty), compiled_body=None → line 316
        result = compiled.apply(source={}, dest={})
        assert result == {"last": 2}


# ─────────────────────────────────────────────────────────────────────────────
# execute_compiled edge cases — WhileHandler
# ─────────────────────────────────────────────────────────────────────────────

class TestWhileExecuteCompiledEdgeCases:
    def test_execute_compiled_while_fallback_no_compiled_body(self):
        """execute_compiled falls back to apply_to_context when nested cleared."""
        engine = build_default_engine()
        spec = [{"op": "while", "cond": {"$lt": ["${@:/n}", 2]},
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}}]}]
        compiled = engine.compile(spec)
        # Manually clear the nested body to force fallback path
        compiled.steps[0].nested.clear()
        result = compiled.apply(source={}, dest={"n": 0})
        assert result == {"n": 2}

    def test_execute_compiled_while_break(self):
        engine = build_default_engine()
        spec = [{"op": "while", "cond": True,
                 "do": [{"op": "set", "path": "/n", "value": 1},
                        {"$break": None}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={"n": 0})
        assert result == {"n": 1}

    def test_execute_compiled_while_continue(self):
        engine = build_default_engine()
        spec = [{"op": "while", "cond": {"$lt": ["${@:/n}", 3]},
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}},
                        {"$continue": None}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={"n": 0})
        assert result == {"n": 3}

    def test_execute_compiled_while_return_propagates(self):
        from j_perm.handlers.signals import ReturnSignal
        engine = build_default_engine()
        spec = [{"op": "while", "cond": True,
                 "do": [{"$return": 42}]}]
        compiled = engine.compile(spec)
        with pytest.raises(ReturnSignal):
            compiled.apply(source={}, dest={})

    def test_execute_compiled_while_error_rollback(self):
        engine = build_default_engine()
        spec = [{"op": "while", "cond": True,
                 "do": [{"op": "set", "path": "/n", "value": 1},
                        {"op": "copy", "from": "/bad", "path": "/x", "ignore_missing": False}]}]
        compiled = engine.compile(spec)
        with pytest.raises(Exception):
            compiled.apply(source={}, dest={"n": 0})

    def test_execute_compiled_while_max_iterations_exceeded(self):
        """Max iterations error in execute_compiled (line 424)."""
        engine = build_default_engine(max_loop_iterations=2)
        spec = [{"op": "while", "cond": True,
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}}]}]
        compiled = engine.compile(spec)
        with pytest.raises(RuntimeError, match="exceeded maximum iterations"):
            compiled.apply(source={}, dest={"n": 0})

    def test_execute_compiled_while_fallback_no_do_in_nested(self):
        """execute_compiled while: nested non-empty but no 'do' key → fallback (line 434)."""
        engine = build_default_engine()
        spec = [{"op": "while", "cond": {"$lt": ["${@:/n}", 2]},
                 "do": [{"op": "set", "path": "/n", "value": {"$add": ["${@:/n}", 1]}}]}]
        compiled = engine.compile(spec)
        # Replace "do" with a different key so nested is non-empty but has no "do"
        do_compiled = compiled.steps[0].nested.pop("do")
        compiled.steps[0].nested["other"] = do_compiled
        # Now execute_compiled is called (nested non-empty), compiled_body=None → line 434
        result = compiled.apply(source={}, dest={"n": 0})
        assert result == {"n": 2}


# ─────────────────────────────────────────────────────────────────────────────
# execute_compiled edge cases — IfHandler
# ─────────────────────────────────────────────────────────────────────────────

class TestIfExecuteCompiledEdgeCases:
    def test_execute_compiled_if_no_matching_branch(self):
        """Condition false, no else → returns dest unchanged (line 527)."""
        engine = build_default_engine()
        spec = [{"op": "if", "cond": False,
                 "then": [{"op": "set", "path": "/r", "value": "yes"}]}]
        compiled = engine.compile(spec)
        result = compiled.apply(source={}, dest={"original": True})
        assert result == {"original": True}

    def test_execute_compiled_if_fallback_to_apply_to_context(self):
        """Nested has one branch but we take the other → fallback (line 534)."""
        engine = build_default_engine()
        spec = [{"op": "if", "cond": False,
                 "then": [{"op": "set", "path": "/r", "value": "yes"}],
                 "else": [{"op": "set", "path": "/r", "value": "no"}]}]
        compiled = engine.compile(spec)
        # Remove "else" from nested, keep "then" so nested is non-empty
        del compiled.steps[0].nested["else"]
        # Condition is False → branch_key = "else" → not in nested → apply_to_context fallback
        result = compiled.apply(source={}, dest={})
        assert result == {"r": "no"}

    def test_execute_compiled_if_exception_rolls_back(self):
        engine = build_default_engine()
        spec = [{"op": "if", "cond": False,
                 "then": [{"op": "set", "path": "/r", "value": "yes"}],
                 "else": [{"op": "copy", "from": "/bad", "path": "/r", "ignore_missing": False}]}]
        compiled = engine.compile(spec)
        with pytest.raises(Exception):
            compiled.apply(source={}, dest={"r": "original"})

    def test_execute_compiled_if_control_flow_propagates(self):
        from j_perm.handlers.signals import BreakSignal
        engine = build_default_engine()
        spec = [{"op": "foreach", "in_value": [1], "as": "i",
                 "do": [{"op": "if", "cond": True,
                         "then": [{"$break": None}]}]}]
        compiled = engine.compile(spec)
        # Break inside if inside foreach should propagate correctly
        result = compiled.apply(source={}, dest={"r": 0})
        assert result == {"r": 0}  # foreach completed with break, dest unchanged