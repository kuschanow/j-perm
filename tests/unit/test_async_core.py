"""Tests for async infrastructure."""

import logging

import pytest
from j_perm import (
    ExecutionContext,
    Engine,
    Pipeline,
    StageNode,
    StageRegistry,
    StageProcessor,
    ActionNode,
    ActionTypeRegistry,
    ActionMatcher,
    ActionHandler,
    PointerResolver,
    PipelineSignal,
    UnescapeRule,
)
from j_perm.core import AsyncStageProcessor, AsyncMiddleware, AsyncActionHandler
from j_perm.processors.pointer_processor import PointerProcessor


class TestAsyncStageProcessor:
    """Test AsyncStageProcessor with StageRegistry.run_all_async()."""

    @pytest.mark.asyncio
    async def test_async_stage_processor(self):
        """AsyncStageProcessor can be executed via run_all_async()."""

        class AsyncDoubleStage(AsyncStageProcessor):
            async def apply(self, steps, ctx):
                # Simulate async work
                return [s * 2 for s in steps]

        registry = StageRegistry()
        registry.register(
            StageNode(name="double", priority=10, processor=AsyncDoubleStage())
        )

        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await registry.run_all_async([1, 2, 3], ctx)

        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_sync_stage_in_async_context(self):
        """Sync StageProcessor works in run_all_async()."""

        class SyncDoubleStage(StageProcessor):
            def apply(self, steps, ctx):
                return [s * 2 for s in steps]

        registry = StageRegistry()
        registry.register(
            StageNode(name="double", priority=10, processor=SyncDoubleStage())
        )

        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await registry.run_all_async([1, 2, 3], ctx)

        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_mixed_sync_and_async_stages(self):
        """Can mix sync and async stages in run_all_async()."""
        executed = []

        class SyncStage(StageProcessor):
            def apply(self, steps, ctx):
                executed.append("sync")
                return steps

        class AsyncStage(AsyncStageProcessor):
            async def apply(self, steps, ctx):
                executed.append("async")
                return steps

        registry = StageRegistry()
        registry.register(StageNode("sync", priority=100, processor=SyncStage()))
        registry.register(StageNode("async", priority=50, processor=AsyncStage()))

        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        await registry.run_all_async([], ctx)

        assert executed == ["sync", "async"]


class TestAsyncMiddleware:
    """Test AsyncMiddleware with Pipeline.run_async()."""

    @pytest.mark.asyncio
    async def test_async_middleware(self):
        """AsyncMiddleware can be executed in run_async()."""
        processed = []

        class AsyncTestMiddleware(AsyncMiddleware):
            name = "test"
            priority = 10

            async def process(self, step, ctx):
                processed.append("async_mw")
                return step

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class TestHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                processed.append("handler")
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("test", 10, TestMatcher(), handler=TestHandler())
        )

        pipeline = Pipeline(registry=registry, middlewares=[AsyncTestMiddleware()])
        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        await pipeline.run_async({"op": "test"}, ctx)
        assert processed == ["async_mw", "handler"]

    @pytest.mark.asyncio
    async def test_sync_middleware_in_async_pipeline(self):
        """Sync Middleware works in run_async()."""
        from j_perm.core import Middleware

        processed = []

        class SyncTestMiddleware(Middleware):
            name = "test"
            priority = 10

            def process(self, step, ctx):
                processed.append("sync_mw")
                return step

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class TestHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                processed.append("handler")
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("test", 10, TestMatcher(), handler=TestHandler())
        )

        pipeline = Pipeline(registry=registry, middlewares=[SyncTestMiddleware()])
        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        await pipeline.run_async({"op": "test"}, ctx)
        assert processed == ["sync_mw", "handler"]


class TestAsyncActionHandler:
    """Test AsyncActionHandler with Pipeline.run_async()."""

    @pytest.mark.asyncio
    async def test_async_handler_execution(self):
        """AsyncActionHandler can be executed via run_async()."""

        class AsyncTestHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["async_executed"] = True
                return ctx.dest

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return step.get("op") == "test"

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("test", 10, TestMatcher(), handler=AsyncTestHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        await pipeline.run_async({"op": "test"}, ctx)
        assert ctx.dest == {"async_executed": True}

    @pytest.mark.asyncio
    async def test_sync_handler_in_async_pipeline(self):
        """Sync ActionHandler works in run_async()."""

        class SyncTestHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["sync_executed"] = True
                return ctx.dest

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return step.get("op") == "test"

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("test", 10, TestMatcher(), handler=SyncTestHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        await pipeline.run_async({"op": "test"}, ctx)
        assert ctx.dest == {"sync_executed": True}

    @pytest.mark.asyncio
    async def test_mixed_sync_and_async_handlers(self):
        """Can mix sync and async handlers in run_async()."""

        class AsyncHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["async"] = True
                return ctx.dest

        class SyncHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["sync"] = True
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("async", 100, AlwaysMatcher(), handler=AsyncHandler(), exclusive=False)
        )
        registry.register(
            ActionNode("sync", 50, AlwaysMatcher(), handler=SyncHandler(), exclusive=False)
        )

        pipeline = Pipeline(registry=registry)
        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        await pipeline.run_async({}, ctx)
        assert ctx.dest == {"async": True, "sync": True}


class TestEngineAsync:
    """Test Engine async methods."""

    @pytest.mark.asyncio
    async def test_apply_async_returns_deep_copy(self):
        """apply_async() should return deep copy of result."""

        class AsyncSetHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["key"] = "value"
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("set", 10, AlwaysMatcher(), handler=AsyncSetHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        dest = {}
        result = await engine.apply_async({}, source={}, dest=dest)

        assert result == {"key": "value"}
        assert dest == {}  # original unchanged

    @pytest.mark.asyncio
    async def test_process_value_async_with_no_value_pipeline(self):
        """process_value_async with no value_pipeline returns value as-is."""

        class DummyHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("dummy", 10, AlwaysMatcher(), handler=DummyHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
            value_pipeline=None,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await engine.process_value_async("test", ctx)

        assert result == "test"

    @pytest.mark.asyncio
    async def test_process_value_async_stabilization(self):
        """process_value_async runs value pipeline until stabilization."""
        iterations = []

        class CountHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                iterations.append(len(iterations))
                # Stabilize after 3 iterations
                if len(iterations) >= 3:
                    return ctx.dest
                return ctx.dest + 1

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        value_registry = ActionTypeRegistry()
        value_registry.register(
            ActionNode("count", 10, AlwaysMatcher(), handler=CountHandler())
        )
        value_pipeline = Pipeline(registry=value_registry)

        main_registry = ActionTypeRegistry()
        main_registry.register(
            ActionNode("dummy", 10, AlwaysMatcher(), handler=CountHandler())
        )
        main_pipeline = Pipeline(registry=main_registry)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await engine.process_value_async(0, ctx)

        assert len(iterations) == 3
        assert result == 2

    @pytest.mark.asyncio
    async def test_apply_to_context_async_mutates_context(self):
        """apply_to_context_async() mutates the provided context's dest."""

        class AsyncSetHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["key"] = "value"
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("set", 10, AlwaysMatcher(), handler=AsyncSetHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        ctx = ExecutionContext(
            source={},
            dest={},
            engine=engine,
        )

        result = await engine.apply_to_context_async({}, ctx)

        # The context's dest was mutated
        assert ctx.dest == {"key": "value"}
        # apply_to_context_async returns deep copy
        assert result == {"key": "value"}
        assert result is not ctx.dest

    @pytest.mark.asyncio
    async def test_run_pipeline_async_with_isolated_dest(self):
        """run_pipeline_async() uses isolated dest copy."""

        class AsyncSetHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["from_pipeline"] = True
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("set", 10, AlwaysMatcher(), handler=AsyncSetHandler())
        )

        named_pipeline = Pipeline(registry=registry)
        main_pipeline = Pipeline(registry=registry)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            pipelines={"named": named_pipeline},
        )

        ctx = ExecutionContext(
            source={},
            dest={"original": True},
            engine=engine,
        )

        result = await engine.run_pipeline_async("named", {}, ctx)

        # Named pipeline result includes new key
        assert result == {"original": True, "from_pipeline": True}
        # Original context dest unchanged
        assert ctx.dest == {"original": True}

    @pytest.mark.asyncio
    async def test_sync_and_async_handlers_in_same_pipeline(self):
        """Can use both sync and async handlers in apply_async()."""
        executed = []

        class SyncHandler(ActionHandler):
            def execute(self, step, ctx):
                executed.append("sync")
                ctx.dest["sync"] = True
                return ctx.dest

        class AsyncHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                executed.append("async")
                ctx.dest["async"] = True
                return ctx.dest

        class OpMatcher(ActionMatcher):
            def __init__(self, op):
                self.op = op

            def matches(self, step):
                return step.get("op") == self.op

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("sync", 10, OpMatcher("sync"), handler=SyncHandler())
        )
        registry.register(
            ActionNode("async", 10, OpMatcher("async"), handler=AsyncHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        result = await engine.apply_async(
            [{"op": "sync"}, {"op": "async"}],
            source={},
            dest={}
        )

        assert result == {"sync": True, "async": True}
        assert executed == ["sync", "async"]


class TestStageRegistryAsyncExtended:
    """Extended async tests for StageRegistry with children."""

    @pytest.mark.asyncio
    async def test_run_all_async_with_children(self):
        """run_all_async() with children node runs child registry (line 353)."""
        executed = []

        class RecordStage(StageProcessor):
            def __init__(self, name):
                self.name = name

            def apply(self, steps, ctx):
                executed.append(self.name)
                return steps

        child_reg = StageRegistry()
        child_reg.register(StageNode("child", 10, processor=RecordStage("child")))

        parent_reg = StageRegistry()
        parent_reg.register(StageNode(
            name="parent",
            priority=10,
            processor=RecordStage("parent"),
            children=child_reg,
        ))

        ctx = ExecutionContext(source={}, dest={}, engine=object())
        await parent_reg.run_all_async([], ctx)

        assert "child" in executed
        assert "parent" in executed


class TestAsyncPipelineExtended:
    """Extended tests for async pipeline execution."""

    @pytest.mark.asyncio
    async def test_async_pipeline_unhandled_step_raises(self):
        """Async pipeline raises ValueError for unhandled step (line 776)."""

        class NeverMatcher(ActionMatcher):
            def matches(self, step):
                return False

        class DummyHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("dummy", 10, NeverMatcher(), handler=DummyHandler()))

        pipeline = Pipeline(registry=registry)
        ctx = ExecutionContext(source={}, dest={}, engine=object())

        with pytest.raises(ValueError, match="unhandled step"):
            await pipeline.run_async({"op": "test"}, ctx)

    @pytest.mark.asyncio
    async def test_async_pipeline_control_flow_propagates(self):
        """Async pipeline re-raises ControlFlowSignal (line 807)."""
        from j_perm import BreakSignal

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class AsyncBreakHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                raise BreakSignal()

        registry = ActionTypeRegistry()
        registry.register(ActionNode("break", 10, AlwaysMatcher(), handler=AsyncBreakHandler()))

        pipeline = Pipeline(registry=registry)
        ctx = ExecutionContext(source={}, dest={}, engine=object())

        with pytest.raises(BreakSignal):
            await pipeline.run_async({}, ctx)

    @pytest.mark.asyncio
    async def test_async_trace_logging(self):
        """Async pipeline emits debug log with trace_logging=True."""
        from j_perm import build_default_engine

        engine = build_default_engine(trace_logging=True, trace_repr_max=50)
        log = logging.getLogger("j_perm")
        original_level = log.level
        log.setLevel(logging.DEBUG)
        try:
            result = await engine.apply_async(
                {"op": "set", "path": "/x", "value": 42},
                source={}, dest={},
            )
            assert result == {"x": 42}
        finally:
            log.setLevel(original_level)

    @pytest.mark.asyncio
    async def test_process_value_async_pipeline_signal_from_stage(self):
        """process_value_async catches PipelineSignal from stage (lines 1147-1149)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class IdentityHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                return ctx.dest

        class SignalStage(StageProcessor):
            """Stage that raises PipelineSignal to break async stabilization loop."""
            def apply(self, steps, ctx):
                class StopSignal(PipelineSignal):
                    def handle(self, ctx):
                        ctx.dest = "async_stopped"
                raise StopSignal()

        value_reg = ActionTypeRegistry()
        value_reg.register(ActionNode("id", 10, AlwaysMatcher(), handler=IdentityHandler()))
        value_stages = StageRegistry()
        value_stages.register(StageNode("stop", 10, processor=SignalStage()))
        value_pipeline = Pipeline(registry=value_reg, stages=value_stages)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_reg = ActionTypeRegistry()
        main_reg.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_reg)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await engine.process_value_async("input", ctx)
        # PipelineSignal from stage breaks loop early; current stays at initial value
        assert result == "input"

    @pytest.mark.asyncio
    async def test_async_pipeline_trace_logging(self):
        """Async pipeline with trace_logging emits debug log (lines 789-794)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class AsyncTestHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["done"] = True
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("test", 10, AlwaysMatcher(), handler=AsyncTestHandler()))

        engine_obj = type("E", (), {
            "trace_logging": True,
            "trace_repr_max": 200,
        })()

        pipeline = Pipeline(registry=registry, track_execution=True)
        ctx = ExecutionContext(source={}, dest={}, engine=engine_obj)

        log = logging.getLogger("j_perm")
        original_level = log.level
        log.setLevel(logging.DEBUG)
        try:
            await pipeline.run_async({"op": "test"}, ctx)
        finally:
            log.setLevel(original_level)

        assert ctx.dest == {"done": True}

    @pytest.mark.asyncio
    async def test_async_pipeline_exception_annotates_lang_stack(self):
        """Exception in async handler annotates lang stack (lines 804-814)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class AsyncFailHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                raise ValueError("async fail")

        registry = ActionTypeRegistry()
        registry.register(ActionNode("fail", 10, AlwaysMatcher(), handler=AsyncFailHandler()))

        pipeline = Pipeline(registry=registry, track_execution=True)
        engine_obj = type("E", (), {
            "trace_logging": False,
            "trace_repr_max": 200,
        })()

        ctx = ExecutionContext(source={}, dest={}, engine=engine_obj)
        with pytest.raises(ValueError, match="async fail"):
            await pipeline.run_async({}, ctx)

    @pytest.mark.asyncio
    async def test_async_pipeline_operation_count_exceed(self):
        """Async pipeline raises when operation limit exceeded (lines 776-785)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class AsyncTestHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("test", 10, AlwaysMatcher(), handler=AsyncTestHandler()))

        engine_obj = type("E", (), {
            "trace_logging": False,
            "trace_repr_max": 200,
            "max_operations": 0,
        })()

        pipeline = Pipeline(registry=registry)
        ctx = ExecutionContext(source={}, dest={}, engine=engine_obj)
        with pytest.raises(RuntimeError, match="Operation limit exceeded"):
            await pipeline.run_async({}, ctx)


class TestEngineAsyncExtended:
    """Extended tests for Engine async methods."""

    @pytest.mark.asyncio
    async def test_apply_async_exception_with_lang_stack(self):
        """apply_async() logs error when exception has lang_stack (lines 960-970)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class FailHandler(ActionHandler):
            def execute(self, step, ctx):
                raise ValueError("test error")

        registry = ActionTypeRegistry()
        registry.register(ActionNode("fail", 10, AlwaysMatcher(), handler=FailHandler()))
        pipeline = Pipeline(registry=registry, track_execution=True)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        log = logging.getLogger("j_perm")
        original_level = log.level
        log.setLevel(logging.ERROR)
        try:
            with pytest.raises(ValueError, match="test error"):
                await engine.apply_async({}, source={}, dest={})
        finally:
            log.setLevel(original_level)

    @pytest.mark.asyncio
    async def test_run_pipeline_async_raises_for_unknown(self):
        """run_pipeline_async() raises KeyError for unknown pipeline (line 1038)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class DummyHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("d", 10, AlwaysMatcher(), handler=DummyHandler()))
        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        with pytest.raises(KeyError):
            await engine.run_pipeline_async("nonexistent", {}, ctx)

    @pytest.mark.asyncio
    async def test_run_pipeline_async_with_debug_logging(self):
        """run_pipeline_async() emits debug log when enabled (lines 1050-1051)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class SetHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                ctx.dest["ok"] = True
                return ctx.dest

        named_reg = ActionTypeRegistry()
        named_reg.register(ActionNode("set", 10, AlwaysMatcher(), handler=SetHandler()))
        named_pipeline = Pipeline(registry=named_reg)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_reg = ActionTypeRegistry()
        main_reg.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_reg)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            pipelines={"named": named_pipeline},
        )

        log = logging.getLogger("j_perm.pipeline.named")
        original_level = log.level
        log.setLevel(logging.DEBUG)
        try:
            ctx = ExecutionContext(source={}, dest={"initial": True}, engine=engine)
            result = await engine.run_pipeline_async("named", {}, ctx)
            assert result.get("ok") is True
        finally:
            log.setLevel(original_level)

    @pytest.mark.asyncio
    async def test_run_pipeline_async_propagates_exception(self):
        """run_pipeline_async() propagates exception from named pipeline (lines 1054-1064)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class FailHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                raise ValueError("pipeline async error")

        named_reg = ActionTypeRegistry()
        named_reg.register(ActionNode("fail", 10, AlwaysMatcher(), handler=FailHandler()))
        named_pipeline = Pipeline(registry=named_reg, track_execution=True)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_reg = ActionTypeRegistry()
        main_reg.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_reg)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            pipelines={"bad": named_pipeline},
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        with pytest.raises(ValueError, match="pipeline async error"):
            await engine.run_pipeline_async("bad", {}, ctx)

    @pytest.mark.asyncio
    async def test_process_value_async_pipeline_signal(self):
        """process_value_async() handles PipelineSignal (lines 1147-1149)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class SignalHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                class MySig(PipelineSignal):
                    def handle(self, ctx):
                        ctx.dest = "signal_result"
                raise MySig()

        value_registry = ActionTypeRegistry()
        value_registry.register(ActionNode("v", 10, AlwaysMatcher(), handler=SignalHandler()))
        value_pipeline = Pipeline(registry=value_registry)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_registry = ActionTypeRegistry()
        main_registry.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_registry)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await engine.process_value_async("input", ctx)
        assert result == "signal_result"

    @pytest.mark.asyncio
    async def test_process_value_async_trace_logging(self):
        """process_value_async() logs trace when _log_values is DEBUG (line 1153)."""
        counter = [0]

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class StabilizeHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                counter[0] += 1
                if counter[0] == 1:
                    return "transformed"
                return ctx.dest

        value_registry = ActionTypeRegistry()
        value_registry.register(ActionNode("v", 10, AlwaysMatcher(), handler=StabilizeHandler()))
        value_pipeline = Pipeline(registry=value_registry)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_registry = ActionTypeRegistry()
        main_registry.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_registry)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
        )

        log = logging.getLogger("j_perm.values")
        original_level = log.level
        log.setLevel(logging.DEBUG)
        try:
            ctx = ExecutionContext(source={}, dest={}, engine=engine)
            result = await engine.process_value_async("original", ctx)
            assert result == "transformed"
        finally:
            log.setLevel(original_level)

    @pytest.mark.asyncio
    async def test_process_value_async_recursion_error(self):
        """process_value_async() raises RecursionError on oscillation (line 1161)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class OscillateHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                if isinstance(ctx.dest, int):
                    return ctx.dest + 1
                return 0

        value_registry = ActionTypeRegistry()
        value_registry.register(ActionNode("v", 10, AlwaysMatcher(), handler=OscillateHandler()))
        value_pipeline = Pipeline(registry=value_registry)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_registry = ActionTypeRegistry()
        main_registry.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_registry)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
            value_max_depth=5,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        with pytest.raises(RecursionError):
            await engine.process_value_async(0, ctx)

    @pytest.mark.asyncio
    async def test_process_value_async_unescape(self):
        """process_value_async() applies unescape rules after stabilization (line 1165)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class IdentityHandler(AsyncActionHandler):
            async def execute(self, step, ctx):
                return ctx.dest

        value_registry = ActionTypeRegistry()
        value_registry.register(ActionNode("id", 10, AlwaysMatcher(), handler=IdentityHandler()))
        value_pipeline = Pipeline(registry=value_registry)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_registry = ActionTypeRegistry()
        main_registry.register(ActionNode("m", 10, AlwaysMatcher(), handler=MainHandler()))
        main_pipeline = Pipeline(registry=main_registry)

        def my_unescape(obj):
            if isinstance(obj, str):
                return obj.replace("$$", "$")
            return obj

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
            unescape_rules=[UnescapeRule(name="test", priority=0, unescape=my_unescape)],
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = await engine.process_value_async("$$hello", ctx)
        assert result == "$hello"