"""Tests for async infrastructure."""

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