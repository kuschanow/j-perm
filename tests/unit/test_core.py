"""Tests for core infrastructure."""

import pytest
from j_perm import (
    ExecutionContext,
    Engine,
    Pipeline,
    StageNode,
    StageRegistry,
    StageMatcher,
    StageProcessor,
    ActionNode,
    ActionTypeRegistry,
    ActionMatcher,
    ActionHandler,
    PointerResolver,
    UnescapeRule,
)


class TestExecutionContext:
    """Test ExecutionContext dataclass."""

    def test_create_context(self):
        """Can create execution context."""
        engine = object()
        resolver = PointerResolver()

        ctx = ExecutionContext(
            source={"a": 1},
            dest={"b": 2},
            engine=engine,
            resolver=resolver,
        )

        assert ctx.source == {"a": 1}
        assert ctx.dest == {"b": 2}
        assert ctx.engine is engine
        assert ctx.resolver is resolver
        assert ctx.metadata == {}

    def test_metadata_default(self):
        """Metadata defaults to empty dict."""
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=object(),
            resolver=PointerResolver(),
        )

        assert isinstance(ctx.metadata, dict)
        assert len(ctx.metadata) == 0

    def test_metadata_mutable(self):
        """Can mutate metadata."""
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=object(),
            resolver=PointerResolver(),
        )

        ctx.metadata["key"] = "value"
        assert ctx.metadata["key"] == "value"


class TestStageRegistry:
    """Test StageRegistry."""

    def test_empty_registry_returns_steps_unchanged(self):
        """Empty registry should return input as-is."""
        registry = StageRegistry()
        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())

        result = registry.run_all([1, 2, 3], ctx)
        assert result == [1, 2, 3]

    def test_register_and_execute_stage(self):
        """Can register and execute a stage."""

        class DoubleStage(StageProcessor):
            def apply(self, steps, ctx):
                return [s * 2 for s in steps]

        registry = StageRegistry()
        registry.register(
            StageNode(name="double", priority=10, processor=DoubleStage())
        )

        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())
        result = registry.run_all([1, 2, 3], ctx)

        assert result == [2, 4, 6]

    def test_priority_order(self):
        """Stages execute in priority order (high to low)."""
        executed = []

        class RecordStage(StageProcessor):
            def __init__(self, name):
                self.name = name

            def apply(self, steps, ctx):
                executed.append(self.name)
                return steps

        registry = StageRegistry()
        registry.register(StageNode("low", priority=10, processor=RecordStage("low")))
        registry.register(
            StageNode("high", priority=100, processor=RecordStage("high"))
        )
        registry.register(StageNode("mid", priority=50, processor=RecordStage("mid")))

        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())
        registry.run_all([], ctx)

        assert executed == ["high", "mid", "low"]

    def test_matcher_filters_execution(self):
        """Stage only runs if matcher returns True."""

        class EvenMatcher(StageMatcher):
            def matches(self, steps, ctx):
                return len(steps) % 2 == 0

        class DoubleStage(StageProcessor):
            def apply(self, steps, ctx):
                return [s * 2 for s in steps]

        registry = StageRegistry()
        registry.register(
            StageNode("double", priority=10, matcher=EvenMatcher(), processor=DoubleStage())
        )

        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())

        # Odd length - should not run
        result = registry.run_all([1, 2, 3], ctx)
        assert result == [1, 2, 3]

        # Even length - should run
        result = registry.run_all([1, 2], ctx)
        assert result == [2, 4]


class TestActionTypeRegistry:
    """Test ActionTypeRegistry."""

    def test_resolve_returns_empty_for_no_match(self):
        """resolve() returns empty list if no matcher matches."""
        registry = ActionTypeRegistry()

        class NeverMatcher(ActionMatcher):
            def matches(self, step):
                return False

        class DummyHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        registry.register(
            ActionNode("dummy", 10, NeverMatcher(), handler=DummyHandler())
        )

        handlers = registry.resolve({"op": "test"})
        assert handlers == []

    def test_resolve_returns_handler_on_match(self):
        """resolve() returns handler when matcher matches."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class DummyHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        registry = ActionTypeRegistry()
        handler_instance = DummyHandler()
        registry.register(
            ActionNode("always", 10, AlwaysMatcher(), handler=handler_instance)
        )

        handlers = registry.resolve({"op": "test"})
        assert len(handlers) == 1
        assert handlers[0] is handler_instance

    def test_exclusive_stops_after_first_match(self):
        """exclusive=True stops resolution after first match."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class H1(ActionHandler):
            def execute(self, step, ctx):
                return "h1"

        class H2(ActionHandler):
            def execute(self, step, ctx):
                return "h2"

        registry = ActionTypeRegistry()
        h1_instance = H1()
        h2_instance = H2()

        registry.register(
            ActionNode("h1", 100, AlwaysMatcher(), handler=h1_instance, exclusive=True)
        )
        registry.register(
            ActionNode("h2", 50, AlwaysMatcher(), handler=h2_instance, exclusive=True)
        )

        handlers = registry.resolve({})
        assert len(handlers) == 1
        assert handlers[0] is h1_instance

    def test_non_exclusive_continues(self):
        """exclusive=False continues resolution."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class H1(ActionHandler):
            def execute(self, step, ctx):
                return "h1"

        class H2(ActionHandler):
            def execute(self, step, ctx):
                return "h2"

        registry = ActionTypeRegistry()
        h1_instance = H1()
        h2_instance = H2()

        registry.register(
            ActionNode("h1", 100, AlwaysMatcher(), handler=h1_instance, exclusive=False)
        )
        registry.register(
            ActionNode("h2", 50, AlwaysMatcher(), handler=h2_instance, exclusive=True)
        )

        handlers = registry.resolve({})
        assert len(handlers) == 2
        assert handlers[0] is h1_instance
        assert handlers[1] is h2_instance


class TestPipeline:
    """Test Pipeline."""

    def test_empty_pipeline_fails_on_unhandled_step(self):
        """Pipeline with empty registry should raise on any step."""
        pipeline = Pipeline()
        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())

        with pytest.raises(ValueError, match="unhandled step"):
            pipeline.run({"op": "test"}, ctx)

    def test_pipeline_executes_handler(self):
        """Pipeline executes matched handler."""

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return step.get("op") == "test"

        class TestHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["executed"] = True
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(
            ActionNode("test", 10, TestMatcher(), handler=TestHandler())
        )

        pipeline = Pipeline(registry=registry)
        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())

        pipeline.run({"op": "test"}, ctx)
        assert ctx.dest == {"executed": True}

    def test_pipeline_runs_stages_before_dispatch(self):
        """Stages run before handler dispatch."""
        executed_order = []

        class RecordStage(StageProcessor):
            def apply(self, steps, ctx):
                executed_order.append("stage")
                return steps

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class TestHandler(ActionHandler):
            def execute(self, step, ctx):
                executed_order.append("handler")
                return ctx.dest

        stages = StageRegistry()
        stages.register(StageNode("record", 10, processor=RecordStage()))

        registry = ActionTypeRegistry()
        registry.register(ActionNode("test", 10, TestMatcher(), handler=TestHandler()))

        pipeline = Pipeline(stages=stages, registry=registry)
        engine = object()
        ctx = ExecutionContext({}, {}, engine, PointerResolver())

        pipeline.run({}, ctx)
        assert executed_order == ["stage", "handler"]


class TestEngine:
    """Test Engine."""

    def test_apply_returns_deep_copy(self):
        """apply() should return deep copy of result."""

        class SetHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["key"] = "value"
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(ActionNode("set", 10, AlwaysMatcher(), handler=SetHandler()))

        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            main_pipeline=pipeline,
        )

        dest = {}
        result = engine.apply({}, source={}, dest=dest)

        assert result == {"key": "value"}
        assert dest == {}  # original unchanged

    def test_process_value_returns_unchanged_if_no_value_pipeline(self):
        """process_value with no value_pipeline returns value as-is."""

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
            main_pipeline=pipeline,
            value_pipeline=None,
        )

        ctx = ExecutionContext({}, {}, engine, PointerResolver())
        result = engine.process_value("test", ctx)

        assert result == "test"
