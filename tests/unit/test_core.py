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
)
from j_perm.processors.pointer_processor import PointerProcessor


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
        )

        assert ctx.source == {"a": 1}
        assert ctx.dest == {"b": 2}
        assert ctx.engine is engine
        assert ctx.metadata == {}

    def test_metadata_default(self):
        """Metadata defaults to empty dict."""
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=object(),
        )

        assert isinstance(ctx.metadata, dict)
        assert len(ctx.metadata) == 0

    def test_metadata_mutable(self):
        """Can mutate metadata."""
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=object(),
        )

        ctx.metadata["key"] = "value"
        assert ctx.metadata["key"] == "value"

    def test_copy_basic(self):
        """copy() creates a new context with same values by default."""
        engine = object()
        resolver = PointerResolver()
        ctx = ExecutionContext(
            source={"a": 1},
            dest={"b": 2},
            engine=engine,
            metadata={"key": "value"},
        )

        ctx2 = ctx.copy()

        assert ctx2.source is ctx.source
        assert ctx2.dest is ctx.dest
        assert ctx2.engine is ctx.engine
        assert ctx2.metadata is ctx.metadata

    def test_copy_with_new_source(self):
        """copy() can override source."""
        ctx = ExecutionContext(
            source={"a": 1},
            dest={"b": 2},
            engine=object(),
        )

        ctx2 = ctx.copy(new_source={"c": 3})

        assert ctx2.source == {"c": 3}
        assert ctx2.dest is ctx.dest

    def test_copy_with_deepcopy_source(self):
        """copy() can deepcopy source."""
        ctx = ExecutionContext(
            source={"a": {"nested": "value"}},
            dest={},
            engine=object(),
        )

        ctx2 = ctx.copy(deepcopy_source=True)

        assert ctx2.source == ctx.source
        assert ctx2.source is not ctx.source
        assert ctx2.source["a"] is not ctx.source["a"]

    def test_copy_with_new_dest(self):
        """copy() can override dest."""
        ctx = ExecutionContext(
            source={},
            dest={"b": 2},
            engine=object(),
        )

        ctx2 = ctx.copy(new_dest={"d": 4})

        assert ctx2.dest == {"d": 4}
        assert ctx2.source is ctx.source

    def test_copy_with_deepcopy_dest(self):
        """copy() can deepcopy dest."""
        ctx = ExecutionContext(
            source={},
            dest={"a": {"nested": "value"}},
            engine=object(),
        )

        ctx2 = ctx.copy(deepcopy_dest=True)

        assert ctx2.dest == ctx.dest
        assert ctx2.dest is not ctx.dest
        assert ctx2.dest["a"] is not ctx.dest["a"]

    def test_copy_with_new_engine(self):
        """copy() can override engine."""
        engine1 = object()
        engine2 = object()
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=engine1,
        )

        ctx2 = ctx.copy(new_engine=engine2)

        assert ctx2.engine is engine2
        assert ctx.engine is engine1

    def test_copy_with_new_metadata(self):
        """copy() can override metadata."""
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=object(),
            metadata={"key": "value"},
        )

        ctx2 = ctx.copy(new_metadata={"new_key": "new_value"})

        assert ctx2.metadata == {"new_key": "new_value"}
        assert ctx.metadata == {"key": "value"}

    def test_copy_with_deepcopy_metadata(self):
        """copy() can deepcopy metadata."""
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=object(),
            metadata={"key": {"nested": "value"}},
        )

        ctx2 = ctx.copy(deepcopy_metadata=True)

        assert ctx2.metadata == ctx.metadata
        assert ctx2.metadata is not ctx.metadata
        assert ctx2.metadata["key"] is not ctx.metadata["key"]


class TestStageRegistry:
    """Test StageRegistry."""

    def test_empty_registry_returns_steps_unchanged(self):
        """Empty registry should return input as-is."""
        registry = StageRegistry()
        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

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
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
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
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
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
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

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
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

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
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

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
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

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
            processor=PointerProcessor(),
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
            processor=PointerProcessor(),
            main_pipeline=pipeline,
            value_pipeline=None,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = engine.process_value("test", ctx)

        assert result == "test"

    def test_apply_to_context_mutates_context_in_place(self):
        """apply_to_context() mutates the provided context's dest."""

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
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        ctx = ExecutionContext(
            source={},
            dest={},
            engine=engine,
        )

        result = engine.apply_to_context({}, ctx)

        # The context's dest was mutated
        assert ctx.dest == {"key": "value"}
        # apply_to_context returns deep copy
        assert result == {"key": "value"}
        assert result is not ctx.dest

    def test_apply_to_context_uses_provided_context(self):
        """apply_to_context() uses the provided context's source and dest."""

        class CopyHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["from_source"] = ctx.source.get("data", "missing")
                ctx.dest["from_dest"] = ctx.dest.get("existing", "missing")
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(ActionNode("copy", 10, AlwaysMatcher(), handler=CopyHandler()))

        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

        ctx = ExecutionContext(
            source={"data": "source_value"},
            dest={"existing": "dest_value"},
            engine=engine,
        )

        result = engine.apply_to_context({}, ctx)

        assert result == {
            "from_source": "source_value",
            "from_dest": "dest_value",
            "existing": "dest_value",
        }
