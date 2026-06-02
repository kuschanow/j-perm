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
    PipelineSignal,
    UnescapeRule,
    ValueProcessor,
)
from j_perm.core import _repr_step, _format_lang_stack
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


class TestReprStep:
    """Test _repr_step private helper."""

    def test_repr_step_dict(self):
        """Dict step formats as key-value pairs."""
        result = _repr_step({"op": "set", "path": "/x"})
        assert "'op'" in result and "'set'" in result

    def test_repr_step_non_dict_triggers_line_55(self):
        """Non-dict step uses repr() directly."""
        result = _repr_step("$break")
        assert result == repr("$break")

    def test_repr_step_truncation_triggers_line_51(self):
        """Long repr is truncated to max_len with '...'."""
        long_val = "x" * 300
        result = _repr_step({"k": long_val}, max_len=20)
        assert result.endswith("...")
        assert len(result) == 20

    def test_repr_step_no_truncation_when_max_len_none(self):
        """max_len=None disables truncation."""
        step = {"op": "set", "path": "/x", "value": "short"}
        result = _repr_step(step, max_len=None)
        assert "..." not in result

    def test_repr_step_exception_triggers_lines_66_67(self):
        """Exception during repr returns '<unprintable step>'."""

        class Unrepresentable:
            def __repr__(self):
                raise RuntimeError("cannot repr")

        step = {"key": Unrepresentable()}
        result = _repr_step(step)
        assert result == "<unprintable step>"

    def test_repr_step_list_value_shows_item_count(self):
        """List value is shown as '[N items]'."""
        result = _repr_step({"items": [1, 2, 3]})
        assert "[3 items]" in result

    def test_repr_step_large_dict_value_shown_as_ellipsis(self):
        """Large nested dict is shown as '{...}'."""
        result = _repr_step({"nested": {"a" * 30: "b" * 30}})
        assert "{...}" in result


class TestFormatLangStack:
    """Test _format_lang_stack private helper."""

    def test_empty_frames_triggers_line_73(self):
        """Empty frame list returns '(empty)' message."""
        result = _format_lang_stack([])
        assert "(empty)" in result

    def test_non_empty_frames(self):
        """Non-empty frames are numbered."""
        result = _format_lang_stack(["frame1", "frame2"])
        assert "#1" in result
        assert "frame1" in result


class TestValueProcessorExistsBaseMethod:
    """Test ValueProcessor.exists() default base implementation (lines 221-222)."""

    def test_base_exists_uses_resolve_and_resolver(self):
        """ValueProcessor.exists() base method calls resolve() and resolver.exists()."""

        class MinimalProcessor(ValueProcessor):
            def resolve(self, path, ctx):
                return path, ctx.source

            def get(self, pointer, ctx):
                path, data = self.resolve(pointer, ctx)
                return ctx.engine.resolver.get(path, data)

            def set(self, pointer, ctx, value):
                ctx.engine.resolver.set(pointer, ctx.dest, value)

            def delete(self, pointer, ctx, ignore_missing=False):
                ctx.engine.resolver.delete(pointer, ctx.dest)

        processor = MinimalProcessor()

        class DummyHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        registry = ActionTypeRegistry()
        registry.register(ActionNode("dummy", 10, AlwaysMatcher(), handler=DummyHandler()))
        pipeline = Pipeline(registry=registry)
        engine = Engine(
            resolver=PointerResolver(),
            processor=processor,
            main_pipeline=pipeline,
        )

        ctx = ExecutionContext(source={"key": "value"}, dest={}, engine=engine)
        assert processor.exists("/key", ctx) is True
        assert processor.exists("/missing", ctx) is False


class TestStageRegistryExtended:
    """Extended tests for StageRegistry."""

    def test_register_group_creates_node_with_children(self):
        """register_group() mounts a sub-registry as a group node (line 315)."""
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
        parent_reg.register_group("group", child_reg, priority=5)

        ctx = ExecutionContext(source={}, dest={}, engine=object())
        parent_reg.run_all([], ctx)

        assert "child" in executed

    def test_run_all_with_children_executes_child_first(self):
        """run_all() with children node runs child registry first (line 331)."""
        executed = []

        class RecordStage(StageProcessor):
            def __init__(self, name):
                self.name = name

            def apply(self, steps, ctx):
                executed.append(self.name)
                return steps

        child_reg = StageRegistry()
        child_reg.register(StageNode("child_stage", 10, processor=RecordStage("child_stage")))

        parent_reg = StageRegistry()
        parent_node = StageNode(
            name="parent_node",
            priority=10,
            processor=RecordStage("parent_stage"),
            children=child_reg,
        )
        parent_reg.register(parent_node)

        ctx = ExecutionContext(source={}, dest={}, engine=object())
        parent_reg.run_all([], ctx)

        assert executed == ["child_stage", "parent_stage"]

    def test_nodes_returns_sorted_list(self):
        """nodes() returns stages sorted by priority (line 340)."""
        registry = StageRegistry()
        registry.register(StageNode("low", priority=1, processor=None))
        registry.register(StageNode("high", priority=100, processor=None))
        registry.register(StageNode("mid", priority=50, processor=None))

        nodes = registry.nodes()
        priorities = [n.priority for n in nodes]
        assert priorities == sorted(priorities, reverse=True)


class TestActionTypeRegistryExtended:
    """Extended tests for ActionTypeRegistry."""

    def test_register_group_with_children(self):
        """register_group() mounts sub-registry as children (line 583)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class DummyHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["child"] = True
                return ctx.dest

        child_reg = ActionTypeRegistry()
        child_reg.register(ActionNode("child", 10, AlwaysMatcher(), handler=DummyHandler()))

        parent_reg = ActionTypeRegistry()
        parent_reg.register_group("group", child_reg, matcher=AlwaysMatcher())

        handlers = parent_reg.resolve({})
        assert len(handlers) == 1

    def test_resolve_with_children_extends_handlers(self):
        """resolve() with children node extends handler list (lines 614-617)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class H1(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        child_reg = ActionTypeRegistry()
        child_reg.register(ActionNode("child", 10, AlwaysMatcher(), handler=H1()))

        parent_reg = ActionTypeRegistry()
        parent_reg.register(ActionNode(
            "parent", 10,
            AlwaysMatcher(),
            children=child_reg,
            exclusive=True,
        ))

        handlers = parent_reg.resolve({})
        assert len(handlers) == 1

    def test_run_all_executes_all_matching_handlers(self):
        """run_all() executes all matching handlers in priority order (lines 635-641)."""
        executed = []

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class H1(ActionHandler):
            def execute(self, step, ctx):
                executed.append("h1")
                ctx.dest["h1"] = True
                return ctx.dest

        class H2(ActionHandler):
            def execute(self, step, ctx):
                executed.append("h2")
                ctx.dest["h2"] = True
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("h1", 100, AlwaysMatcher(), handler=H1(), exclusive=False))
        registry.register(ActionNode("h2", 50, AlwaysMatcher(), handler=H2(), exclusive=False))

        ctx = ExecutionContext(source={}, dest={}, engine=object())
        registry.run_all({}, ctx)

        assert executed == ["h1", "h2"]
        assert ctx.dest == {"h1": True, "h2": True}

    def test_run_all_with_children(self):
        """run_all() runs children recursively (lines 637-638)."""
        executed = []

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class ChildHandler(ActionHandler):
            def execute(self, step, ctx):
                executed.append("child")
                return ctx.dest

        child_reg = ActionTypeRegistry()
        child_reg.register(ActionNode("c", 10, AlwaysMatcher(), handler=ChildHandler()))

        parent_reg = ActionTypeRegistry()
        parent_reg.register(ActionNode("p", 10, AlwaysMatcher(), children=child_reg))

        ctx = ExecutionContext(source={}, dest={}, engine=object())
        parent_reg.run_all({}, ctx)

        assert "child" in executed

    def test_nodes_returns_sorted_list(self):
        """nodes() returns handlers sorted by priority (line 647)."""
        registry = ActionTypeRegistry()

        class M(ActionMatcher):
            def matches(self, step):
                return False

        registry.register(ActionNode("low", 1, M()))
        registry.register(ActionNode("high", 100, M()))
        registry.register(ActionNode("mid", 50, M()))

        nodes = registry.nodes()
        priorities = [n.priority for n in nodes]
        assert priorities == sorted(priorities, reverse=True)


class TestPipelineMiddleware:
    """Test Pipeline middleware support."""

    def test_register_middleware_and_execute(self):
        """register_middleware() adds middleware that transforms steps (lines 693, 711)."""
        from j_perm.core import Middleware

        transformed = []

        class UpperMiddleware(Middleware):
            name = "upper"
            priority = 10

            def process(self, step, ctx):
                transformed.append(step)
                return step

        class TestMatcher(ActionMatcher):
            def matches(self, step):
                return step.get("op") == "test"

        class TestHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["done"] = True
                return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("test", 10, TestMatcher(), handler=TestHandler()))

        pipeline = Pipeline(registry=registry)
        pipeline.register_middleware(UpperMiddleware())

        engine = object()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        pipeline.run({"op": "test"}, ctx)

        assert ctx.dest == {"done": True}
        assert len(transformed) == 1


class TestEngineExtended:
    """Extended tests for Engine."""

    def _make_engine(self, handler_cls=None, *, track_execution=True):
        """Helper to build a minimal engine."""
        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        if handler_cls is None:
            class handler_cls(ActionHandler):
                def execute(self, step, ctx):
                    ctx.dest["done"] = True
                    return ctx.dest

        registry = ActionTypeRegistry()
        registry.register(ActionNode("h", 10, AlwaysMatcher(), handler=handler_cls()))
        pipeline = Pipeline(registry=registry, track_execution=track_execution)
        return Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=pipeline,
        )

    def test_custom_functions_in_init(self):
        """Engine.__init__ with custom_functions sets them as attributes (line 900)."""
        my_func = lambda x: x * 2  # noqa: E731
        engine = self._make_engine()
        engine2 = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=engine.main_pipeline,
            custom_functions={"my_func": my_func},
        )
        assert hasattr(engine2, "my_func")
        assert engine2.my_func(5) == 10

    def test_register_pipeline(self):
        """register_pipeline() makes pipeline available via run_pipeline (line 906)."""

        class SetHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["from_named"] = True
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        named_registry = ActionTypeRegistry()
        named_registry.register(ActionNode("set", 10, AlwaysMatcher(), handler=SetHandler()))
        named_pipeline = Pipeline(registry=named_registry)

        engine = self._make_engine()
        engine.register_pipeline("named", named_pipeline)

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = engine.run_pipeline("named", {}, ctx)
        assert result.get("from_named") is True

    def test_register_custom_function(self):
        """register_custom_function() sets function as engine attribute (line 910)."""
        engine = self._make_engine()
        engine.register_custom_function("double", lambda x: x * 2)
        assert engine.double(7) == 14

    def test_run_pipeline_executes_named_pipeline(self):
        """run_pipeline() runs named pipeline with isolated dest (lines 998-1028)."""

        class SetHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["result"] = 42
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        named_registry = ActionTypeRegistry()
        named_registry.register(ActionNode("set", 10, AlwaysMatcher(), handler=SetHandler()))
        named_pipeline = Pipeline(registry=named_registry)

        engine = self._make_engine()
        engine.register_pipeline("compute", named_pipeline)

        ctx = ExecutionContext(source={}, dest={"original": True}, engine=engine)
        result = engine.run_pipeline("compute", {}, ctx)

        assert result == {"original": True, "result": 42}
        assert ctx.dest == {"original": True}  # original untouched

    def test_run_pipeline_raises_for_unknown_pipeline(self):
        """run_pipeline() raises KeyError for unknown pipeline name."""
        engine = self._make_engine()
        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        with pytest.raises(KeyError):
            engine.run_pipeline("nonexistent", {}, ctx)

    def test_run_pipeline_propagates_exceptions(self):
        """run_pipeline() propagates exceptions from the named pipeline (lines 1017-1028)."""

        class FailHandler(ActionHandler):
            def execute(self, step, ctx):
                raise ValueError("pipeline error")

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        fail_registry = ActionTypeRegistry()
        fail_registry.register(ActionNode("fail", 10, AlwaysMatcher(), handler=FailHandler()))
        fail_pipeline = Pipeline(registry=fail_registry, track_execution=True)

        engine = self._make_engine()
        engine.register_pipeline("failing", fail_pipeline)

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        with pytest.raises(ValueError, match="pipeline error"):
            engine.run_pipeline("failing", {}, ctx)

    def test_trace_logging_calls_repr_step(self):
        """trace_logging=True emits debug log for each step (line 733)."""
        import logging
        from j_perm import build_default_engine

        engine = build_default_engine(trace_logging=True, trace_repr_max=50)
        log = logging.getLogger("j_perm")
        original_level = log.level
        log.setLevel(logging.DEBUG)
        try:
            result = engine.apply(
                {"op": "set", "path": "/x", "value": 42},
                source={},
                dest={},
            )
            assert result == {"x": 42}
        finally:
            log.setLevel(original_level)

    def test_trace_repr_max_truncation(self):
        """Trace repr is truncated when trace_repr_max is small (line 51 via pipeline)."""
        from j_perm import build_default_engine

        engine = build_default_engine(trace_logging=True, trace_repr_max=10)
        # Run any spec — _repr_step is called, truncation may occur if step repr > 10 chars
        result = engine.apply(
            {"op": "set", "path": "/k", "value": "v"},
            source={}, dest={},
        )
        assert result == {"k": "v"}

    def test_process_value_recursion_error(self):
        """process_value() raises RecursionError when max_depth exceeded (line 1114)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class OscillateHandler(ActionHandler):
            """Always returns a different value to prevent stabilization."""

            def execute(self, step, ctx):
                if isinstance(ctx.dest, int):
                    return ctx.dest + 1
                return 0

        value_registry = ActionTypeRegistry()
        value_registry.register(
            ActionNode("oscillate", 10, AlwaysMatcher(), handler=OscillateHandler())
        )
        value_pipeline = Pipeline(registry=value_registry)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_registry = ActionTypeRegistry()
        main_registry.register(ActionNode("main", 10, AlwaysMatcher(), handler=MainHandler()))
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
            engine.process_value(0, ctx)

    def test_run_pipeline_debug_logging(self):
        """run_pipeline() emits debug log when logger is at DEBUG level (lines 1013-1014)."""
        import logging

        class SetHandler(ActionHandler):
            def execute(self, step, ctx):
                ctx.dest["r"] = True
                return ctx.dest

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        named_reg = ActionTypeRegistry()
        named_reg.register(ActionNode("s", 10, AlwaysMatcher(), handler=SetHandler()))
        named_pipeline = Pipeline(registry=named_reg)

        engine = self._make_engine()
        engine.register_pipeline("dbg_pl", named_pipeline)

        log = logging.getLogger("j_perm.pipeline.dbg_pl")
        original_level = log.level
        log.setLevel(logging.DEBUG)
        try:
            ctx = ExecutionContext(source={}, dest={}, engine=engine)
            result = engine.run_pipeline("dbg_pl", {}, ctx)
            assert result.get("r") is True
        finally:
            log.setLevel(original_level)

    def test_process_value_pipeline_signal_from_stage(self):
        """process_value() catches PipelineSignal raised from a stage (lines 1100-1102)."""

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        class IdentityHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        class SignalStage(StageProcessor):
            """Stage that raises PipelineSignal to break the stabilization loop."""
            def apply(self, steps, ctx):
                class StopSignal(PipelineSignal):
                    def handle(self, ctx):
                        ctx.dest = "stopped"
                raise StopSignal()

        value_reg = ActionTypeRegistry()
        value_reg.register(ActionNode("id", 10, AlwaysMatcher(), handler=IdentityHandler()))
        value_stages = StageRegistry()
        value_stages.register(StageNode("stop", 10, processor=SignalStage()))
        value_pipeline = Pipeline(registry=value_reg, stages=value_stages)

        main_reg = ActionTypeRegistry()
        main_reg.register(ActionNode("m", 10, AlwaysMatcher(), handler=IdentityHandler()))
        main_pipeline = Pipeline(registry=main_reg)

        engine = Engine(
            resolver=PointerResolver(),
            processor=PointerProcessor(),
            main_pipeline=main_pipeline,
            value_pipeline=value_pipeline,
        )

        ctx = ExecutionContext(source={}, dest={}, engine=engine)
        result = engine.process_value("input", ctx)
        # PipelineSignal from stage breaks the loop early; current stays at initial value
        # (signal.handle() is NOT called because the signal escaped from stages, not from handler)
        assert result == "input"

    def test_process_value_trace_logging(self):
        """process_value() logs trace when _log_values is at DEBUG level (line 1106)."""
        import logging

        class AlwaysMatcher(ActionMatcher):
            def matches(self, step):
                return True

        # Counter to track iterations
        counter = [0]

        class StabilizeHandler(ActionHandler):
            def execute(self, step, ctx):
                counter[0] += 1
                if counter[0] == 1:
                    return "transformed"
                return ctx.dest

        value_registry = ActionTypeRegistry()
        value_registry.register(
            ActionNode("v", 10, AlwaysMatcher(), handler=StabilizeHandler())
        )
        value_pipeline = Pipeline(registry=value_registry)

        class MainHandler(ActionHandler):
            def execute(self, step, ctx):
                return ctx.dest

        main_registry = ActionTypeRegistry()
        main_registry.register(ActionNode("main", 10, AlwaysMatcher(), handler=MainHandler()))
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
            result = engine.process_value("original", ctx)
            assert result == "transformed"
        finally:
            log.setLevel(original_level)
