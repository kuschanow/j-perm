"""Core abstractions, registries, Pipeline, and Engine.

This module owns every *interface* in the system.  Nothing here depends on a
concrete implementation — all concrete classes live in the sub-packages
(``stages``, ``handlers``, ``resolvers``) or in ``factory``.

Execution flow (``Engine.apply`` entry point)::

    spec (raw user input)
      │
      ▼
    StageRegistry.run_all(steps, ctx)   ← batch pre-processing (tree, run ALL)
      │
      ▼
    for step in steps:
        Middleware chain (per-step, by priority)
        ActionTypeRegistry.resolve(step) → [handler, ...]   ← select (tree, first-match)
        for handler in handlers:
            ctx.dest = handler.execute(step, ctx)
                │
                └─ ctx.engine.process_value(val, ctx)       ← value substitution
                       └── value_pipeline.run([val], …)     ← stabilisation loop
"""

from __future__ import annotations

import copy
import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple, Union

_log = logging.getLogger("j_perm")
_log_values = logging.getLogger("j_perm.values")

# Metadata key for the language-level execution stack
_LANG_EXEC_STACK_KEY = "_lang_exec_stack"

def _repr_step(step: Any, max_len: Optional[int] = 200) -> str:
    """Compact human-readable representation of a DSL step for the language call stack.

    Args:
        step:    The DSL step to format.
        max_len: Maximum string length before truncating with ``"..."``.
                 ``None`` disables truncation (shows the full step).
    """
    def _trunc(s: str) -> str:
        if max_len is None or len(s) <= max_len:
            return s
        return s[:max_len - 3] + "..."

    try:
        if not isinstance(step, dict):
            return _trunc(repr(step))
        parts = []
        for k, v in step.items():
            if isinstance(v, list):
                vr = f"[{len(v)} items]"
            elif isinstance(v, dict) and len(repr(v)) > 50:
                vr = "{...}"
            else:
                vr = repr(v)
            parts.append(f"{k!r}: {vr}")
        return _trunc("{" + ", ".join(parts) + "}")
    except Exception:
        return "<unprintable step>"


def _format_lang_stack(frames: list) -> str:
    """Format a list of language stack frames for human-readable display."""
    if not frames:
        return "  (empty)"
    lines = [f"  #{i + 1:<3} {frame}" for i, frame in enumerate(frames)]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers (Engine internals — not exported)
# ─────────────────────────────────────────────────────────────────────────────


def _tuples_to_lists(obj: Any) -> Any:
    """Recursively convert tuples → lists (JMESPath does not see tuples)."""
    if isinstance(obj, (tuple, list)):
        return [_tuples_to_lists(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _tuples_to_lists(v) for k, v in obj.items()}
    return obj


# ─────────────────────────────────────────────────────────────────────────────
# ExecutionContext
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ExecutionContext:
    """Shared mutable state threaded through an entire ``apply`` call.

    Attributes:
        source: Read-only input document (normalized at Engine.apply time).
        dest: The document being built; mutated by each handler in sequence.
        engine: Back-reference to the owning Engine (gives access to
                ``process_value``, ``run_pipeline``, ``engine.resolver``,
                ``engine.processor``, etc.).
        metadata: Arbitrary dict for passing side-channel data between
                  stages/middlewares/handlers within one ``apply`` call.
        temp_read_only: Arbitrary dict for sharing data that must not be mutated by handlers. Useful for things like function arguments.
        temp: Arbitrary dict for sharing data that may be mutated by handlers, but should not appear in the final output.
    """

    source: Any
    dest: Any
    engine: 'Engine'
    metadata: dict[str, Any] = field(default_factory=dict)
    temp_read_only: dict[str, Any] = field(default_factory=dict)
    temp: dict[str, Any] = field(default_factory=dict)

    def copy(
            self,
            new_source: Any = None,
            new_dest: Any = None,
            new_metadata: Optional[dict[str, Any]] = None,
            new_temp_read_only: Optional[dict[str, Any]] = None,
            new_temp: Optional[dict[str, Any]] = None,
            new_engine: Optional['Engine'] = None,
            deepcopy_source: bool = False,
            deepcopy_dest: bool = False,
            deepcopy_metadata: bool = False,
            deepcopy_temp_read_only: bool = False,
            deepcopy_temp: bool = False,
    ) -> ExecutionContext:
        """Return a copy of this context with optional overrides.

        Used by Engine.run_pipeline to create an isolated sub-context for the
        nested pipeline.
        """
        return ExecutionContext(
            source=new_source if new_source is not None else (copy.deepcopy(self.source) if deepcopy_source else self.source),
            dest=new_dest if new_dest is not None else (copy.deepcopy(self.dest) if deepcopy_dest else self.dest),
            engine=new_engine if new_engine is not None else self.engine,
            metadata=new_metadata if new_metadata is not None else (copy.deepcopy(self.metadata) if deepcopy_metadata else self.metadata),
            temp_read_only=new_temp_read_only if new_temp_read_only is not None else (copy.deepcopy(self.temp_read_only) if deepcopy_temp_read_only else self.temp_read_only),
            temp=new_temp if new_temp is not None else (copy.deepcopy(self.temp) if deepcopy_temp else self.temp),
        )


# ─────────────────────────────────────────────────────────────────────────────
# ValueResolver — path-addressing abstraction
# ─────────────────────────────────────────────────────────────────────────────


class ValueResolver(ABC):
    """Abstract interface for reading/writing paths inside documents.

    Swap the concrete implementation and the entire system adapts to a
    different addressing scheme (JSON Pointer, dot-notation, …).

    Default implementation: ``resolvers.pointer.PointerResolver`` (wraps
    ``PointerManager``).
    """

    @abstractmethod
    def get(self, path: str, data: Any) -> Any:
        """Read the value at *path*.  Raises ``KeyError`` / ``IndexError`` if absent."""

    @abstractmethod
    def set(self, path: str, data: Any, value: Any) -> Any:
        """Write *value* at *path*.  Returns the (possibly new) *data* root."""

    @abstractmethod
    def delete(self, path: str, data: Any) -> Any:
        """Delete the value at *path*.  Returns the (possibly new) *data* root."""

    def exists(self, path: str, data: Any) -> bool:
        """Check whether *path* resolves to a value.

        Default: wraps ``get`` in a try/except.  Override for cheaper probes.
        """
        try:
            self.get(path, data)
            return True
        except (KeyError, IndexError, TypeError):
            return False


class ValueProcessor(ABC):
    """Abstract interface for processing values during substitution.

    The main use case is to support custom functions in the value pipeline.
    """

    @abstractmethod
    def resolve(self, path: str, ctx: ExecutionContext) -> Tuple[str, Any]:
        """Resolves path with prefix and returns normalized path and data source."""
        pass

    @abstractmethod
    def get(self, pointer: str, ctx: ExecutionContext) -> Any:
        """Read the value at *pointer*"""

    @abstractmethod
    def set(self, pointer: str, ctx: ExecutionContext, value: Any) -> None:
        """Write *value* at *pointer*"""

    @abstractmethod
    def delete(self, pointer: str, ctx: ExecutionContext) -> None:
        """Delete the value at *pointer*"""

    def exists(self, pointer: str, ctx: ExecutionContext) -> bool:
        """Checks if path exists with prefix support.

        Args:
            pointer: JSON pointer with optional prefix
            ctx: Execution context

        Returns:
            True if path exists, False otherwise
        """
        processed_path, data_source = self.resolve(pointer, ctx)
        return ctx.engine.resolver.exists(processed_path, data_source)


# ─────────────────────────────────────────────────────────────────────────────
# Stage system — tree-structured batch pre-processing
# ─────────────────────────────────────────────────────────────────────────────


class StageMatcher(ABC):
    """Predicate that decides whether a StageNode should fire.

    If a node has ``matcher=None`` it fires unconditionally.
    """

    @abstractmethod
    def matches(self, steps: List[Any], ctx: ExecutionContext) -> bool: ...


class StageProcessor(ABC):
    """Batch transformation of the full step list.

    Use cases: shorthand expansion, step validation/rewriting, sorting, …
    """

    @abstractmethod
    def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        """Return the (possibly transformed) step list."""


class AsyncStageProcessor(ABC):
    """Async version of StageProcessor for async batch transformations."""

    @abstractmethod
    async def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        """Return the (possibly transformed) step list asynchronously."""


@dataclass
class StageNode:
    """Single node in the stage tree.

    Unlike ``ActionNode`` there is no *exclusive* flag — ``StageRegistry``
    always runs **every** matching node (``run_all`` semantics).

    Field combinations::

        processor + no children   → leaf: just run this processor
        no processor + children   → group container (no own logic)
        processor + children      → children run first, then this processor
    """

    name: str
    priority: int
    processor: Optional[Union[StageProcessor, AsyncStageProcessor]] = None
    matcher: Optional[StageMatcher] = None  # None ⇒ always fires
    children: Optional['StageRegistry'] = None


class StageRegistry:
    """Tree-structured registry with *run-all* dispatch semantics.

    ``run_all`` iterates nodes by descending priority.  For every node whose
    matcher fires (or that has no matcher) it first recurses into ``children``,
    then invokes ``processor``.  **All** matching nodes execute — there is no
    short-circuit.

    ::

        steps = registry.run_all(steps, ctx)
    """

    def __init__(self) -> None:
        self._nodes: List[StageNode] = []

    # -- registration ------------------------------------------------------

    def register(self, node: StageNode) -> None:
        """Add a node to this registry level."""
        self._nodes.append(node)

    def register_group(
            self,
            name: str,
            registry: 'StageRegistry',
            *,
            matcher: Optional[StageMatcher] = None,
            priority: int = 0,
            processor: Optional[StageProcessor] = None,
    ) -> None:
        """Mount a sub-registry as a group node.

        Sugar for ``register(StageNode(…, children=registry))``.
        """
        self.register(StageNode(
            name=name, priority=priority,
            processor=processor, matcher=matcher,
            children=registry,
        ))

    # -- execution ----------------------------------------------------------

    def run_all(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        """Execute all matching processors in priority-descending order.

        Returns the fully transformed step list.
        """
        for node in sorted(self._nodes, key=lambda n: n.priority, reverse=True):
            if node.matcher is None or node.matcher.matches(steps, ctx):
                if node.children is not None:
                    steps = node.children.run_all(steps, ctx)
                if node.processor is not None:
                    steps = node.processor.apply(steps, ctx)
        return steps

    # -- introspection ------------------------------------------------------

    def nodes(self) -> List[StageNode]:
        """Return nodes sorted by descending priority."""
        return sorted(self._nodes, key=lambda n: n.priority, reverse=True)

    # -- async execution ----------------------------------------------------

    async def run_all_async(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        """Async version of run_all that supports both sync and async processors.

        For AsyncStageProcessor instances, awaits them. For sync processors,
        calls them directly. Returns the fully transformed step list.
        """
        for node in sorted(self._nodes, key=lambda n: n.priority, reverse=True):
            if node.matcher is None or node.matcher.matches(steps, ctx):
                if node.children is not None:
                    steps = await node.children.run_all_async(steps, ctx)
                if node.processor is not None:
                    # Check if processor is async
                    if isinstance(node.processor, AsyncStageProcessor):
                        steps = await node.processor.apply(steps, ctx)
                    else:
                        steps = node.processor.apply(steps, ctx)
        return steps


# Backward-compat alias: ``Stage`` was the original name for ``StageProcessor``.
Stage = StageProcessor


# ─────────────────────────────────────────────────────────────────────────────
# Middleware — per-step cross-cutting concerns
# ─────────────────────────────────────────────────────────────────────────────


class Middleware(ABC):
    """Per-step hook that runs *after* the previous step's handler has updated
    ``ctx.dest``, but *before* the current step is dispatched.

    Intended for validation, logging, metrics — **not** for value substitution
    (that is the handler's job via ``process_value``).

    Class attributes (set in subclass)::

        name:     str   – unique key
        priority: int   – higher = earlier; baseline = 0
    """

    name: str
    priority: int

    @abstractmethod
    def process(self, step: Any, ctx: ExecutionContext) -> Any:
        """Transform (or validate) a single step before dispatch."""


class AsyncMiddleware(ABC):
    """Async version of Middleware for async per-step processing.

    Class attributes (set in subclass)::

        name:     str   – unique key
        priority: int   – higher = earlier; baseline = 0
    """

    name: str
    priority: int

    @abstractmethod
    async def process(self, step: Any, ctx: ExecutionContext) -> Any:
        """Transform (or validate) a single step before dispatch asynchronously."""


# ─────────────────────────────────────────────────────────────────────────────
# ControlFlowSignal — base for $break / $continue / $return
# ─────────────────────────────────────────────────────────────────────────────


class ControlFlowSignal(Exception):
    """Base class for control flow signals ($break, $continue, $return).

    These are NOT errors — they implement loop/function control flow.
    They inherit from ``Exception`` so that ``Pipeline.run`` can distinguish
    them from real errors and skip error annotation / logging.

    Concrete subclasses live in ``handlers/signals.py``.
    """


# ─────────────────────────────────────────────────────────────────────────────
# PipelineSignal — extensible in-pipeline control flow
# ─────────────────────────────────────────────────────────────────────────────


class PipelineSignal(Exception):
    """Base class for signals that ``Pipeline.run`` intercepts during execution.

    ``Pipeline.run`` wraps each ``handler.execute(step, ctx)`` call in a
    ``try/except PipelineSignal`` block and calls ``signal.handle(ctx)``.

    * If ``handle`` returns normally, execution continues with the next step.
    * If ``handle`` re-raises (or raises any exception), the signal propagates
      up the call stack — allowing ``Engine.process_value`` (or any other
      caller) to catch ``PipelineSignal`` and react accordingly.

    This keeps ``core.py`` free from concrete signal knowledge: it only knows
    the abstract contract.  Concrete signals (e.g. ``RawValueSignal``) live in
    ``handlers/signals.py`` and self-describe their behaviour via ``handle``.

    Example — a signal that stops value-pipeline stabilisation::

        class RawValueSignal(PipelineSignal):
            def __init__(self, value):
                self.value = value
            def handle(self, ctx):
                ctx.dest = self.value
                raise self          # propagate → process_value catches & breaks

    Example — a signal that silently mutates ctx without propagating::

        class SomeLocalSignal(PipelineSignal):
            def handle(self, ctx):
                ctx.dest = ...      # just update dest, no re-raise
    """

    def handle(self, ctx: 'ExecutionContext') -> None:
        """React to the signal.  Override in subclasses.

        *ctx* is the active ``ExecutionContext`` at the point the signal was
        raised.  The default implementation is a no-op (signal is silently
        swallowed by ``Pipeline.run``).
        """


# ─────────────────────────────────────────────────────────────────────────────
# Action system — tree-structured per-step dispatch
# ─────────────────────────────────────────────────────────────────────────────


class ActionMatcher(ABC):
    """Predicate: does this *step* belong to the given tree node?

    Examples::

        OpMatcher("set")     → step.get("op") == "set"
        AlwaysMatcher()      → True
    """

    @abstractmethod
    def matches(self, step: Any) -> bool: ...


class ActionHandler(ABC):
    """Execute a single DSL action.

    The handler is responsible for calling ``ctx.engine.process_value(…)``
    whenever it needs a substituted value — there is no automatic pre-pass.
    """

    @abstractmethod
    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        """Run the action.  Return the new ``dest``."""


class AsyncActionHandler(ABC):
    """Async version of ActionHandler for operations that benefit from async execution.

    The handler is responsible for calling ``await ctx.engine.process_value_async(…)``
    whenever it needs a substituted value.
    """

    @abstractmethod
    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        """Run the action asynchronously.  Return the new ``dest``."""


@dataclass
class ActionNode:
    """Node in the action-type tree.

    Field combinations::

        handler, no children   → leaf action
        no handler, children   → group (no fallback)
        handler + children     → group with fallback handler

    ``exclusive`` controls resolution after this node:

    * ``True`` (default) – stop collecting once this node yields ≥ 1 handler
      (standard *first-match*).
    * ``False`` – keep going; all collected handlers will execute in sequence.

    The *fallback* rule: ``handler`` is only selected when
    ``children.resolve()`` returns an empty list.  If children matched,
    the parent handler is skipped entirely.
    """

    name: str
    priority: int
    matcher: ActionMatcher
    handler: Optional[Union[ActionHandler, AsyncActionHandler]] = None
    children: Optional['ActionTypeRegistry'] = None
    exclusive: bool = True


class ActionTypeRegistry:
    """Hierarchical registry with *select* (``resolve``) and *run-all*
    (``run_all``) dispatch modes.

    Each instance is one level of the tree and may be nested as the
    ``children`` of an ``ActionNode``.

    ``resolve``
        Walk nodes by descending priority.  Honour ``exclusive`` and the
        group-fallback rule.  Return the *selected* handler list — execution
        is the caller's job.

    ``run_all``
        Walk nodes by descending priority, **ignoring** ``exclusive``.
        Execute every matching handler immediately, updating ``ctx.dest``
        as it goes.  Useful for middleware-style "apply everything" logic.
    """

    def __init__(self) -> None:
        self._nodes: List[ActionNode] = []

    # -- registration -------------------------------------------------------

    def register(self, node: ActionNode) -> None:
        """Add a node to this registry level."""
        self._nodes.append(node)

    def register_group(
            self,
            name: str,
            registry: 'ActionTypeRegistry',
            *,
            matcher: ActionMatcher,
            priority: int = 0,
            handler: Optional[ActionHandler] = None,
            exclusive: bool = True,
    ) -> None:
        """Mount a sub-registry as a group node.

        Sugar for ``register(ActionNode(…, children=registry))``.
        """
        self.register(ActionNode(
            name=name, priority=priority,
            matcher=matcher, handler=handler,
            children=registry, exclusive=exclusive,
        ))

    # -- dispatch (select) --------------------------------------------------

    def resolve(self, step: Any) -> List[ActionHandler]:
        """Select handlers for *step* using the full exclusive / fallback rules.

        Returns an ordered list (may be empty).  The caller (Pipeline) raises
        if the list is empty.

        Algorithm::

            for node by priority desc:
                if matcher matches:
                    node_resolved = False
                    if children:
                        sub = children.resolve(step)
                        if sub → extend; node_resolved = True
                    if not node_resolved and handler:
                        append handler          # fallback
                    if exclusive and list non-empty → break
        """
        handlers: List[ActionHandler] = []
        for node in sorted(self._nodes, key=lambda n: n.priority, reverse=True):
            if node.matcher.matches(step):
                node_resolved = False
                if node.children is not None:
                    sub = node.children.resolve(step)
                    if sub:
                        handlers.extend(sub)
                        node_resolved = True
                if not node_resolved and node.handler is not None:
                    handlers.append(node.handler)
                if node.exclusive and handlers:
                    break
        return handlers

    # -- dispatch (run-all) -------------------------------------------------

    def run_all(self, step: Any, ctx: ExecutionContext) -> Any:
        """Execute **all** matching handlers in priority-descending order.

        Unlike ``resolve``, ``exclusive`` is ignored and every matching node
        (including nested children) fires.  Each handler sees the ``ctx.dest``
        that the previous handler left behind.

        Returns the final ``ctx.dest``.
        """
        for node in sorted(self._nodes, key=lambda n: n.priority, reverse=True):
            if node.matcher.matches(step):
                if node.children is not None:
                    ctx.dest = node.children.run_all(step, ctx)
                if node.handler is not None:
                    ctx.dest = node.handler.execute(step, ctx)
        return ctx.dest

    # -- introspection ------------------------------------------------------

    def nodes(self) -> List[ActionNode]:
        """Return nodes sorted by descending priority."""
        return sorted(self._nodes, key=lambda n: n.priority, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline — pure step processor
# ─────────────────────────────────────────────────────────────────────────────


class Pipeline:
    """Self-contained step processor: stages → middlewares → dispatch.

    A Pipeline knows nothing about other pipelines, does not create contexts,
    and does not deepcopy anything.  All of that is ``Engine``'s job.

    Both *main_pipeline* and *value_pipeline* are plain ``Pipeline`` instances
    — they differ only in their stage/handler configuration.

    Execution::

        steps = spec if list else [spec]
        steps = self.stages.run_all(steps, ctx)

        for step in steps:
            step = middleware chain …
            handlers = self.registry.resolve(step)
            for handler in handlers:
                ctx.dest = handler.execute(step, ctx)
    """

    def __init__(
            self,
            *,
            registry: Optional[ActionTypeRegistry] = None,
            stages: Optional[StageRegistry] = None,
            middlewares: Optional[List[Middleware]] = None,
            track_execution: bool = False,
    ) -> None:
        self.registry = registry or ActionTypeRegistry()
        self.stages = stages or StageRegistry()
        self._middlewares = list(middlewares) if middlewares else []
        self.track_execution = track_execution

    # -- registration -------------------------------------------------------

    def register_middleware(self, middleware: Middleware) -> None:
        """Add a per-step middleware."""
        self._middlewares.append(middleware)

    # -- execution ----------------------------------------------------------

    def run(self, spec: Any, ctx: ExecutionContext) -> None:
        """Run the pipeline.  Context is created/owned by Engine.

        *spec* is either a single step or a list of steps.  For the
        value_pipeline the Engine wraps the value in ``[value]`` so that a
        list-typed value is not mistakenly unpacked into multiple steps.

        After ``run`` returns, the result lives in ``ctx.dest``.
        """
        steps: List[Any] = spec if isinstance(spec, list) else [spec]
        steps = self.stages.run_all(steps, ctx)

        for step in steps:
            for mw in sorted(self._middlewares, key=lambda m: m.priority, reverse=True):
                step = mw.process(step, ctx)

            handlers = self.registry.resolve(step)
            if not handlers:
                raise ValueError(f"unhandled step: {step!r}")
            for handler in handlers:
                # Increment operation counter
                ctx.metadata['_operation_count'] = ctx.metadata.get('_operation_count', 0) + 1
                max_ops = getattr(ctx.engine, 'max_operations', float('inf'))
                if ctx.metadata['_operation_count'] > max_ops:
                    raise RuntimeError(
                        f"Operation limit exceeded: {ctx.metadata['_operation_count']} operations executed, "
                        f"maximum allowed is {max_ops}"
                    )

                # Track language-level execution stack
                if self.track_execution:
                    lang_stack = ctx.metadata.setdefault(_LANG_EXEC_STACK_KEY, [])
                    repr_max = getattr(ctx.engine, 'trace_repr_max', 200)
                    frame = _repr_step(step, max_len=repr_max)
                    lang_stack.append(frame)
                    if getattr(ctx.engine, 'trace_logging', False):
                        _log.debug("%s→ %s", "  " * (len(lang_stack) - 1), frame)
                else:
                    lang_stack = None

                try:
                    ctx.dest = handler.execute(step, ctx)
                except PipelineSignal as sig:
                    sig.handle(ctx)
                except ControlFlowSignal:
                    raise  # $break / $continue / $return — not errors, don't annotate
                except Exception as e:
                    if lang_stack is not None and not hasattr(e, '_j_perm_lang_stack'):
                        e._j_perm_lang_stack = list(lang_stack)
                    raise
                finally:
                    if lang_stack is not None:
                        lang_stack.pop()

    # -- async execution ----------------------------------------------------

    async def run_async(self, spec: Any, ctx: ExecutionContext) -> None:
        """Run the pipeline asynchronously.

        Supports async stages, middlewares, and handlers. Sync components
        are called directly, async components are awaited.

        *spec* is either a single step or a list of steps.
        After ``run_async`` returns, the result lives in ``ctx.dest``.
        """
        steps: List[Any] = spec if isinstance(spec, list) else [spec]
        # Use async version of stages if any are async
        steps = await self.stages.run_all_async(steps, ctx)

        for step in steps:
            # Process middlewares (check if async)
            for mw in sorted(self._middlewares, key=lambda m: m.priority, reverse=True):
                if isinstance(mw, AsyncMiddleware):
                    step = await mw.process(step, ctx)
                else:
                    step = mw.process(step, ctx)

            handlers = self.registry.resolve(step)
            if not handlers:
                raise ValueError(f"unhandled step: {step!r}")
            for handler in handlers:
                # Increment operation counter
                ctx.metadata['_operation_count'] = ctx.metadata.get('_operation_count', 0) + 1
                max_ops = getattr(ctx.engine, 'max_operations', float('inf'))
                if ctx.metadata['_operation_count'] > max_ops:
                    raise RuntimeError(
                        f"Operation limit exceeded: {ctx.metadata['_operation_count']} operations executed, "
                        f"maximum allowed is {max_ops}"
                    )

                # Track language-level execution stack
                if self.track_execution:
                    lang_stack = ctx.metadata.setdefault(_LANG_EXEC_STACK_KEY, [])
                    repr_max = getattr(ctx.engine, 'trace_repr_max', 200)
                    frame = _repr_step(step, max_len=repr_max)
                    lang_stack.append(frame)
                    if getattr(ctx.engine, 'trace_logging', False):
                        _log.debug("%s→ %s", "  " * (len(lang_stack) - 1), frame)
                else:
                    lang_stack = None

                try:
                    # Check if handler is async
                    if isinstance(handler, AsyncActionHandler):
                        ctx.dest = await handler.execute(step, ctx)
                    else:
                        ctx.dest = handler.execute(step, ctx)
                except PipelineSignal as sig:
                    sig.handle(ctx)
                except ControlFlowSignal:
                    raise  # $break / $continue / $return — not errors, don't annotate
                except Exception as e:
                    if lang_stack is not None and not hasattr(e, '_j_perm_lang_stack'):
                        e._j_perm_lang_stack = list(lang_stack)
                    raise
                finally:
                    if lang_stack is not None:
                        lang_stack.pop()


# ─────────────────────────────────────────────────────────────────────────────
# UnescapeRule — post-stabilisation escape cleanup
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class UnescapeRule:
    """A single unescape pass applied after the value-stabilisation loop.

    Each handler that introduces an escape convention (e.g. ``$${`` for
    template literals) exports a matching unescape function and registers it
    here.  Engine applies all rules in descending *priority* order once the
    value has converged.

    Attributes:
        name:     Human-readable label (for debugging / deduplication).
        priority: Higher = runs first.  Use 0 as baseline.
        unescape: ``value → value`` — must recurse into containers itself
                  (see ``handlers.template.template_unescape`` for an example).
    """

    name: str
    priority: int
    unescape: Callable[[Any], Any]


# ─────────────────────────────────────────────────────────────────────────────
# Engine — orchestrator / public entry point
# ─────────────────────────────────────────────────────────────────────────────


class Engine:
    """Top-level orchestrator.  Holds the resolver and all pipelines, creates
    execution contexts, and owns the value-stabilisation loop.

    Pipeline categories:

    * ``main_pipeline`` – entry point, invoked by ``apply()``.
    * ``value_pipeline`` – used by ``process_value()`` (with stabilisation).
    * ``_pipelines`` (named) – invoked on demand via ``run_pipeline(name, …)``.

    Algorithms (see individual method docs for details):

    * ``apply``          – normalize source, deepcopy dest, run main_pipeline,
                           return deepcopy of result.
    * ``process_value``  – loop ``value_pipeline.run([val])`` until output ==
                           input or ``value_max_depth`` exceeded.
    * ``run_pipeline``   – run a named pipeline with an *isolated* dest copy;
                           original ctx is never mutated.
    """

    def __init__(
            self,
            *,
            resolver: ValueResolver,
            processor: ValueProcessor,
            main_pipeline: Pipeline,
            value_pipeline: Optional[Pipeline] = None,
            value_max_depth: int = 50,
            pipelines: Optional[dict[str, Pipeline]] = None,
            unescape_rules: Optional[List[UnescapeRule]] = None,
            custom_functions: Optional[dict[str, Callable]] = None,
            max_operations: int = 1_000_000,
            max_function_recursion_depth: int = 100,
            trace_logging: bool = False,
            trace_repr_max: Optional[int] = 200,
    ) -> None:
        self.resolver = resolver
        self.processor = processor
        self.main_pipeline = main_pipeline
        self.value_pipeline = value_pipeline
        self.value_max_depth = value_max_depth
        self.max_operations = max_operations
        self.max_function_recursion_depth = max_function_recursion_depth
        self._pipelines = dict(pipelines) if pipelines else {}
        self._unescape_rules = sorted(unescape_rules or [], key=lambda r: r.priority, reverse=True)
        self.trace_logging = trace_logging
        """If ``True``, emit a ``DEBUG`` log line for every main-pipeline step as it executes."""
        self.trace_repr_max = trace_repr_max
        """Max characters per step in the language call stack / trace output.
        ``None`` disables truncation and shows each step in full.
        """
        for name, func in (custom_functions or {}).items():
            setattr(self, name, func)

    # -- registration -------------------------------------------------------

    def register_pipeline(self, name: str, pipeline: Pipeline) -> None:
        """Register a named pipeline (callable via ``run_pipeline``)."""
        self._pipelines[name] = pipeline

    def register_custom_function(self, name: str, func: Callable[[Any], Any]) -> None:
        """Add a custom function as an Engine method (callable from handlers)."""
        setattr(self, name, func)

    # -- public API ---------------------------------------------------------

    def apply(self, spec: Any, *, source: Any, dest: Any) -> Any:
        """Execute a DSL script through *main_pipeline*.

        *source* is normalized (tuples → lists for JMESPath compatibility).
        *dest* is deep-copied before processing; the return value is another
        deep copy so the caller's original is never touched.

        On unhandled error, logs the language-level call stack at ``ERROR``
        level via the ``j_perm`` logger before re-raising.
        """
        ctx = ExecutionContext(
            source=_tuples_to_lists(source),
            dest=copy.deepcopy(dest),
            engine=self,
        )
        try:
            self.main_pipeline.run(spec, ctx)
        except Exception as e:
            if not isinstance(e, (PipelineSignal, ControlFlowSignal)):
                lang_stack = getattr(e, '_j_perm_lang_stack', None)
                if lang_stack:
                    _log.error(
                        "j-perm execution failed: %s: %s\n"
                        "Language call stack (innermost last):\n%s",
                        type(e).__name__, e,
                        _format_lang_stack(lang_stack),
                    )
            raise
        return copy.deepcopy(ctx.dest)

    async def apply_async(self, spec: Any, *, source: Any, dest: Any) -> Any:
        """Async version of apply().

        Execute a DSL script through *main_pipeline* asynchronously.
        Supports async handlers, stages, and middlewares.

        On unhandled error, logs the language-level call stack at ``ERROR``
        level via the ``j_perm`` logger before re-raising.
        """
        ctx = ExecutionContext(
            source=_tuples_to_lists(source),
            dest=copy.deepcopy(dest),
            engine=self,
        )
        try:
            await self.main_pipeline.run_async(spec, ctx)
        except Exception as e:
            if not isinstance(e, (PipelineSignal, ControlFlowSignal)):
                lang_stack = getattr(e, '_j_perm_lang_stack', None)
                if lang_stack:
                    _log.error(
                        "j-perm execution failed: %s: %s\n"
                        "Language call stack (innermost last):\n%s",
                        type(e).__name__, e,
                        _format_lang_stack(lang_stack),
                    )
            raise
        return copy.deepcopy(ctx.dest)

    def apply_to_context(self, spec: Any, ctx: ExecutionContext) -> Any:
        """Like ``apply``, but takes a pre-constructed context and mutates it in-place.

        The caller is responsible for normalizing the source and deep-copying
        the dest if necessary.  Returns None; the result lives in ``ctx.dest``.
        """
        self.main_pipeline.run(spec, ctx)
        return copy.deepcopy(ctx.dest)

    async def apply_to_context_async(self, spec: Any, ctx: ExecutionContext) -> Any:
        """Async version of apply_to_context()."""
        await self.main_pipeline.run_async(spec, ctx)
        return copy.deepcopy(ctx.dest)

    def run_pipeline(self, name: str, spec: Any, ctx: ExecutionContext) -> Any:
        """Run a named pipeline with an isolated ``dest``.

        *source* is taken from *ctx*; *dest* is deep-copied so the original
        context is never mutated.  Returns a deep copy of the sub-context's
        final ``dest``.

        Typical call site (inside a handler)::

            result = ctx.engine.run_pipeline("name", spec, ctx)
        """
        if name not in self._pipelines:
            raise KeyError(f"Pipeline {name!r} not registered")
        pipeline = self._pipelines[name]
        log_pl = logging.getLogger(f"j_perm.pipeline.{name}")
        # Share the parent's lang_exec_stack so named-pipeline steps are
        # visible in the integrated trace and error call stack.
        parent_stack = ctx.metadata.get(_LANG_EXEC_STACK_KEY)
        sub_meta: dict = {_LANG_EXEC_STACK_KEY: parent_stack} if parent_stack is not None else {}
        sub_ctx = ExecutionContext(
            source=ctx.source,
            dest=copy.deepcopy(ctx.dest),
            engine=self,
            metadata=sub_meta,
        )
        if log_pl.isEnabledFor(logging.DEBUG):
            depth = len(parent_stack) if parent_stack is not None else 0
            log_pl.debug("%s→ [pipeline:%s]", "  " * depth, name)
        try:
            pipeline.run(spec, sub_ctx)
        except Exception as e:
            if not isinstance(e, (PipelineSignal, ControlFlowSignal)):
                lang_stack = getattr(e, '_j_perm_lang_stack', None)
                if lang_stack:
                    log_pl.error(
                        "pipeline %r failed: %s: %s\n"
                        "Language call stack (innermost last):\n%s",
                        name, type(e).__name__, e,
                        _format_lang_stack(lang_stack),
                    )
            raise
        return copy.deepcopy(sub_ctx.dest)

    async def run_pipeline_async(self, name: str, spec: Any, ctx: ExecutionContext) -> Any:
        """Async version of run_pipeline().

        Typical call site (inside an async handler)::

            result = await ctx.engine.run_pipeline_async("name", spec, ctx)
        """
        if name not in self._pipelines:
            raise KeyError(f"Pipeline {name!r} not registered")
        pipeline = self._pipelines[name]
        log_pl = logging.getLogger(f"j_perm.pipeline.{name}")
        parent_stack = ctx.metadata.get(_LANG_EXEC_STACK_KEY)
        sub_meta: dict = {_LANG_EXEC_STACK_KEY: parent_stack} if parent_stack is not None else {}
        sub_ctx = ExecutionContext(
            source=ctx.source,
            dest=copy.deepcopy(ctx.dest),
            engine=self,
            metadata=sub_meta,
        )
        if log_pl.isEnabledFor(logging.DEBUG):
            depth = len(parent_stack) if parent_stack is not None else 0
            log_pl.debug("%s→ [pipeline:%s]", "  " * depth, name)
        try:
            await pipeline.run_async(spec, sub_ctx)
        except Exception as e:
            if not isinstance(e, (PipelineSignal, ControlFlowSignal)):
                lang_stack = getattr(e, '_j_perm_lang_stack', None)
                if lang_stack:
                    log_pl.error(
                        "pipeline %r failed: %s: %s\n"
                        "Language call stack (innermost last):\n%s",
                        name, type(e).__name__, e,
                        _format_lang_stack(lang_stack),
                    )
            raise
        return copy.deepcopy(sub_ctx.dest)

    def process_value(self, value: Any, ctx: ExecutionContext, *, _unescape: bool = True) -> Any:
        """Run *value_pipeline* over *value* until it stabilises.

        Each iteration feeds the current value as ``[value]`` into
        ``value_pipeline.run``.  The loop continues while ``output != input``,
        up to *value_max_depth* iterations (raises ``RecursionError`` on
        overflow — catches oscillating template cycles like ``a→b→a``).

        ``_unescape=True`` (default) – after stabilisation, apply every
        registered ``UnescapeRule`` in priority order.  Set to ``False`` by
        ``RecursiveDescentHandler`` so that unescaping happens only once at
        the outermost invocation.

        If *value_pipeline* is ``None``, returns *value* unchanged.
        """
        if self.value_pipeline is None:
            return value

        trace = _log_values.isEnabledFor(logging.DEBUG)
        repr_max = self.trace_repr_max
        indent = "  " * len(ctx.metadata.get(_LANG_EXEC_STACK_KEY, []))

        current = value
        for _ in range(self.value_max_depth):
            # Save real dest in metadata for JMESPath access
            # Preserve existing _real_dest if we're in a nested process_value call
            metadata_with_dest = {**ctx.metadata, '_real_dest': ctx.metadata.get('_real_dest', ctx.dest)}
            value_ctx = ctx.copy(
                new_dest=current,
                new_metadata=metadata_with_dest,
            )
            try:
                self.value_pipeline.run([current], value_ctx)
            except PipelineSignal:
                current = value_ctx.dest
                break
            if value_ctx.dest == current:
                break
            if trace:
                _log_values.debug(
                    "%s  %s → %s",
                    indent,
                    _repr_step(current, max_len=repr_max),
                    _repr_step(value_ctx.dest, max_len=repr_max),
                )
            current = value_ctx.dest
        else:
            raise RecursionError("value_max_depth exceeded")

        if _unescape:
            for rule in self._unescape_rules:
                current = rule.unescape(current)
        return current

    async def process_value_async(self, value: Any, ctx: ExecutionContext, *, _unescape: bool = True) -> Any:
        """Async version of process_value().

        Run *value_pipeline* over *value* asynchronously until it stabilises.
        Supports async handlers in the value pipeline.
        """
        if self.value_pipeline is None:
            return value

        trace = _log_values.isEnabledFor(logging.DEBUG)
        repr_max = self.trace_repr_max
        indent = "  " * len(ctx.metadata.get(_LANG_EXEC_STACK_KEY, []))

        current = value
        for _ in range(self.value_max_depth):
            # Save real dest in metadata for JMESPath access
            # Preserve existing _real_dest if we're in a nested process_value call
            metadata_with_dest = {**ctx.metadata, '_real_dest': ctx.metadata.get('_real_dest', ctx.dest)}
            value_ctx = ExecutionContext(
                source=ctx.source,
                dest=current,
                engine=self,
                metadata=metadata_with_dest,
            )
            try:
                await self.value_pipeline.run_async([current], value_ctx)
            except PipelineSignal:
                current = value_ctx.dest
                break
            if value_ctx.dest == current:
                break
            if trace:
                _log_values.debug(
                    "%s  %s → %s",
                    indent,
                    _repr_step(current, max_len=repr_max),
                    _repr_step(value_ctx.dest, max_len=repr_max),
                )
            current = value_ctx.dest
        else:
            raise RecursionError("value_max_depth exceeded")

        if _unescape:
            for rule in self._unescape_rules:
                current = rule.unescape(current)
        return current
