"""Engine factory — the single place where all pieces are assembled.

``build_default_engine`` is the recommended entry point for a fully functional
synchronous Engine.  ``build_default_async_engine`` is its async twin: an
*equivalent* engine (same ops, same constructs, same limits) whose recursive
handlers are async, so that an ``AsyncActionHandler`` (e.g. an async SQL handler
or an async ``$func``) is awaited wherever it appears — as a step, inside a
compound body, inside a ``$func`` body, or in value position.  Drive it through
``engine.apply_async`` / ``engine.apply_compiled_async``.

Customisation points:

* **specials**        – dict of ``SpecialFn`` for ``$ref``, ``$eval``, etc.
                        ``None`` → uses default (ref_handler, eval_handler).
* **value_max_depth** – stabilisation-loop limit (default 50).
"""

from __future__ import annotations

from typing import Mapping, Callable, Any

import jmespath

from .casters import BUILTIN_CASTERS
from .core import (
    ActionNode,
    ActionTypeRegistry,
    Engine,
    Pipeline,
    UnescapeRule,
)
from .handlers import constructs as _constructs
from .handlers import constructs_async as _constructs_async
from .handlers.container import ContainerMatcher, RecursiveDescentHandler
from .handlers.container_async import AsyncRecursiveDescentHandler
from .handlers.flow import (
    BreakMatcher, BreakHandler,
    ContinueMatcher, ContinueHandler,
)
from .handlers.function import (
    DefMatcher, CallMatcher, DefHandler, CallHandler,
    RaiseMatcher, RaiseHandler,
    ReturnMatcher, ReturnHandler,
)
from .handlers.function_async import (
    AsyncDefHandler, AsyncCallHandler, AsyncRaiseHandler, AsyncReturnHandler,
)
from .handlers.identity import IdentityHandler
from .handlers.ops import (
    SetHandler, CopyHandler,
    DeleteHandler,
    ForeachHandler, WhileHandler, IfHandler, ExecHandler,
    UpdateHandler, DistinctHandler,
    AssertHandler,
    TryHandler,
    DeserializeHandler,
)
from .handlers.ops_async import (
    AsyncSetHandler, AsyncCopyHandler, AsyncDeleteHandler,
    AsyncForeachHandler, AsyncWhileHandler, AsyncIfHandler, AsyncExecHandler,
    AsyncUpdateHandler, AsyncDistinctHandler, AsyncAssertHandler,
    AsyncTryHandler, AsyncDeserializeHandler,
)
from .handlers.special import SpecialFn, SpecialMatcher, SpecialResolveHandler
from .handlers.special_async import AsyncSpecialResolveHandler
from .handlers.template import TemplMatcher, TemplSubstHandler, template_unescape
from .matchers import AlwaysMatcher, OpMatcher
from j_perm.processors.pointer_processor import PointerProcessor
from .resolvers.pointer import PointerResolver
from .stages.shorthands import build_default_shorthand_stages


def _default_specials(
        c: Any,
        call_execute: Callable[..., Any],
        resolved_casters: Mapping[str, Any],
        *,
        regex_timeout: float,
        regex_allowed_flags: int | None,
        pow_max_base: float,
        pow_max_exponent: float,
        mul_max_string_result: int,
        mul_max_operand: float,
        str_max_split_results: int,
        str_max_join_result: int,
        str_max_replace_result: int,
        add_max_number_result: float,
        add_max_string_result: int,
        sub_max_number_result: float,
) -> dict[str, SpecialFn]:
    """Build the default ``$``-construct map from a constructs module *c*.

    *c* is either :mod:`j_perm.handlers.constructs` (sync) or
    :mod:`j_perm.handlers.constructs_async` (async) — both expose the same names,
    so the same wiring produces a sync or async special-construct set.
    *call_execute* is the (sync or async) ``$func`` callable.
    """
    return {
        "$ref": c.ref_handler,
        "$eval": c.eval_handler,
        "$and": c.and_handler,
        "$or": c.or_handler,
        "$not": c.not_handler,
        "$cast": c.make_cast_handler(resolved_casters),
        "$gt": c.gt_handler,
        "$gte": c.gte_handler,
        "$lt": c.lt_handler,
        "$lte": c.lte_handler,
        "$eq": c.eq_handler,
        "$ne": c.ne_handler,
        "$in": c.in_handler,
        "$exists": c.exists_handler,
        "$add": c.make_add_handler(
            max_number_result=add_max_number_result,
            max_string_result=add_max_string_result,
        ),
        "$sub": c.make_sub_handler(
            max_number_result=sub_max_number_result,
        ),
        "$mul": c.make_mul_handler(
            max_string_result=mul_max_string_result,
            max_operand=mul_max_operand,
        ),
        "$div": c.div_handler,
        "$pow": c.make_pow_handler(
            max_base=pow_max_base,
            max_exponent=pow_max_exponent,
        ),
        "$mod": c.mod_handler,
        "$round": c.round_handler,
        # String operations
        "$str_split": c.make_str_split_handler(max_results=str_max_split_results),
        "$str_join": c.make_str_join_handler(max_result_length=str_max_join_result),
        "$str_slice": c.str_slice_handler,
        "$str_upper": c.str_upper_handler,
        "$str_lower": c.str_lower_handler,
        "$str_strip": c.str_strip_handler,
        "$str_lstrip": c.str_lstrip_handler,
        "$str_rstrip": c.str_rstrip_handler,
        "$str_replace": c.make_str_replace_handler(max_result_length=str_max_replace_result),
        "$str_contains": c.str_contains_handler,
        "$str_startswith": c.str_startswith_handler,
        "$str_endswith": c.str_endswith_handler,
        # Regex operations
        "$regex_match": c.make_regex_match_handler(
            timeout=regex_timeout, allowed_flags=regex_allowed_flags),
        "$regex_search": c.make_regex_search_handler(
            timeout=regex_timeout, allowed_flags=regex_allowed_flags),
        "$regex_findall": c.make_regex_findall_handler(
            timeout=regex_timeout, allowed_flags=regex_allowed_flags),
        "$regex_replace": c.make_regex_replace_handler(
            timeout=regex_timeout, allowed_flags=regex_allowed_flags),
        "$regex_groups": c.make_regex_groups_handler(
            timeout=regex_timeout, allowed_flags=regex_allowed_flags),
        # $func before $raw so {"$func": ..., "$raw": True} dispatches to $func.
        "$func": call_execute,
        "$raw": c.raw_handler,
    }


def _build_value_pipeline(
        *,
        specials: Mapping[str, SpecialFn],
        special_handler: Any,
        container_handler: Any,
        casters: Mapping[str, Callable[[Any], Any]] | None,
        jmes_options: jmespath.Options | None,
) -> Pipeline:
    """Assemble the value pipeline from the given (sync or async) handlers."""
    special_keys: set[str] = set(specials.keys())
    value_reg = ActionTypeRegistry()

    if specials:
        value_reg.register(ActionNode(
            name="special", priority=10,
            matcher=SpecialMatcher(special_keys),
            handler=special_handler,
        ))

    value_reg.register(ActionNode(
        name="template", priority=8,
        matcher=TemplMatcher(),
        handler=TemplSubstHandler(casters=casters, jmes_options=jmes_options),
    ))
    value_reg.register(ActionNode(
        name="container", priority=5,
        matcher=ContainerMatcher(special_keys),
        handler=container_handler,
    ))
    value_reg.register(ActionNode(
        name="identity", priority=-999,
        matcher=AlwaysMatcher(),
        handler=IdentityHandler(),
    ))

    return Pipeline(registry=value_reg)


def _register_main_ops(main_reg: ActionTypeRegistry, ops: Mapping[str, Any]) -> None:
    """Register all built-in operation nodes from a bundle of handler instances.

    The *ops* bundle maps each op name to its handler; ``break`` / ``continue``
    are always the (stateless) sync handlers.  Both the sync and async builders
    call this with their respective bundles so the two engines stay equivalent.
    """
    nodes: list[tuple[str, Any, Any]] = [
        ("set", OpMatcher("set"), ops["set"]),
        ("copy", OpMatcher("copy"), ops["copy"]),
        ("delete", OpMatcher("delete"), ops["delete"]),
        ("foreach", OpMatcher("foreach"), ops["foreach"]),
        ("while", OpMatcher("while"), ops["while"]),
        ("if", OpMatcher("if"), ops["if"]),
        ("exec", OpMatcher("exec"), ops["exec"]),
        ("update", OpMatcher("update"), ops["update"]),
        ("distinct", OpMatcher("distinct"), ops["distinct"]),
        ("assert", OpMatcher("assert"), ops["assert"]),
        ("try", OpMatcher("try"), ops["try"]),
        ("deserialize", OpMatcher("deserialize"), ops["deserialize"]),
        ("def", DefMatcher(), ops["def"]),
        ("func", CallMatcher(), ops["func"]),
        ("raise", RaiseMatcher(), ops["raise"]),
        ("return", ReturnMatcher(), ops["return"]),
        ("break", BreakMatcher(), BreakHandler()),
        ("continue", ContinueMatcher(), ContinueHandler()),
    ]
    for name, matcher, handler in nodes:
        main_reg.register(ActionNode(name=name, priority=10, matcher=matcher, handler=handler))


def _make_engine(
        *,
        specials: Mapping[str, SpecialFn] | None,
        casters: Mapping[str, Callable[[Any], Any]] | None,
        jmes_options: jmespath.Options | None,
        value_max_depth: int,
        regex_timeout: float,
        regex_allowed_flags: int | None,
        pow_max_base: float,
        pow_max_exponent: float,
        mul_max_string_result: int,
        mul_max_operand: float,
        str_max_split_results: int,
        str_max_join_result: int,
        str_max_replace_result: int,
        max_operations: int,
        max_function_recursion_depth: int,
        add_max_number_result: float,
        add_max_string_result: int,
        sub_max_number_result: float,
        trace_logging: bool,
        trace_repr_max: int | None,
        constructs_module: Any,
        special_handler_cls: Any,
        container_handler: Any,
        call_handler: Any,
        ops: Mapping[str, Any],
) -> Engine:
    """Shared assembly for the sync and async engine builders."""
    resolver = PointerResolver()
    processor = PointerProcessor()

    if specials is None:
        resolved_casters = casters if casters is not None else BUILTIN_CASTERS
        specials = _default_specials(
            constructs_module,
            call_handler.execute,
            resolved_casters,
            regex_timeout=regex_timeout,
            regex_allowed_flags=regex_allowed_flags,
            pow_max_base=pow_max_base,
            pow_max_exponent=pow_max_exponent,
            mul_max_string_result=mul_max_string_result,
            mul_max_operand=mul_max_operand,
            str_max_split_results=str_max_split_results,
            str_max_join_result=str_max_join_result,
            str_max_replace_result=str_max_replace_result,
            add_max_number_result=add_max_number_result,
            add_max_string_result=add_max_string_result,
            sub_max_number_result=sub_max_number_result,
        )

    value_pipeline = _build_value_pipeline(
        specials=specials,
        special_handler=special_handler_cls(specials),
        container_handler=container_handler,
        casters=casters,
        jmes_options=jmes_options,
    )

    main_stages = build_default_shorthand_stages()
    main_reg = ActionTypeRegistry()
    _register_main_ops(main_reg, ops)
    main_pipeline = Pipeline(stages=main_stages, registry=main_reg, track_execution=True)

    unescape_rules = [
        UnescapeRule(name="template", priority=0, unescape=template_unescape),
    ]

    return Engine(
        resolver=resolver,
        processor=processor,
        main_pipeline=main_pipeline,
        value_pipeline=value_pipeline,
        value_max_depth=value_max_depth,
        unescape_rules=unescape_rules,
        max_operations=max_operations,
        max_function_recursion_depth=max_function_recursion_depth,
        trace_logging=trace_logging,
        trace_repr_max=trace_repr_max,
    )


def build_default_engine(
        *,
        specials: Mapping[str, SpecialFn] | None = None,
        casters: Mapping[str, Callable[[Any], Any]] | None = None,
        jmes_options: jmespath.Options | None = None,
        value_max_depth: int = 50,
        # Security limits
        regex_timeout: float = 2.0,
        regex_allowed_flags: int | None = None,
        pow_max_base: float = 1e6,
        pow_max_exponent: float = 1000,
        mul_max_string_result: int = 1_000_000,
        mul_max_operand: float = 1e9,
        # Loop and iteration limits
        max_loop_iterations: int = 10_000,
        max_foreach_items: int = 100_000,
        # String operation limits
        str_max_split_results: int = 100_000,
        str_max_join_result: int = 10_000_000,
        str_max_replace_result: int = 10_000_000,
        # Global limits
        max_operations: int = 1_000_000,
        max_function_recursion_depth: int = 100,
        # Math operation result limits
        add_max_number_result: float = 1e15,
        add_max_string_result: int = 100_000_000,
        sub_max_number_result: float = 1e15,
        # Logging / tracing
        trace_logging: bool = False,
        trace_repr_max: int | None = 200,
) -> Engine:
    """Assemble a synchronous Engine with the standard resolver and pipelines.

    See the module docstring for the async twin.  All keyword arguments are
    shared verbatim by :func:`build_default_async_engine`.

    Example::

        engine = build_default_engine()
        result = engine.apply(
            spec={"/name": "/user/name", "/age": "${int:/user/age}"},
            source={"user": {"name": "Alice", "age": "30"}},
            dest={},
        )
        # → {"name": "Alice", "age": "30"}
    """
    set_handler = SetHandler()
    ops = {
        "set": set_handler,
        "copy": CopyHandler(set_handler),
        "delete": DeleteHandler(),
        "foreach": ForeachHandler(max_items=max_foreach_items),
        "while": WhileHandler(max_iterations=max_loop_iterations),
        "if": IfHandler(),
        "exec": ExecHandler(),
        "update": UpdateHandler(),
        "distinct": DistinctHandler(),
        "assert": AssertHandler(),
        "try": TryHandler(),
        "deserialize": DeserializeHandler(set_handler=set_handler),
        "def": DefHandler(),
        "func": CallHandler(),
        "raise": RaiseHandler(),
        "return": ReturnHandler(),
    }
    return _make_engine(
        specials=specials, casters=casters, jmes_options=jmes_options,
        value_max_depth=value_max_depth,
        regex_timeout=regex_timeout, regex_allowed_flags=regex_allowed_flags,
        pow_max_base=pow_max_base, pow_max_exponent=pow_max_exponent,
        mul_max_string_result=mul_max_string_result, mul_max_operand=mul_max_operand,
        str_max_split_results=str_max_split_results, str_max_join_result=str_max_join_result,
        str_max_replace_result=str_max_replace_result,
        max_operations=max_operations,
        max_function_recursion_depth=max_function_recursion_depth,
        add_max_number_result=add_max_number_result, add_max_string_result=add_max_string_result,
        sub_max_number_result=sub_max_number_result,
        trace_logging=trace_logging, trace_repr_max=trace_repr_max,
        constructs_module=_constructs,
        special_handler_cls=SpecialResolveHandler,
        container_handler=RecursiveDescentHandler(),
        call_handler=CallHandler(),
        ops=ops,
    )


def build_default_async_engine(
        *,
        specials: Mapping[str, SpecialFn] | None = None,
        casters: Mapping[str, Callable[[Any], Any]] | None = None,
        jmes_options: jmespath.Options | None = None,
        value_max_depth: int = 50,
        regex_timeout: float = 2.0,
        regex_allowed_flags: int | None = None,
        pow_max_base: float = 1e6,
        pow_max_exponent: float = 1000,
        mul_max_string_result: int = 1_000_000,
        mul_max_operand: float = 1e9,
        max_loop_iterations: int = 10_000,
        max_foreach_items: int = 100_000,
        str_max_split_results: int = 100_000,
        str_max_join_result: int = 10_000_000,
        str_max_replace_result: int = 10_000_000,
        max_operations: int = 1_000_000,
        max_function_recursion_depth: int = 100,
        add_max_number_result: float = 1e15,
        add_max_string_result: int = 100_000_000,
        sub_max_number_result: float = 1e15,
        trace_logging: bool = False,
        trace_repr_max: int | None = 200,
) -> Engine:
    """Assemble the async twin of :func:`build_default_engine`.

    Identical wiring — same ops, constructs, casters and limits — but every
    recursive handler (compound ops, ``$func``/``$def``, the value-pipeline
    special/container dispatch and every ``$``-construct) is async, so an
    ``AsyncActionHandler`` is awaited wherever it is reached.  Stateless
    handlers (``$break`` / ``$continue`` / identity / template) stay sync; they
    run unchanged on the async path.

    Drive the resulting engine through the async entry points
    (``apply_async`` / ``apply_to_context_async`` / ``apply_compiled_async`` /
    ``run_pipeline_async``).

    Example::

        engine = build_default_async_engine()
        result = await engine.apply_async(
            spec={"op": "foreach", "in": "/items", "as": "it",
                  "parallel": True,
                  "do": [{"op": "set", "path": "/out/-",
                          "value": {"$func": "fetch", "args": ["&:/it"]}}]},
            source={"items": [1, 2, 3]}, dest={},
        )
    """
    set_handler = AsyncSetHandler()
    ops = {
        "set": set_handler,
        "copy": AsyncCopyHandler(set_handler),
        "delete": AsyncDeleteHandler(),
        "foreach": AsyncForeachHandler(max_items=max_foreach_items),
        "while": AsyncWhileHandler(max_iterations=max_loop_iterations),
        "if": AsyncIfHandler(),
        "exec": AsyncExecHandler(),
        "update": AsyncUpdateHandler(),
        "distinct": AsyncDistinctHandler(),
        "assert": AsyncAssertHandler(),
        "try": AsyncTryHandler(),
        "deserialize": AsyncDeserializeHandler(set_handler=set_handler),
        "def": AsyncDefHandler(),
        "func": AsyncCallHandler(),
        "raise": AsyncRaiseHandler(),
        "return": AsyncReturnHandler(),
    }
    return _make_engine(
        specials=specials, casters=casters, jmes_options=jmes_options,
        value_max_depth=value_max_depth,
        regex_timeout=regex_timeout, regex_allowed_flags=regex_allowed_flags,
        pow_max_base=pow_max_base, pow_max_exponent=pow_max_exponent,
        mul_max_string_result=mul_max_string_result, mul_max_operand=mul_max_operand,
        str_max_split_results=str_max_split_results, str_max_join_result=str_max_join_result,
        str_max_replace_result=str_max_replace_result,
        max_operations=max_operations,
        max_function_recursion_depth=max_function_recursion_depth,
        add_max_number_result=add_max_number_result, add_max_string_result=add_max_string_result,
        sub_max_number_result=sub_max_number_result,
        trace_logging=trace_logging, trace_repr_max=trace_repr_max,
        constructs_module=_constructs_async,
        special_handler_cls=AsyncSpecialResolveHandler,
        container_handler=AsyncRecursiveDescentHandler(),
        call_handler=AsyncCallHandler(),
        ops=ops,
    )
