"""Engine factory — the single place where all pieces are assembled.

``build_default_engine`` is the recommended entry point for users who want a
fully functional Engine without hand-wiring every registry.

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
    UnescapeRule
)
from .handlers.constructs import (
    ref_handler, eval_handler, and_handler, or_handler, not_handler,
    make_cast_handler,
    gt_handler, gte_handler, lt_handler, lte_handler, eq_handler, ne_handler,
    div_handler, mod_handler,
    make_add_handler, make_sub_handler, make_mul_handler, make_pow_handler,
    in_handler, exists_handler,
    # String operations
    make_str_split_handler, make_str_join_handler, str_slice_handler,
    str_upper_handler, str_lower_handler,
    str_strip_handler, str_lstrip_handler, str_rstrip_handler,
    make_str_replace_handler, str_contains_handler,
    str_startswith_handler, str_endswith_handler,
    # Regex operations
    make_regex_match_handler, make_regex_search_handler, make_regex_findall_handler,
    make_regex_replace_handler, make_regex_groups_handler,
    raw_handler,
)
from .handlers.container import ContainerMatcher, RecursiveDescentHandler
from .handlers.flow import (
    BreakMatcher, BreakHandler,
    ContinueMatcher, ContinueHandler,
)
from .handlers.function import (
    DefMatcher, CallMatcher, DefHandler, CallHandler,
    RaiseMatcher, RaiseHandler,
    ReturnMatcher, ReturnHandler,
)
from .handlers.identity import IdentityHandler
from .handlers.ops import (
    SetHandler, CopyHandler,
    DeleteHandler,
    ForeachHandler, WhileHandler, IfHandler, ExecHandler,
    UpdateHandler, DistinctHandler,
    AssertHandler,
    TryHandler,
)
from .handlers.special import SpecialFn, SpecialMatcher, SpecialResolveHandler
from .handlers.template import TemplMatcher, TemplSubstHandler, template_unescape
from .matchers import AlwaysMatcher, OpMatcher
from j_perm.processors.pointer_processor import PointerProcessor
from .resolvers.pointer import PointerResolver
from .stages.shorthands import build_default_shorthand_stages


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
) -> Engine:
    """Assemble an Engine with the standard resolver and pipelines.

    What gets wired
    ---------------
    resolver
        ``PointerResolver`` — self-contained JSON Pointer implementation.

    main_pipeline
        * Stages:
            - ``AssertShorthandProcessor``  (priority 100) — ``~assert``
            - ``DeleteShorthandProcessor``  (priority  50) — ``~delete``
            - ``AssignShorthandProcessor``  (priority   0) — fallback ``/path``
        * Registry: all 13 built-in ops (set, copy, delete, foreach, while, if, exec, update, distinct, assert, try, def, func).

    value_pipeline
        * ``SpecialResolveHandler``   (priority 10)  – ``$ref``, ``$eval``, ``$cast``,
          ``$and``, ``$or``, ``$not``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$eq``, ``$ne``,
          ``$in``, ``$exists``, ``$add``, ``$sub``, ``$mul``, ``$div``, ``$pow``, ``$mod``.
        * ``TemplSubstHandler``       (priority  8)  – ``${…}`` with built-in
          casters (int, float, bool, str), JMESPath function (subtract), and
          dest pointer (@:/path).
        * ``RecursiveDescentHandler`` (priority  5)  – recurse into containers.
        * ``IdentityHandler``         (priority -999, catch-all).

    unescape_rules
        * ``template_unescape``       (priority  0)  – strips ``$${`` → ``${``
          and ``$$`` → ``$``.

    Args:
        specials:                Custom special-construct handlers.  ``None`` → uses
                                 defaults (``$ref``, ``$eval``, ``$cast``, ``$and``, ``$or``, ``$not``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$eq``, ``$ne``).
        casters:                 Custom template casters (used in both ``${type:...}`` and ``$cast``).  ``None`` → uses built-in (int, float, bool, str).
        jmes_options:            Custom JMESPath options for template handler.  ``None`` → uses built-in with subtract function.
        value_max_depth:         Stabilisation-loop iteration cap.
        regex_timeout:           Timeout in seconds for regex operations (default: 2.0).
        regex_allowed_flags:     Bitmask of allowed regex flags. ``None`` → default safe flags
                                 (IGNORECASE, MULTILINE, DOTALL, VERBOSE, ASCII).
                                 Use -1 to allow all flags (not recommended for untrusted input).
        pow_max_base:            Maximum base value for ``$pow`` (default: 1e6).
        pow_max_exponent:        Maximum exponent value for ``$pow`` (default: 1000).
        mul_max_string_result:   Maximum length of string result in ``$mul`` (default: 1_000_000).
        mul_max_operand:         Maximum numeric operand value in ``$mul`` (default: 1e9).
        max_loop_iterations:     Maximum iterations for ``while`` loops (default: 10_000).
        max_foreach_items:       Maximum items to process in ``foreach`` (default: 100_000).
        str_max_split_results:   Maximum number of results from ``$str_split`` (default: 100_000).
        str_max_join_result:     Maximum length of result from ``$str_join`` (default: 10_000_000).
        str_max_replace_result:  Maximum length of result from ``$str_replace`` (default: 10_000_000).
        max_operations:          Maximum total operations allowed (default: 1_000_000).
        max_function_recursion_depth: Maximum function call depth (default: 100).
        add_max_number_result:   Maximum numeric result from ``$add`` (default: 1e15).
        add_max_string_result:   Maximum string length result from ``$add`` (default: 100_000_000).
        sub_max_number_result:   Maximum numeric result from ``$sub`` (default: 1e15).

    Returns:
        Fully wired ``Engine`` ready for use.

    Example::

        engine = build_default_engine()
        result = engine.apply(
            spec={"/name": "/user/name", "/age": "${int:/user/age}"},
            source={"user": {"name": "Alice", "age": "30"}},
            dest={},
        )
        # → {"name": "Alice", "age": "30"}
    """
    resolver = PointerResolver()
    processor = PointerProcessor()

    # -- default specials ---------------------------------------------------
    if specials is None:
        # Resolve casters for $cast handler
        resolved_casters = casters if casters is not None else BUILTIN_CASTERS
        specials = {
            "$ref": ref_handler,
            "$eval": eval_handler,
            "$and": and_handler,
            "$or": or_handler,
            "$not": not_handler,
            "$cast": make_cast_handler(resolved_casters),
            "$gt": gt_handler,
            "$gte": gte_handler,
            "$lt": lt_handler,
            "$lte": lte_handler,
            "$eq": eq_handler,
            "$ne": ne_handler,
            "$in": in_handler,
            "$exists": exists_handler,
            "$add": make_add_handler(
                max_number_result=add_max_number_result,
                max_string_result=add_max_string_result,
            ),
            "$sub": make_sub_handler(
                max_number_result=sub_max_number_result,
            ),
            "$mul": make_mul_handler(
                max_string_result=mul_max_string_result,
                max_operand=mul_max_operand,
            ),
            "$div": div_handler,
            "$pow": make_pow_handler(
                max_base=pow_max_base,
                max_exponent=pow_max_exponent,
            ),
            "$mod": mod_handler,
            # String operations
            "$str_split": make_str_split_handler(
                max_results=str_max_split_results,
            ),
            "$str_join": make_str_join_handler(
                max_result_length=str_max_join_result,
            ),
            "$str_slice": str_slice_handler,
            "$str_upper": str_upper_handler,
            "$str_lower": str_lower_handler,
            "$str_strip": str_strip_handler,
            "$str_lstrip": str_lstrip_handler,
            "$str_rstrip": str_rstrip_handler,
            "$str_replace": make_str_replace_handler(
                max_result_length=str_max_replace_result,
            ),
            "$str_contains": str_contains_handler,
            "$str_startswith": str_startswith_handler,
            "$str_endswith": str_endswith_handler,
            # Regex operations
            "$regex_match": make_regex_match_handler(
                timeout=regex_timeout,
                allowed_flags=regex_allowed_flags,
            ),
            "$regex_search": make_regex_search_handler(
                timeout=regex_timeout,
                allowed_flags=regex_allowed_flags,
            ),
            "$regex_findall": make_regex_findall_handler(
                timeout=regex_timeout,
                allowed_flags=regex_allowed_flags,
            ),
            "$regex_replace": make_regex_replace_handler(
                timeout=regex_timeout,
                allowed_flags=regex_allowed_flags,
            ),
            "$regex_groups": make_regex_groups_handler(
                timeout=regex_timeout,
                allowed_flags=regex_allowed_flags,
            ),
            # $func is registered here (not as a separate node) so that
            # SpecialResolveHandler handles it — giving $raw: True flag support.
            # It must come before $raw so {"$func": ..., "$raw": True} dispatches
            # to $func first.
            "$func": CallHandler().execute,
            # $raw must be last so that {"$ref": ..., "$raw": True} dispatches
            # to its primary construct first; the flag is then caught by
            # SpecialResolveHandler after the primary handler returns.
            "$raw": raw_handler,
        }

    # -- value pipeline -----------------------------------------------------
    special_keys: set[str] = set(specials.keys())
    value_reg = ActionTypeRegistry()

    if specials:
        value_reg.register(ActionNode(
            name="special", priority=10,
            matcher=SpecialMatcher(special_keys),
            handler=SpecialResolveHandler(specials),
        ))

    value_reg.register(ActionNode(
        name="template", priority=8,
        matcher=TemplMatcher(),
        handler=TemplSubstHandler(casters=casters, jmes_options=jmes_options),
    ))
    value_reg.register(ActionNode(
        name="container", priority=5,
        matcher=ContainerMatcher(special_keys),
        handler=RecursiveDescentHandler(),
    ))
    value_reg.register(ActionNode(
        name="identity", priority=-999,
        matcher=AlwaysMatcher(),
        handler=IdentityHandler(),
    ))

    value_pipeline = Pipeline(registry=value_reg)

    # -- main pipeline ------------------------------------------------------
    # Stages (shorthand expansion via StageNodes)
    main_stages = build_default_shorthand_stages()

    # Registry (all ops)
    main_reg = ActionTypeRegistry()

    # Instantiate shared SetHandler for copy/copyD reuse
    set_handler = SetHandler()

    main_reg.register(ActionNode(
        name="set", priority=10,
        matcher=OpMatcher("set"),
        handler=set_handler,
    ))
    main_reg.register(ActionNode(
        name="copy", priority=10,
        matcher=OpMatcher("copy"),
        handler=CopyHandler(set_handler),
    ))
    main_reg.register(ActionNode(
        name="delete", priority=10,
        matcher=OpMatcher("delete"),
        handler=DeleteHandler(),
    ))
    main_reg.register(ActionNode(
        name="foreach", priority=10,
        matcher=OpMatcher("foreach"),
        handler=ForeachHandler(max_items=max_foreach_items),
    ))
    main_reg.register(ActionNode(
        name="while", priority=10,
        matcher=OpMatcher("while"),
        handler=WhileHandler(max_iterations=max_loop_iterations),
    ))
    main_reg.register(ActionNode(
        name="if", priority=10,
        matcher=OpMatcher("if"),
        handler=IfHandler(),
    ))
    main_reg.register(ActionNode(
        name="exec", priority=10,
        matcher=OpMatcher("exec"),
        handler=ExecHandler(),
    ))
    main_reg.register(ActionNode(
        name="update", priority=10,
        matcher=OpMatcher("update"),
        handler=UpdateHandler(),
    ))
    main_reg.register(ActionNode(
        name="distinct", priority=10,
        matcher=OpMatcher("distinct"),
        handler=DistinctHandler(),
    ))
    main_reg.register(ActionNode(
        name="assert", priority=10,
        matcher=OpMatcher("assert"),
        handler=AssertHandler(),
    ))
    main_reg.register(ActionNode(
        name="try", priority=10,
        matcher=OpMatcher("try"),
        handler=TryHandler(),
    ))
    main_reg.register(ActionNode(
        name="def", priority=10,
        matcher=DefMatcher(),
        handler=DefHandler(),
    ))
    main_reg.register(ActionNode(
        name="func", priority=10,
        matcher=CallMatcher(),
        handler=CallHandler(),
    ))
    main_reg.register(ActionNode(
        name="raise", priority=10,
        matcher=RaiseMatcher(),
        handler=RaiseHandler(),
    ))
    main_reg.register(ActionNode(
        name="return", priority=10,
        matcher=ReturnMatcher(),
        handler=ReturnHandler(),
    ))
    main_reg.register(ActionNode(
        name="break", priority=10,
        matcher=BreakMatcher(),
        handler=BreakHandler(),
    ))
    main_reg.register(ActionNode(
        name="continue", priority=10,
        matcher=ContinueMatcher(),
        handler=ContinueHandler(),
    ))

    main_pipeline = Pipeline(stages=main_stages, registry=main_reg)

    # -- unescape rules -----------------------------------------------------
    unescape_rules = [
        UnescapeRule(name="template", priority=0, unescape=template_unescape),
    ]

    # -- engine -------------------------------------------------------------
    return Engine(
        resolver=resolver,
        processor=processor,
        main_pipeline=main_pipeline,
        value_pipeline=value_pipeline,
        value_max_depth=value_max_depth,
        unescape_rules=unescape_rules,
        max_operations=max_operations,
        max_function_recursion_depth=max_function_recursion_depth,
    )
