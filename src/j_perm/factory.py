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
    add_handler, sub_handler, mul_handler, div_handler, pow_handler, mod_handler,
    in_handler,
    # String operations
    str_split_handler, str_join_handler, str_slice_handler,
    str_upper_handler, str_lower_handler,
    str_strip_handler, str_lstrip_handler, str_rstrip_handler,
    str_replace_handler, str_contains_handler,
    str_startswith_handler, str_endswith_handler,
    # Regex operations
    regex_match_handler, regex_search_handler, regex_findall_handler,
    regex_replace_handler, regex_groups_handler,
)
from .handlers.container import ContainerMatcher, RecursiveDescentHandler
from .handlers.function import (
    DefMatcher, CallMatcher, DefHandler, CallHandler,
    RaiseMatcher, RaiseHandler
)
from .handlers.identity import IdentityHandler
from .handlers.ops import (
    SetHandler, CopyHandler,
    DeleteHandler,
    ForeachHandler, WhileHandler, IfHandler, ExecHandler,
    UpdateHandler, DistinctHandler,
    AssertHandler,
)
from .handlers.special import SpecialFn, SpecialMatcher, SpecialResolveHandler
from .handlers.template import TemplMatcher, TemplSubstHandler, template_unescape
from .matchers import AlwaysMatcher, OpMatcher
from .pointer_processor import PointerProcessor
from .resolvers.pointer import PointerResolver
from .stages.shorthands import build_default_shorthand_stages


def build_default_engine(
        *,
        specials: Mapping[str, SpecialFn] | None = None,
        casters: Mapping[str, Callable[[Any], Any]] | None = None,
        jmes_options: jmespath.Options | None = None,
        value_max_depth: int = 50,
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
        * Registry: all 12 built-in ops (set, copy, delete, foreach, while, if, exec, update, distinct, assert, def, func).

    value_pipeline
        * ``SpecialResolveHandler``   (priority 10)  – ``$ref``, ``$eval``, ``$cast``,
          ``$and``, ``$or``, ``$not``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$eq``, ``$ne``,
          ``$add``, ``$sub``, ``$mul``, ``$div``, ``$pow``, ``$mod``.
        * ``TemplSubstHandler``       (priority  8)  – ``${…}`` with built-in
          casters (int, float, bool, str), JMESPath function (subtract), and
          dest pointer (@:/path).
        * ``RecursiveDescentHandler`` (priority  5)  – recurse into containers.
        * ``IdentityHandler``         (priority -999, catch-all).

    unescape_rules
        * ``template_unescape``       (priority  0)  – strips ``$${`` → ``${``
          and ``$$`` → ``$``.

    Args:
        specials:        Custom special-construct handlers.  ``None`` → uses
                         defaults (``$ref``, ``$eval``, ``$cast``, ``$and``, ``$or``, ``$not``, ``$gt``, ``$gte``, ``$lt``, ``$lte``, ``$eq``, ``$ne``).
        casters:         Custom template casters (used in both ``${type:...}`` and ``$cast``).  ``None`` → uses built-in (int, float, bool, str).
        jmes_options:    Custom JMESPath options for template handler.  ``None`` → uses built-in with subtract function.
        value_max_depth: Stabilisation-loop iteration cap.

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
            "$add": add_handler,
            "$sub": sub_handler,
            "$mul": mul_handler,
            "$div": div_handler,
            "$pow": pow_handler,
            "$mod": mod_handler,
            # String operations
            "$str_split": str_split_handler,
            "$str_join": str_join_handler,
            "$str_slice": str_slice_handler,
            "$str_upper": str_upper_handler,
            "$str_lower": str_lower_handler,
            "$str_strip": str_strip_handler,
            "$str_lstrip": str_lstrip_handler,
            "$str_rstrip": str_rstrip_handler,
            "$str_replace": str_replace_handler,
            "$str_contains": str_contains_handler,
            "$str_startswith": str_startswith_handler,
            "$str_endswith": str_endswith_handler,
            # Regex operations
            "$regex_match": regex_match_handler,
            "$regex_search": regex_search_handler,
            "$regex_findall": regex_findall_handler,
            "$regex_replace": regex_replace_handler,
            "$regex_groups": regex_groups_handler,
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
        name="function", priority=9,
        matcher=CallMatcher(),
        handler=CallHandler(),
    ))
    value_reg.register(ActionNode(
        name="raise", priority=9,
        matcher=RaiseMatcher(),
        handler=RaiseHandler(),
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
        handler=ForeachHandler(),
    ))
    main_reg.register(ActionNode(
        name="while", priority=10,
        matcher=OpMatcher("while"),
        handler=WhileHandler(),
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
    )
