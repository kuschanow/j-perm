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

from .core import (
    ActionNode,
    ActionTypeRegistry,
    Engine,
    Pipeline,
    UnescapeRule,
)
from .handlers.constructs import ref_handler, eval_handler
from .handlers.container import ContainerMatcher, RecursiveDescentHandler
from .handlers.identity import IdentityHandler
from .handlers.ops import (
    SetHandler, CopyHandler, CopyDHandler,
    DeleteHandler,
    ForeachHandler, IfHandler, ExecHandler,
    UpdateHandler, DistinctHandler,
    ReplaceRootHandler,
    AssertHandler, AssertDHandler,
)
from .handlers.special import SpecialFn, SpecialMatcher, SpecialResolveHandler
from .handlers.template import TemplMatcher, TemplSubstHandler, template_unescape
from .matchers import AlwaysMatcher, OpMatcher
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
            - ``AssertShorthandProcessor``  (priority 100) — ``~assert`` / ``~assertD``
            - ``DeleteShorthandProcessor``  (priority  50) — ``~delete``
            - ``AssignShorthandProcessor``  (priority   0) — fallback ``/path``
        * Registry: all 12 built-in ops (set, copy, copyD, delete, foreach,
          if, exec, update, distinct, replace_root, assert, assertD).

    value_pipeline
        * ``SpecialResolveHandler``   (priority 10)  – ``$ref``, ``$eval``.
        * ``TemplSubstHandler``       (priority  8)  – ``${…}`` with built-in
          casters (int, float, bool, str) and JMESPath function (subtract).
        * ``RecursiveDescentHandler`` (priority  5)  – recurse into containers.
        * ``IdentityHandler``         (priority -999, catch-all).

    unescape_rules
        * ``template_unescape``       (priority  0)  – strips ``$${`` → ``${``
          and ``$$`` → ``$``.

    Args:
        specials:        Custom special-construct handlers.  ``None`` → uses
                         defaults (``{"$ref": ref_handler, "$eval": eval_handler}``).
        casters:         Custom template casters.  ``None`` → uses built-in
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

    # -- default specials ---------------------------------------------------
    if specials is None:
        specials = {
            "$ref": ref_handler,
            "$eval": eval_handler,
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
        name="copyD", priority=10,
        matcher=OpMatcher("copyD"),
        handler=CopyDHandler(set_handler),
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
        name="replace_root", priority=10,
        matcher=OpMatcher("replace_root"),
        handler=ReplaceRootHandler(),
    ))
    main_reg.register(ActionNode(
        name="assert", priority=10,
        matcher=OpMatcher("assert"),
        handler=AssertHandler(),
    ))
    main_reg.register(ActionNode(
        name="assertD", priority=10,
        matcher=OpMatcher("assertD"),
        handler=AssertDHandler(),
    ))

    main_pipeline = Pipeline(stages=main_stages, registry=main_reg)

    # -- unescape rules -----------------------------------------------------
    unescape_rules = [
        UnescapeRule(name="template", priority=0, unescape=template_unescape),
    ]

    # -- engine -------------------------------------------------------------
    return Engine(
        resolver=resolver,
        main_pipeline=main_pipeline,
        value_pipeline=value_pipeline,
        value_max_depth=value_max_depth,
        unescape_rules=unescape_rules,
    )
