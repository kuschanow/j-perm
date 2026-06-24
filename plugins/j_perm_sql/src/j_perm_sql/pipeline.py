"""Assembly of the isolated, named SQL value-pipeline.

The SQL constructs live **only** in this pipeline's ``SpecialMatcher`` — they are
never registered in the engine's default ``value_pipeline``.  So ``{"$select": …}``
outside of the ``sql`` operation is just a plain dict, not a construct.
"""
from __future__ import annotations

from j_perm import (
    ActionNode,
    ActionTypeRegistry,
    AlwaysMatcher,
    IdentityHandler,
    Pipeline,
    SpecialMatcher,
    SpecialResolveHandler,
)

from .constructs import build_sql_specials
from .dialect import RenderOptions
from .render import SQL_PIPELINE_NAME

__all__ = ["build_sql_pipeline", "SQL_PIPELINE_NAME"]


def build_sql_pipeline(opts: RenderOptions | None = None) -> Pipeline:
    """Build the isolated SQL pipeline for the given dialect options."""
    opts = opts if opts is not None else RenderOptions()
    specials = build_sql_specials(opts)
    registry = ActionTypeRegistry()
    registry.register(
        ActionNode(
            name="sql_special",
            priority=10,
            matcher=SpecialMatcher(set(specials)),
            handler=SpecialResolveHandler(specials),
        )
    )
    registry.register(
        ActionNode(
            name="sql_identity",
            priority=-999,
            matcher=AlwaysMatcher(),
            handler=IdentityHandler(),
        )
    )
    return Pipeline(registry=registry, track_execution=True)
