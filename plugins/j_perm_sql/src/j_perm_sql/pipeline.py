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
from .constructs_write import build_sql_write_specials
from .dialect import RenderOptions
from .render import SQL_PIPELINE_NAME

#: Name the write (DML) SQL pipeline is registered under on the engine.
SQL_WRITE_PIPELINE_NAME = "sql_write"

__all__ = [
    "build_sql_pipeline",
    "build_sql_write_pipeline",
    "SQL_PIPELINE_NAME",
    "SQL_WRITE_PIPELINE_NAME",
]


def _build_pipeline(specials: dict) -> Pipeline:
    """Assemble an isolated SQL pipeline from a ``{key: handler}`` mapping."""
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


def build_sql_pipeline(opts: RenderOptions | None = None) -> Pipeline:
    """Build the isolated read-only SQL pipeline for the given dialect options."""
    opts = opts if opts is not None else RenderOptions()
    return _build_pipeline(build_sql_specials(opts))


def build_sql_write_pipeline(opts: RenderOptions | None = None) -> Pipeline:
    """Build the isolated write (DML) SQL pipeline (read constructs + DML)."""
    opts = opts if opts is not None else RenderOptions()
    return _build_pipeline(build_sql_write_specials(opts))
