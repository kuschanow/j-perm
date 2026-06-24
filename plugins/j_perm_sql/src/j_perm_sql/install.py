"""``install_sql`` — patch an existing engine with SQL support.

Rather than building a new engine, this registers the isolated SQL pipeline
and adds the single ``op: sql`` top-level operation to an already-built
``Engine``.  It composes with any engine (and any other plugins).
"""
from __future__ import annotations

import asyncio

from j_perm import ActionNode, OpMatcher

from .dialect import RenderOptions
from .handler import AsyncSqlHandler, SqlHandler, SqlRenderer
from .pipeline import build_sql_pipeline
from .render import SQL_PIPELINE_NAME

__all__ = ["install_sql"]


def install_sql(engine, executor, *, paramstyle: str = "qmark", dialect=None, op: str = "sql"):
    """Install SQL support into *engine*.

    Args:
        engine:    a built ``j_perm`` engine (e.g. from ``build_default_engine``).
        executor:  ``executor(sql, params) -> result``.  If it is a coroutine
                   function, the async ``op: sql`` handler is registered (use
                   with ``engine.apply_async``); otherwise the sync handler.
        paramstyle: placeholder style when *dialect* is not given
                    (``qmark`` | ``format`` | ``numeric`` | ``named``).
        dialect:   an explicit :class:`RenderOptions`; overrides *paramstyle*.
        op:        the operation name to register (default ``"sql"``).

    Returns:
        The same *engine*, for chaining.
    """
    opts = dialect if dialect is not None else RenderOptions(paramstyle=paramstyle)
    engine.register_pipeline(SQL_PIPELINE_NAME, build_sql_pipeline(opts))
    renderer = SqlRenderer(opts)
    if asyncio.iscoroutinefunction(executor):
        handler = AsyncSqlHandler(executor, renderer)
    else:
        handler = SqlHandler(executor, renderer)
    engine.main_pipeline.registry.register(
        ActionNode(name=op, priority=10, matcher=OpMatcher(op), handler=handler)
    )
    return engine
