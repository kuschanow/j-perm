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
from .pipeline import (
    SQL_PIPELINE_NAME,
    SQL_WRITE_PIPELINE_NAME,
    build_sql_pipeline,
    build_sql_write_pipeline,
)

__all__ = ["install_sql", "install_sql_write"]


def _register_op(engine, executor, renderer, op):
    """Register an ``op`` handler, choosing sync/async by the executor."""
    if asyncio.iscoroutinefunction(executor):
        handler = AsyncSqlHandler(executor, renderer)
    else:
        handler = SqlHandler(executor, renderer)
    engine.main_pipeline.registry.register(
        ActionNode(name=op, priority=10, matcher=OpMatcher(op), handler=handler)
    )


def install_sql(engine, executor, *, paramstyle: str = "qmark", dialect=None, op: str = "sql",
                text_syntax: bool = True):
    """Install read-only SQL (``SELECT``) support into *engine*.

    Args:
        engine:    a built ``j_perm`` engine (e.g. from ``build_default_engine``).
        executor:  ``executor(sql, params) -> result``.  If it is a coroutine
                   function, the async ``op: sql`` handler is registered (use
                   with ``engine.apply_async``); otherwise the sync handler.
        paramstyle: placeholder style when *dialect* is not given
                    (``qmark`` | ``format`` | ``numeric`` | ``named``).
        dialect:   an explicit :class:`RenderOptions`; overrides *paramstyle*.
        op:        the operation name to register (default ``"sql"``).
        text_syntax: when ``True`` (default), also register the ``sql{ … }``
                   text-syntax stage so ``SELECT`` queries can be written as text
                   in a spec.  Set ``False`` to register only the op handler.

    Returns:
        The same *engine*, for chaining.
    """
    opts = dialect if dialect is not None else RenderOptions(paramstyle=paramstyle)
    engine.register_pipeline(SQL_PIPELINE_NAME, build_sql_pipeline(opts))
    _register_op(engine, executor, SqlRenderer(opts, SQL_PIPELINE_NAME), op)
    if text_syntax:
        from .text import register_sql_text_stage
        register_sql_text_stage(engine, tag="sql", op=op, read_only=True)
    return engine


def install_sql_write(
    engine, executor, *, paramstyle: str = "qmark", dialect=None, op: str = "sql_write",
    text_syntax: bool = True,
):
    """Install write (DML) SQL support — ``INSERT`` / ``UPDATE`` / ``DELETE``.

    This is a separate, opt-in install: it registers an isolated write pipeline
    (read constructs + DML) and a distinct ``op`` so that ``op: sql`` — if also
    installed — stays guaranteed read-only.  Independent of :func:`install_sql`
    (either may be installed alone, in any order).

    Args:
        engine:    a built ``j_perm`` engine.
        executor:  ``executor(sql, params) -> result`` (sync or coroutine).
        paramstyle: placeholder style when *dialect* is not given.
        dialect:   an explicit :class:`RenderOptions`; overrides *paramstyle*.
        op:        the operation name to register (default ``"sql_write"``).

    Returns:
        The same *engine*, for chaining.
    """
    opts = dialect if dialect is not None else RenderOptions(paramstyle=paramstyle)
    engine.register_pipeline(SQL_WRITE_PIPELINE_NAME, build_sql_write_pipeline(opts))
    _register_op(engine, executor, SqlRenderer(opts, SQL_WRITE_PIPELINE_NAME), op)
    if text_syntax:
        from .text import register_sql_text_stage
        register_sql_text_stage(engine, tag="sql_write", op=op, read_only=False)
    return engine
