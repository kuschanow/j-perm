"""The single top-level ``op: sql`` operation (sync + async).

Schema::

    {"op": "sql", "query": <SQL construct tree>, "to": "/dest/path"}

* ``query`` — the SQL construct tree (rendered via the isolated SQL pipeline).
* ``to``    — optional destination pointer; the executor's result is written
              there (template-expanded).  If omitted, the result is discarded.

Rendering is identical for sync and async; only the executor call differs
(call vs ``await``).  A single class can't expose both a sync and an async
``execute`` (same method name), and ``Pipeline.run_async`` dispatches to the
async path only for :class:`AsyncActionHandler` — hence two classes over one
shared :class:`SqlRenderer`.
"""
from __future__ import annotations

from j_perm import ActionHandler, AsyncActionHandler

from .dialect import RenderOptions
from .render import SQL_PIPELINE_NAME, is_fragment


class SqlRenderer:
    """Render a SQL construct tree to ``(sql, params)`` for a target dialect."""

    def __init__(self, opts: RenderOptions) -> None:
        self.opts = opts

    def render(self, query, ctx) -> tuple:
        # Render against a scratch dest so the real document is never clobbered,
        # but expose the real dest under _real_dest so @: pointers inside $val
        # can still read the document being built.
        render_ctx = ctx.copy(
            new_dest={},
            new_metadata={**ctx.metadata, "_real_dest": ctx.dest},
        )
        frag = ctx.engine.run_pipeline(SQL_PIPELINE_NAME, query, render_ctx).dest
        if not is_fragment(frag):
            raise ValueError("top-level SQL query must be a SQL construct")
        return self.opts.finalize(frag["sql"], frag["params"])


def _write_result(step, ctx, result):
    if "to" in step:
        path = ctx.engine.process_value(step["to"], ctx)
        ctx.engine.processor.set(path, ctx, result)
    return ctx.dest


class SqlHandler(ActionHandler):
    """``op: sql`` with a synchronous executor ``executor(sql, params) -> result``."""

    def __init__(self, executor, renderer: SqlRenderer) -> None:
        self._executor = executor
        self._renderer = renderer

    def execute(self, step, ctx):
        sql, params = self._renderer.render(step["query"], ctx)
        result = self._executor(sql, params)
        return _write_result(step, ctx, result)


class AsyncSqlHandler(AsyncActionHandler):
    """``op: sql`` with an async executor ``await executor(sql, params) -> result``."""

    def __init__(self, executor, renderer: SqlRenderer) -> None:
        self._executor = executor
        self._renderer = renderer

    async def execute(self, step, ctx):
        sql, params = self._renderer.render(step["query"], ctx)
        result = await self._executor(sql, params)
        return _write_result(step, ctx, result)
