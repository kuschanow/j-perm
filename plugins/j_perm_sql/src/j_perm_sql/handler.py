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
from j_perm.core import Compound

from .dialect import RenderOptions
from .render import (
    ACTIVE_PIPELINE_KEY,
    COMPILE_CACHE_KEY,
    SQL_PIPELINE_NAME,
    is_fragment,
)

#: Attribute under which a compiled query spec carries its per-node compilation
#: cache (see :func:`j_perm_sql.render.render`).  Stored on the spec — not in the
#: context — so it survives across runs of the same compiled query.
_NODE_CACHE_ATTR = "_sql_node_cache"


class SqlRenderer:
    """Render a SQL construct tree to ``(sql, params)`` for a target dialect.

    *pipeline_name* selects which isolated pipeline the tree (and all its
    recursion) is dispatched through — the read-only ``"sql"`` pipeline by
    default, or the write pipeline for ``op: sql_write``.
    """

    def __init__(self, opts: RenderOptions, pipeline_name: str = SQL_PIPELINE_NAME) -> None:
        self.opts = opts
        self.pipeline_name = pipeline_name

    def _render_ctx(self, ctx, extra_metadata: dict | None = None):
        # Render against a scratch dest so the real document is never clobbered,
        # but expose the real dest under _real_dest so @: pointers inside $val
        # can still read the document being built.  The active pipeline name is
        # threaded through metadata so recursion stays in this pipeline.
        metadata = {
            **ctx.metadata,
            "_real_dest": ctx.dest,
            ACTIVE_PIPELINE_KEY: self.pipeline_name,
        }
        if extra_metadata:
            metadata.update(extra_metadata)
        return ctx.copy(new_dest={}, new_metadata=metadata)

    def _finalize(self, frag) -> tuple:
        if not is_fragment(frag):
            raise ValueError("top-level SQL query must be a SQL construct")
        return self.opts.finalize(frag["sql"], frag["params"])

    def render(self, query, ctx) -> tuple:
        """Render *query* through the interpreted SQL pipeline."""
        render_ctx = self._render_ctx(ctx)
        frag = ctx.engine.run_pipeline(self.pipeline_name, query, render_ctx).dest
        return self._finalize(frag)

    def render_compiled(self, compiled_query, ctx) -> tuple:
        """Render a pre-compiled query spec, reusing per-node compilation.

        *compiled_query* is the :class:`~j_perm.core.CompiledSpec` produced for
        the ``query`` key at compile time (its top node is already resolved).
        Nested nodes are compiled lazily on first run and memoised on the spec,
        so repeated executions of the same compiled query stay fully compiled.
        """
        cache = getattr(compiled_query, _NODE_CACHE_ATTR, None)
        if cache is None:
            cache = {}
            setattr(compiled_query, _NODE_CACHE_ATTR, cache)
        render_ctx = self._render_ctx(ctx, {COMPILE_CACHE_KEY: cache})
        ctx.engine.get_pipeline(self.pipeline_name).run_compiled(compiled_query, render_ctx)
        return self._finalize(render_ctx.dest)


class _SqlCompound(Compound):
    """Mixin marking the ``op: sql`` handlers as compilable.

    The ``query`` subtree is compiled against the handler's isolated SQL
    pipeline (read or write) rather than the main pipeline, so the engine never
    needs to know the SQL constructs.
    """

    _renderer: SqlRenderer

    def nested_spec_keys(self, step) -> list[str]:
        return ["query"] if "query" in step else []

    def nested_spec_pipeline(self, step, key) -> str:
        return self._renderer.pipeline_name


def _write_result(step, ctx, result):
    if "to" in step:
        path = ctx.engine.process_value(step["to"], ctx)
        ctx.engine.processor.set(path, ctx, result)
    return ctx.dest


class SqlHandler(ActionHandler, _SqlCompound):
    """``op: sql`` with a synchronous executor ``executor(sql, params) -> result``."""

    def __init__(self, executor, renderer: SqlRenderer) -> None:
        self._executor = executor
        self._renderer = renderer

    def execute(self, step, ctx):
        sql, params = self._renderer.render(step["query"], ctx)
        result = self._executor(sql, params)
        return _write_result(step, ctx, result)

    def execute_compiled(self, step, ctx, nested):
        sql, params = self._renderer.render_compiled(nested["query"], ctx)
        result = self._executor(sql, params)
        return _write_result(step, ctx, result)


class AsyncSqlHandler(AsyncActionHandler, _SqlCompound):
    """``op: sql`` with an async executor ``await executor(sql, params) -> result``."""

    def __init__(self, executor, renderer: SqlRenderer) -> None:
        self._executor = executor
        self._renderer = renderer

    async def execute(self, step, ctx):
        sql, params = self._renderer.render(step["query"], ctx)
        result = await self._executor(sql, params)
        return _write_result(step, ctx, result)

    async def execute_compiled(self, step, ctx, nested):
        sql, params = self._renderer.render_compiled(nested["query"], ctx)
        result = await self._executor(sql, params)
        return _write_result(step, ctx, result)
