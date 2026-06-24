"""Rendering primitives shared by the SQL construct handlers.

A *fragment* is the unit every construct produces::

    {"sql": "<sql text>", "params": [<bound values>]}

Values are always bound as parameters (never interpolated), so the rendered
``sql`` only ever contains the neutral ``?`` placeholder for data.

Recursion happens through the isolated, named SQL pipeline: :func:`render`
dispatches a node back through ``engine.run_pipeline`` so nested constructs
resolve via the same handlers.  The pipeline is invoked over the caller's
context as-is (j-perm's ``run_pipeline`` is passthrough), which keeps recursion
cheap — no per-node deep copy.
"""
from __future__ import annotations

from typing import Any

from j_perm.handlers.signals import ControlFlowSignal

from .dialect import PLACEHOLDER, RenderOptions

#: Name the read-only SQL value-pipeline is registered under on the engine.
SQL_PIPELINE_NAME = "sql"

#: Metadata key carrying the async value cache (``{id(node): resolved}``) on the
#: *async* render path.  Its presence is what switches :func:`resolve_value` from
#: resolving inline (sync) to the resolve-on-demand-with-restart protocol used by
#: :meth:`j_perm_sql.handler.SqlRenderer.render_async`.
ASYNC_VALUE_CACHE_KEY = "_sql_async_value_cache"

#: Metadata key carrying the name of the pipeline that recursion should dispatch
#: through.  Set by :class:`~j_perm_sql.handler.SqlRenderer` so a write
#: statement's sub-parts resolve in the write pipeline; defaults to the read
#: pipeline when absent.
ACTIVE_PIPELINE_KEY = "_sql_pipeline"

#: Metadata key carrying the per-node compilation cache (``{id(node): CompiledSpec}``)
#: for the *compiled* render path.  Present only when the top-level ``op: sql``
#: was reached through a compiled pipeline; absent for the plain interpreted path
#: (then :func:`render` dispatches through ``run_pipeline`` as before).
COMPILE_CACHE_KEY = "_sql_compile_cache"

#: Keys whose presence marks a node as a *query* (must be parenthesised when
#: used as an operand, subquery, or derived table).
_QUERY_KEYS = frozenset(
    {"$select", "$union", "$union_all", "$intersect", "$except", "$values"}
)


class _NeedAsyncValue(ControlFlowSignal):
    """Raised mid-render to ask the async driver to resolve a value node.

    Subclasses :class:`~j_perm.handlers.signals.ControlFlowSignal` so it
    propagates straight up through the (synchronous) SQL pipeline without being
    annotated or logged, and is caught only by
    :meth:`j_perm_sql.handler.SqlRenderer.render_async`.
    """

    def __init__(self, node: Any) -> None:
        self.node = node


def resolve_value(node: Any, ctx) -> Any:
    """Resolve an embedded engine value (``$val`` / ``$in`` / ``$values`` cell).

    * Sync render (no async cache in metadata) → resolve inline via
      ``process_value``.
    * Async render → consult the per-render cache keyed by ``id(node)``; on a
      miss, raise :class:`_NeedAsyncValue` so the async driver can
      ``await process_value_async`` and restart the render with the value cached.
    """
    cache = ctx.metadata.get(ASYNC_VALUE_CACHE_KEY)
    if cache is None:
        return ctx.engine.process_value(node, ctx)
    key = id(node)
    if key in cache:
        return cache[key]
    raise _NeedAsyncValue(node)


def fragment(sql: str, params: list | None = None) -> dict:
    """Build a SQL fragment dict."""
    return {"sql": sql, "params": list(params) if params else []}


def is_fragment(value: Any) -> bool:
    """True if *value* is a rendered SQL fragment."""
    return isinstance(value, dict) and "sql" in value and "params" in value


def is_query(node: Any) -> bool:
    """True if *node* is a query construct (``$select``/set-op/``$values``)."""
    return isinstance(node, dict) and any(k in node for k in _QUERY_KEYS)


def render(node: Any, ctx) -> Any:
    """Dispatch *node* through the active SQL pipeline and return the result.

    The active pipeline name is read from ``ctx.metadata`` (set by the renderer)
    so recursion stays within the same pipeline the top-level operation chose;
    it defaults to the read-only :data:`SQL_PIPELINE_NAME`.

    When a compilation cache is active (the top-level ``op: sql`` was compiled),
    each node is compiled once against the SQL pipeline and the resulting
    :class:`~j_perm.core.CompiledSpec` is memoised on the cache, so subsequent
    runs of the same compiled query skip stage processing and handler resolution
    for every node.  The cache lives on the top-level compiled query spec, so it
    is bounded by that query's nodes and freed together with it.
    """
    name = ctx.metadata.get(ACTIVE_PIPELINE_KEY, SQL_PIPELINE_NAME)
    cache = ctx.metadata.get(COMPILE_CACHE_KEY)
    if cache is None:
        return ctx.engine.run_pipeline(name, node, ctx).dest
    pipeline = ctx.engine.get_pipeline(name)
    compiled = cache.get(id(node))
    if compiled is None:
        compiled = pipeline.compile(node, ctx)
        cache[id(node)] = compiled
    pipeline.run_compiled(compiled, ctx)
    return ctx.dest


def render_construct(node: Any, ctx) -> dict:
    """Render *node*, asserting it produced a fragment."""
    result = render(node, ctx)
    if not is_fragment(result):
        raise ValueError(f"expected a SQL construct, got {node!r}")
    return result


def render_subquery(node: Any, ctx) -> dict:
    """Render a query construct and wrap it in parentheses."""
    frag = render_construct(node, ctx)
    return fragment(f"({frag['sql']})", frag["params"])


def render_operand(node: Any, ctx, opts: RenderOptions) -> dict:
    """Render a value/expression operand.

    * ``str`` → a (possibly dotted, possibly ``*``) column reference.
    * query construct → a parenthesised scalar subquery.
    * other construct dict → rendered fragment.
    * anything else → error (use ``$val`` to bind a data value).
    """
    if isinstance(node, str):
        return fragment(opts.quote_ref(node))
    if isinstance(node, dict):
        if is_query(node):
            return render_subquery(node, ctx)
        return render_construct(node, ctx)
    raise TypeError(f"invalid SQL operand: {node!r}; use {{'$val': ...}} for data values")


def join_fragments(frags: list[dict], sep: str) -> dict:
    """Join rendered fragments' SQL with *sep*, concatenating their params."""
    sql = sep.join(f["sql"] for f in frags)
    params = [p for f in frags for p in f["params"]]
    return fragment(sql, params)


def render_operands(items: list, ctx, opts: RenderOptions, sep: str = ", ") -> dict:
    """Render a list of operands and join them with *sep*."""
    return join_fragments([render_operand(it, ctx, opts) for it in items], sep)


def bind_value(value: Any) -> dict:
    """Bind a concrete Python value as a single placeholder."""
    return fragment(PLACEHOLDER, [value])
