"""DML construct handlers — standard ``INSERT`` / ``UPDATE`` / ``DELETE``.

These build on the read-only constructs in :mod:`.constructs`: a write
statement's sub-parts (``WHERE`` predicates, ``SET`` values, ``INSERT … SELECT``
subqueries) are ordinary read constructs that recurse through the same pipeline.

Only the standard, broadly portable forms are supported.  Non-standard surface
(``RETURNING``, ``ON CONFLICT``/upsert, ``UPDATE … FROM``, ``DELETE … USING``)
is intentionally out of scope.

Safety:

* Data values are always bound as parameters (``$val`` / operands), never
  interpolated — exactly as for ``SELECT``.
* Table and column names are validated and quoted as identifiers.
* ``UPDATE`` / ``DELETE`` without a ``where`` raise unless an explicit
  ``"all": true`` flag is given, to guard against accidental full-table writes.
"""
from __future__ import annotations

from functools import partial

from .constructs import build_sql_specials
from .dialect import RenderOptions
from .render import (
    fragment,
    is_query,
    render_construct,
    render_operand,
    render_operands,
)


def _table_target(spec, opts: RenderOptions) -> str:
    """Quote a plain table target (``INSERT INTO`` / ``UPDATE`` / ``DELETE FROM``).

    Standard DML targets a single named table (optionally schema-qualified) — no
    alias and no subquery.  Accepts a string name or ``{"table", "schema"?}``.
    """
    if isinstance(spec, str):
        return opts.quote_ref(spec)
    if isinstance(spec, dict) and "table" in spec and not is_query(spec):
        parts = ([spec["schema"]] if spec.get("schema") else []) + [spec["table"]]
        return ".".join(opts.quote_identifier(p) for p in parts)
    raise ValueError(f"invalid table target: {spec!r}")


def _where_clause(spec, ctx) -> tuple[str, list]:
    """Render the optional ``WHERE`` of an UPDATE/DELETE, enforcing the guard.

    Returns ``(sql_suffix, params)``.  Without a ``where`` key an explicit
    ``"all": true`` is required, else :class:`ValueError`.
    """
    if "where" in spec:
        wf = render_construct(spec["where"], ctx)
        return f" WHERE {wf['sql']}", wf["params"]
    if spec.get("all") is True:
        return "", []
    raise ValueError(
        "UPDATE/DELETE without 'where' requires an explicit \"all\": true"
    )


def insert(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$insert"]
    sql = f"INSERT INTO {_table_target(spec['into'], opts)}"
    if spec.get("columns"):
        cols = ", ".join(opts.quote_identifier(c) for c in spec["columns"])
        sql += f" ({cols})"
    has_values = "values" in spec
    has_query = "query" in spec
    if has_values == has_query:
        raise ValueError("$insert requires exactly one of 'values' or 'query'")
    params: list = []
    if has_values:
        rows = spec["values"]
        if not rows:
            raise ValueError("$insert 'values' requires at least one row")
        rendered_rows: list[str] = []
        width: int | None = None
        for row in rows:
            if width is None:
                width = len(row)
            elif len(row) != width:
                raise ValueError("all $insert rows must have the same length")
            frag = render_operands(row, ctx, opts)
            rendered_rows.append(f"({frag['sql']})")
            params += frag["params"]
        sql += " VALUES " + ", ".join(rendered_rows)
    else:
        if not is_query(spec["query"]):
            raise ValueError("$insert 'query' must be a SELECT/set-op query construct")
        q = render_construct(spec["query"], ctx)
        sql += f" {q['sql']}"
        params += q["params"]
    return fragment(sql, params)


def _render_set(spec, ctx, opts: RenderOptions) -> dict:
    if not isinstance(spec, dict) or not spec:
        raise ValueError("$update 'set' must be a non-empty mapping of column -> value")
    assignments: list[str] = []
    params: list = []
    for col_name, value in spec.items():
        rhs = render_operand(value, ctx, opts)
        assignments.append(f"{opts.quote_identifier(col_name)} = {rhs['sql']}")
        params += rhs["params"]
    return fragment(", ".join(assignments), params)


def update(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$update"]
    set_frag = _render_set(spec["set"], ctx, opts)
    sql = f"UPDATE {_table_target(spec['table'], opts)} SET {set_frag['sql']}"
    params = list(set_frag["params"])
    where_sql, where_params = _where_clause(spec, ctx)
    return fragment(sql + where_sql, params + where_params)


def delete(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$delete"]
    sql = f"DELETE FROM {_table_target(spec['from'], opts)}"
    where_sql, where_params = _where_clause(spec, ctx)
    return fragment(sql + where_sql, list(where_params))


def build_sql_write_specials(opts: RenderOptions) -> dict:
    """Build the ``{key: handler}`` mapping for the write SQL pipeline.

    The write pipeline is a superset of the read pipeline: all ``SELECT``
    constructs plus the DML statements, so subqueries / predicates / ``SET``
    expressions resolve in the same pipeline.
    """
    specials = dict(build_sql_specials(opts))
    specials["$insert"] = partial(insert, opts=opts)
    specials["$update"] = partial(update, opts=opts)
    specials["$delete"] = partial(delete, opts=opts)
    return specials
