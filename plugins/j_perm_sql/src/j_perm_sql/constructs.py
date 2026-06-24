"""SQL construct handlers — the full standard ``SELECT`` surface.

Each construct is a callable ``(node, ctx) -> fragment`` (built via
:func:`build_sql_specials`, closing over a :class:`RenderOptions`).  Constructs
recurse through the isolated SQL pipeline via the helpers in :mod:`.render`.

Values are always bound as parameters (``$val`` and the data sides of ``$in`` /
``$between`` / ``$values``); identifiers are validated and quoted.  Nothing
user-supplied is interpolated into the SQL string except validated identifiers,
function names, types, and a small set of whitelisted keywords.
"""
from __future__ import annotations

from functools import partial
from typing import Any

from .dialect import PLACEHOLDER, RenderOptions
from .render import (
    fragment,
    is_query,
    join_fragments,
    render_construct,
    render_operand,
    render_operands,
    render_subquery,
)

# ─────────────────────────────────────────────────────────────────────────────
# Whitelists (anything user-supplied that reaches the SQL string verbatim)
# ─────────────────────────────────────────────────────────────────────────────

_COMPARE_SYMBOLS = frozenset({"=", "<>", "!=", "<", "<=", ">", ">="})
_JOIN_KEYWORDS = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL JOIN",
    "cross": "CROSS JOIN",
}
_QUANTIFIERS = {"$any": "ANY", "$all": "ALL", "$some": "SOME"}
_SETOPS = {
    "$union": "UNION",
    "$union_all": "UNION ALL",
    "$intersect": "INTERSECT",
    "$except": "EXCEPT",
}


def _int_literal(value: Any, what: str) -> int:
    """Validate an integer literal (LIMIT/OFFSET/frame offsets)."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{what} must be an integer, got {value!r}")
    return value


# ─────────────────────────────────────────────────────────────────────────────
# Leaves: column reference and bound value
# ─────────────────────────────────────────────────────────────────────────────

def col(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$col"]
    if isinstance(spec, str):
        spec = {"name": spec}
    name = spec["name"]
    table = spec.get("table")
    if name == "*":
        core = (opts.quote_identifier(table) + "." if table else "") + "*"
    else:
        parts = ([table] if table else []) + [name]
        core = ".".join(opts.quote_identifier(p) for p in parts)
    if spec.get("as"):
        core += f" AS {opts.quote_identifier(spec['as'])}"
    return fragment(core)


def val(node, ctx, *, opts: RenderOptions) -> dict:
    value = ctx.engine.process_value(node["$val"], ctx)
    return fragment(PLACEHOLDER, [value])


# ─────────────────────────────────────────────────────────────────────────────
# Expressions: functions, cast, case, concat, arithmetic
# ─────────────────────────────────────────────────────────────────────────────

def func(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node.get("$func", node.get("$call"))
    name = opts.validate_func_name(spec["name"])
    arg_sqls: list[str] = []
    params: list = []
    for arg in spec.get("args", []):
        if arg == "*":
            arg_sqls.append("*")
        else:
            frag = render_operand(arg, ctx, opts)
            arg_sqls.append(frag["sql"])
            params += frag["params"]
    distinct = "DISTINCT " if spec.get("distinct") else ""
    sql = f"{name}({distinct}{', '.join(arg_sqls)})"
    if "over" in spec:
        win = _render_window(spec["over"], ctx, opts)
        sql += f" OVER ({win['sql']})"
        params += win["params"]
    if spec.get("as"):
        sql += f" AS {opts.quote_identifier(spec['as'])}"
    return fragment(sql, params)


def _render_window(over, ctx, opts: RenderOptions) -> dict:
    parts: list[str] = []
    params: list = []
    if over.get("partition_by"):
        pf = render_operands(over["partition_by"], ctx, opts)
        parts.append(f"PARTITION BY {pf['sql']}")
        params += pf["params"]
    if over.get("order_by"):
        of = _render_order_by(over["order_by"], ctx, opts)
        parts.append(f"ORDER BY {of['sql']}")
        params += of["params"]
    if "frame" in over:
        parts.append(_render_frame(over["frame"]))
    return fragment(" ".join(parts), params)


def _render_frame(frame) -> str:
    ftype = frame["type"].upper()
    if ftype not in ("ROWS", "RANGE"):
        raise ValueError(f"frame type must be ROWS or RANGE, got {frame['type']!r}")
    start = _frame_bound(frame["start"])
    if "end" in frame:
        return f"{ftype} BETWEEN {start} AND {_frame_bound(frame['end'])}"
    return f"{ftype} {start}"


def _frame_bound(bound) -> str:
    if isinstance(bound, str):
        norm = bound.strip().lower()
        if norm in ("unbounded preceding", "unbounded following", "current row"):
            return norm.upper()
        raise ValueError(f"invalid frame bound: {bound!r}")
    if isinstance(bound, dict):
        if "preceding" in bound:
            return f"{_int_literal(bound['preceding'], 'frame offset')} PRECEDING"
        if "following" in bound:
            return f"{_int_literal(bound['following'], 'frame offset')} FOLLOWING"
    raise ValueError(f"invalid frame bound: {bound!r}")


def cast(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$cast"]
    expr = render_operand(spec["expr"], ctx, opts)
    type_str = opts.validate_type(spec["type"])
    sql = f"CAST({expr['sql']} AS {type_str})"
    if spec.get("as"):
        sql += f" AS {opts.quote_identifier(spec['as'])}"
    return fragment(sql, expr["params"])


def case(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$case"]
    parts = ["CASE"]
    params: list = []
    for branch in spec["whens"]:
        cond = render_construct(branch["when"], ctx)
        result = render_operand(branch["then"], ctx, opts)
        parts.append(f"WHEN {cond['sql']} THEN {result['sql']}")
        params += cond["params"] + result["params"]
    if "else" in spec:
        els = render_operand(spec["else"], ctx, opts)
        parts.append(f"ELSE {els['sql']}")
        params += els["params"]
    parts.append("END")
    sql = " ".join(parts)
    if spec.get("as"):
        sql += f" AS {opts.quote_identifier(spec['as'])}"
    return fragment(sql, params)


def concat(node, ctx, *, opts: RenderOptions) -> dict:
    frag = render_operands(node["$concat"], ctx, opts, sep=f" {opts.concat_operator} ")
    return fragment(f"({frag['sql']})", frag["params"])


def _arith(node, ctx, *, opts: RenderOptions, key: str, symbol: str) -> dict:
    frag = render_operands(node[key], ctx, opts, sep=f" {symbol} ")
    return fragment(f"({frag['sql']})", frag["params"])


# ─────────────────────────────────────────────────────────────────────────────
# Predicates
# ─────────────────────────────────────────────────────────────────────────────

def _boolean(node, ctx, *, opts: RenderOptions, key: str, word: str) -> dict:
    preds = node[key]
    if not preds:
        raise ValueError(f"{key} requires at least one predicate")
    frags = [render_construct(p, ctx) for p in preds]
    joined = f" {word} ".join(f["sql"] for f in frags)
    params = [p for f in frags for p in f["params"]]
    return fragment(f"({joined})", params)


def not_(node, ctx, *, opts: RenderOptions) -> dict:
    inner = render_construct(node["$not"], ctx)
    return fragment(f"NOT ({inner['sql']})", inner["params"])


def _cmp(node, ctx, *, opts: RenderOptions, key: str, symbol: str) -> dict:
    left, right = node[key]
    lf = render_operand(left, ctx, opts)
    rf = render_operand(right, ctx, opts)
    return fragment(f"{lf['sql']} {symbol} {rf['sql']}", lf["params"] + rf["params"])


def _in(node, ctx, *, opts: RenderOptions, negate: bool) -> dict:
    key = "$not_in" if negate else "$in"
    left, right = node[key]
    lf = render_operand(left, ctx, opts)
    kw = "NOT IN" if negate else "IN"
    if is_query(right):
        sub = render_subquery(right, ctx)
        return fragment(f"{lf['sql']} {kw} {sub['sql']}", lf["params"] + sub["params"])
    values = ctx.engine.process_value(right, ctx)
    if not isinstance(values, (list, tuple)):
        values = [values]
    if not values:
        raise ValueError(f"{key} requires a non-empty value list")
    placeholders = ", ".join(PLACEHOLDER for _ in values)
    return fragment(f"{lf['sql']} {kw} ({placeholders})", lf["params"] + list(values))


def _between(node, ctx, *, opts: RenderOptions, negate: bool) -> dict:
    key = "$not_between" if negate else "$between"
    expr, low, high = node[key]
    ef = render_operand(expr, ctx, opts)
    lf = render_operand(low, ctx, opts)
    hf = render_operand(high, ctx, opts)
    kw = "NOT BETWEEN" if negate else "BETWEEN"
    return fragment(
        f"{ef['sql']} {kw} {lf['sql']} AND {hf['sql']}",
        ef["params"] + lf["params"] + hf["params"],
    )


def _like(node, ctx, *, opts: RenderOptions, negate: bool) -> dict:
    key = "$not_like" if negate else "$like"
    expr, pattern = node[key]
    ef = render_operand(expr, ctx, opts)
    pf = render_operand(pattern, ctx, opts)
    kw = "NOT LIKE" if negate else "LIKE"
    sql = f"{ef['sql']} {kw} {pf['sql']}"
    params = ef["params"] + pf["params"]
    if "escape" in node:
        esc = render_operand(node["escape"], ctx, opts)
        sql += f" ESCAPE {esc['sql']}"
        params += esc["params"]
    return fragment(sql, params)


def _is_null(node, ctx, *, opts: RenderOptions, negate: bool) -> dict:
    key = "$is_not_null" if negate else "$is_null"
    ef = render_operand(node[key], ctx, opts)
    kw = "IS NOT NULL" if negate else "IS NULL"
    return fragment(f"{ef['sql']} {kw}", ef["params"])


def _exists(node, ctx, *, opts: RenderOptions, negate: bool) -> dict:
    key = "$not_exists" if negate else "$exists"
    sub = render_subquery(node[key], ctx)
    kw = "NOT EXISTS" if negate else "EXISTS"
    return fragment(f"{kw} {sub['sql']}", sub["params"])


def _quantified(node, ctx, *, opts: RenderOptions, key: str) -> dict:
    left, op, query = node[key]
    if op not in _COMPARE_SYMBOLS:
        raise ValueError(f"invalid comparison operator: {op!r}")
    lf = render_operand(left, ctx, opts)
    sub = render_subquery(query, ctx)
    return fragment(
        f"{lf['sql']} {op} {_QUANTIFIERS[key]} {sub['sql']}",
        lf["params"] + sub["params"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Clause helpers (not registered constructs — they only appear inside $select)
# ─────────────────────────────────────────────────────────────────────────────

def _render_order_by(items, ctx, opts: RenderOptions) -> dict:
    rendered: list[str] = []
    params: list = []
    for item in items:
        if isinstance(item, dict) and "expr" in item:
            ef = render_operand(item["expr"], ctx, opts)
            sql = ef["sql"]
            params += ef["params"]
            direction = item.get("dir")
            if direction is not None:
                if direction.lower() not in ("asc", "desc"):
                    raise ValueError(f"order_by dir must be asc/desc, got {direction!r}")
                sql += f" {direction.upper()}"
            nulls = item.get("nulls")
            if nulls is not None:
                if nulls.lower() not in ("first", "last"):
                    raise ValueError(f"order_by nulls must be first/last, got {nulls!r}")
                sql += f" NULLS {nulls.upper()}"
            rendered.append(sql)
        else:
            frag = render_operand(item, ctx, opts)
            rendered.append(frag["sql"])
            params += frag["params"]
    return fragment(", ".join(rendered), params)


def _render_group_by(spec, ctx, opts: RenderOptions) -> dict:
    if isinstance(spec, dict):
        if "$rollup" in spec:
            frag = render_operands(spec["$rollup"], ctx, opts)
            return fragment(f"ROLLUP ({frag['sql']})", frag["params"])
        if "$cube" in spec:
            frag = render_operands(spec["$cube"], ctx, opts)
            return fragment(f"CUBE ({frag['sql']})", frag["params"])
        if "$grouping_sets" in spec:
            rendered: list[str] = []
            params: list = []
            for group in spec["$grouping_sets"]:
                gf = render_operands(group, ctx, opts)
                rendered.append(f"({gf['sql']})")
                params += gf["params"]
            return fragment(f"GROUPING SETS ({', '.join(rendered)})", params)
        raise ValueError(f"invalid group_by spec: {spec!r}")
    return render_operands(spec, ctx, opts)


def _source_dict(spec) -> dict:
    """Extract the table-source portion from a $join spec."""
    return {
        "table": spec["table"],
        "as": spec.get("as"),
        "schema": spec.get("schema"),
        "lateral": spec.get("lateral"),
    }


def _render_table_source(node, ctx, opts: RenderOptions) -> dict:
    if isinstance(node, str):
        return fragment(opts.quote_ref(node))
    table = node["table"]
    if is_query(table):
        sub = render_subquery(table, ctx)
        alias = node.get("as")
        if not alias:
            raise ValueError("derived table requires an alias ('as')")
        lateral = "LATERAL " if node.get("lateral") else ""
        return fragment(
            f"{lateral}{sub['sql']} AS {opts.quote_identifier(alias)}", sub["params"]
        )
    parts = ([node["schema"]] if node.get("schema") else []) + [table]
    sql = ".".join(opts.quote_identifier(p) for p in parts)
    if node.get("as"):
        sql += f" AS {opts.quote_identifier(node['as'])}"
    return fragment(sql)


def join(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$join"]
    jtype = spec.get("type", "inner").lower()
    if jtype not in _JOIN_KEYWORDS:
        raise ValueError(f"invalid join type: {spec.get('type')!r}")
    natural = "NATURAL " if spec.get("natural") else ""
    source = _render_table_source(_source_dict(spec), ctx, opts)
    sql = f"{natural}{_JOIN_KEYWORDS[jtype]} {source['sql']}"
    params = list(source["params"])
    if "on" in spec:
        cond = render_construct(spec["on"], ctx)
        sql += f" ON {cond['sql']}"
        params += cond["params"]
    elif "using" in spec:
        cols = ", ".join(opts.quote_identifier(c) for c in spec["using"])
        sql += f" USING ({cols})"
    return fragment(sql, params)


def _render_with(entries, ctx, opts: RenderOptions) -> dict:
    recursive = any(e.get("recursive") for e in entries)
    rendered: list[str] = []
    params: list = []
    for entry in entries:
        name = opts.quote_identifier(entry["name"])
        columns = ""
        if entry.get("columns"):
            cols = ", ".join(opts.quote_identifier(c) for c in entry["columns"])
            columns = f" ({cols})"
        sub = render_construct(entry["query"], ctx)
        rendered.append(f"{name}{columns} AS ({sub['sql']})")
        params += sub["params"]
    keyword = "WITH RECURSIVE " if recursive else "WITH "
    return fragment(keyword + ", ".join(rendered), params)


def _render_pagination(spec, opts: RenderOptions) -> str:
    limit = spec.get("limit")
    offset = spec.get("offset")
    fetch = spec.get("fetch")
    if limit is not None:
        limit = _int_literal(limit, "limit")
    if offset is not None:
        offset = _int_literal(offset, "offset")
    if fetch is not None:
        fetch = _int_literal(fetch, "fetch")
    out: list[str] = []
    if opts.pagination == "limit":
        count = limit if limit is not None else fetch
        if count is not None:
            out.append(f"LIMIT {count}")
        if offset is not None:
            out.append(f"OFFSET {offset}")
    else:  # fetch
        if offset is not None:
            out.append(f"OFFSET {offset} ROWS")
        count = fetch if fetch is not None else limit
        if count is not None:
            out.append(f"FETCH FIRST {count} ROWS ONLY")
    return " ".join(out)


def _render_select_list(columns, ctx, opts: RenderOptions) -> dict:
    if not columns:
        return fragment("*")
    return join_fragments(
        [_render_projection_item(c, ctx, opts) for c in columns], ", "
    )


def _render_projection_item(item, ctx, opts: RenderOptions) -> dict:
    if isinstance(item, dict) and "expr" in item:
        ef = render_operand(item["expr"], ctx, opts)
        sql = ef["sql"]
        if item.get("as"):
            sql += f" AS {opts.quote_identifier(item['as'])}"
        return fragment(sql, ef["params"])
    return render_operand(item, ctx, opts)


# ─────────────────────────────────────────────────────────────────────────────
# Top-level query constructs: $select, set operations, $values
# ─────────────────────────────────────────────────────────────────────────────

def select(node, ctx, *, opts: RenderOptions) -> dict:
    spec = node["$select"]
    clauses: list[str] = []
    params: list = []

    if "with" in spec:
        w = _render_with(spec["with"], ctx, opts)
        clauses.append(w["sql"])
        params += w["params"]

    head = "SELECT DISTINCT" if spec.get("distinct") else "SELECT"
    cols = _render_select_list(spec.get("columns"), ctx, opts)
    clauses.append(f"{head} {cols['sql']}")
    params += cols["params"]

    if "from" in spec:
        frm = _render_table_source(spec["from"], ctx, opts)
        clauses.append(f"FROM {frm['sql']}")
        params += frm["params"]

    for joinspec in spec.get("joins", []):
        jf = render_construct(joinspec, ctx)
        clauses.append(jf["sql"])
        params += jf["params"]

    if "where" in spec:
        wf = render_construct(spec["where"], ctx)
        clauses.append(f"WHERE {wf['sql']}")
        params += wf["params"]

    if "group_by" in spec:
        gf = _render_group_by(spec["group_by"], ctx, opts)
        clauses.append(f"GROUP BY {gf['sql']}")
        params += gf["params"]

    if "having" in spec:
        hf = render_construct(spec["having"], ctx)
        clauses.append(f"HAVING {hf['sql']}")
        params += hf["params"]

    if "order_by" in spec:
        of = _render_order_by(spec["order_by"], ctx, opts)
        clauses.append(f"ORDER BY {of['sql']}")
        params += of["params"]

    pagination = _render_pagination(spec, opts)
    if pagination:
        clauses.append(pagination)

    return fragment(" ".join(clauses), params)


def _setop(node, ctx, *, opts: RenderOptions, key: str) -> dict:
    queries = node[key]
    if not queries:
        raise ValueError(f"{key} requires at least one query")
    frags = [render_construct(q, ctx) for q in queries]
    sql = f" {_SETOPS[key]} ".join(f["sql"] for f in frags)
    params = [p for f in frags for p in f["params"]]
    if "order_by" in node:
        of = _render_order_by(node["order_by"], ctx, opts)
        sql += f" ORDER BY {of['sql']}"
        params += of["params"]
    pagination = _render_pagination(node, opts)
    if pagination:
        sql += f" {pagination}"
    return fragment(sql, params)


def values(node, ctx, *, opts: RenderOptions) -> dict:
    rows = node["$values"]
    if not rows:
        raise ValueError("$values requires at least one row")
    rendered_rows: list[str] = []
    params: list = []
    width: int | None = None
    for row in rows:
        if width is None:
            width = len(row)
        elif len(row) != width:
            raise ValueError("all $values rows must have the same length")
        rendered_rows.append("(" + ", ".join(PLACEHOLDER for _ in row) + ")")
        for cell in row:
            params.append(ctx.engine.process_value(cell, ctx))
    return fragment("VALUES " + ", ".join(rendered_rows), params)


# ─────────────────────────────────────────────────────────────────────────────
# Registry assembly
# ─────────────────────────────────────────────────────────────────────────────

def build_sql_specials(opts: RenderOptions) -> dict:
    """Build the ``{key: handler}`` mapping for the SQL pipeline.

    Every handler is bound to *opts* so the SQL pipeline renders for one
    target dialect.
    """
    specials: dict = {
        "$select": select,
        "$col": col,
        "$val": val,
        "$join": join,
        "$func": func,
        "$call": func,
        "$cast": cast,
        "$case": case,
        "$concat": concat,
        "$not": not_,
        "$values": values,
    }
    # comparisons
    for key, symbol in {
        "$eq": "=", "$ne": "<>", "$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<=",
    }.items():
        specials[key] = partial(_cmp, key=key, symbol=symbol)
    # arithmetic
    for key, symbol in {
        "$add": "+", "$sub": "-", "$mul": "*", "$div": "/", "$mod": "%",
    }.items():
        specials[key] = partial(_arith, key=key, symbol=symbol)
    # boolean connectives
    specials["$and"] = partial(_boolean, key="$and", word="AND")
    specials["$or"] = partial(_boolean, key="$or", word="OR")
    # in / between / like / null / exists (+ negations)
    specials["$in"] = partial(_in, negate=False)
    specials["$not_in"] = partial(_in, negate=True)
    specials["$between"] = partial(_between, negate=False)
    specials["$not_between"] = partial(_between, negate=True)
    specials["$like"] = partial(_like, negate=False)
    specials["$not_like"] = partial(_like, negate=True)
    specials["$is_null"] = partial(_is_null, negate=False)
    specials["$is_not_null"] = partial(_is_null, negate=True)
    specials["$exists"] = partial(_exists, negate=False)
    specials["$not_exists"] = partial(_exists, negate=True)
    # quantified comparisons
    for key in _QUANTIFIERS:
        specials[key] = partial(_quantified, key=key)
    # set operations
    for key in _SETOPS:
        specials[key] = partial(_setop, key=key)

    # bind opts into every handler
    return {key: partial(fn, opts=opts) for key, fn in specials.items()}
