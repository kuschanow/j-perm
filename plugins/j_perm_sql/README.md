# j-perm-sql

A [j-perm](https://github.com/kuschanow/j-perm) plugin that builds and executes
**SQL `SELECT` queries** from j-perm constructs.

* SQL is described with a tree of `$`-constructs (`$select`, `$col`, `$val`,
  predicates, joins, …).
* A single new top-level operation — `op: sql` — renders that tree to a
  **parameterized** `(sql, params)` pair and hands it to a configurable
  executor (any ORM's raw-execute function).
* The SQL constructs live in an **isolated** named pipeline: they mean nothing
  outside `op: sql`. `{"$select": …}` used as an ordinary value is just a dict.

> v1 scope: the full standard **`SELECT`** surface (read-only). DDL/DML
> (`CREATE`/`ALTER`/`INSERT`/`UPDATE`/`DELETE`) is intentionally out of scope.

## Install

```bash
pip install j-perm-sql
```

Requires `j-perm >= 1.9.0` (the version that made `run_pipeline` a passthrough
invoker, which this plugin relies on).

## Quick start

```python
from j_perm import build_default_engine
from j_perm_sql import install_sql

def run_sql(sql, params):
    # any ORM's raw execute: cursor.execute(sql, params); return rows
    ...

engine = build_default_engine()
install_sql(engine, run_sql, paramstyle="qmark")

engine.apply(
    {"op": "sql", "to": "/rows", "query": {"$select": {
        "columns": [{"$col": {"name": "id"}}, {"$col": {"name": "name"}}],
        "from": {"table": "users"},
        "where": {"$gte": [{"$col": {"name": "age"}}, {"$val": 18}]},
        "order_by": [{"expr": {"$col": {"name": "name"}}}],
        "limit": 50,
    }}},
    source={}, dest={},
)
# run_sql receives:  ('SELECT "id", "name" FROM "users" WHERE "age" >= ? ORDER BY "name" LIMIT 50', [18])
# result is written to dest at /rows
```

`install_sql` **patches an existing engine** — it registers the isolated SQL
pipeline and the `op: sql` operation. It composes with any engine and any other
plugins.

## The `op: sql` operation

```js
{"op": "sql", "query": <SQL construct tree>, "to": "/dest/path"}
```

* `query` — the SQL construct tree.
* `to` — optional destination pointer (template-expanded); the executor's
  result is written there. If omitted, the result is discarded.

## Parameterization & injection safety

Data values are **always bound as parameters**, never interpolated:

* `$val` (and the data sides of `$in`, `$between`, `$values`) emit a placeholder
  and add the value to `params`.
* Identifiers (table/column/alias names) are validated against a conservative
  charset and quoted.
* Function names, CAST types, join types, sort directions, etc. are validated
  against whitelists.

```python
{"$eq": [{"$col": {"name": "name"}}, {"$val": {"$ref": "/user_input"}}]}
# → '"name" = ?'   with the (possibly malicious) value safely in params
```

Inside `$val`, the value expression is resolved with j-perm's normal value
pipeline, so `$ref`, `${…}` templates, and `@:` dest-pointers all work.

## Dialect / `RenderOptions`

Everything that genuinely differs between databases is configurable:

```python
from j_perm_sql import RenderOptions

install_sql(engine, run_sql, dialect=RenderOptions(
    paramstyle="numeric",      # qmark (?) | format (%s) | numeric ($1) | named (:p1)
    identifier_quote='"',      # e.g. "`" for MySQL
    pagination="fetch",        # "limit" (LIMIT n OFFSET m) | "fetch" (OFFSET m ROWS FETCH FIRST n ROWS ONLY)
    concat_operator="||",      # "||" or "+"
))
```

## Sync & async

`install_sql` inspects the executor: a coroutine function registers the async
handler (use with `engine.apply_async`); a regular function registers the sync
handler (use with `engine.apply`).

```python
async def run_sql(sql, params): ...
install_sql(engine, run_sql)                  # async
await engine.apply_async(spec, source=…, dest=…)
```

## Construct reference

**Query**

| Construct | Form |
|---|---|
| `$select` | `{with?, distinct?, columns?, from?, joins?, where?, group_by?, having?, order_by?, limit?/offset? \| fetch?}` |
| `$union` / `$union_all` / `$intersect` / `$except` | `{"$union": [q1, q2, …], order_by?, limit?…}` |
| `$values` | `{"$values": [[…row…], …]}` (table source or `IN`) |

**Projection / expressions**

| Construct | Renders |
|---|---|
| `$col` | `"t"."name" [AS "alias"]`; `"id"`; `*`; `"t".*` |
| `$val` | a bound parameter |
| `$func` / `$call` | `NAME([DISTINCT ]args)[ OVER (…)][ AS "alias"]` (use `"*"` for `COUNT(*)`) |
| `$cast` | `CAST(expr AS TYPE)` |
| `$case` | searched `CASE WHEN … THEN … [ELSE …] END` |
| `$concat` | `(a || b …)` |
| `$add` `$sub` `$mul` `$div` `$mod` | `(a op b …)` |

Projection items may also be `{"expr": <operand>, "as": "alias"}`.

**Predicates** (WHERE / HAVING / ON)

`$and` `$or` `$not` · `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` ·
`$in`/`$not_in` (list or subquery) · `$between`/`$not_between` ·
`$like`/`$not_like` (+ `escape`) · `$is_null`/`$is_not_null` ·
`$exists`/`$not_exists` · `$any`/`$all`/`$some` (quantified).

**FROM / JOIN** — *table source* = a table name (string), a
`{table, as?, schema?}` dict, a nested `$select`/`$values` (derived table, needs
`as`), or `lateral: true`. `$join`: `{type, table, as?, on? | using?, natural?}`
with type `inner`/`left`/`right`/`full`/`cross`.

**Windows** — `over` on `$func`: `{partition_by?, order_by?, frame?}` where
`frame` is `{type: "rows"|"range", start, end?}` and a bound is
`"unbounded preceding" | "unbounded following" | "current row" |
{preceding: n} | {following: n}`.

**GROUP BY** — a list of expressions, or `{"$rollup": […]}` /
`{"$cube": […]}` / `{"$grouping_sets": [[…], …]}`.

**CTE** — `with`: `[{name, columns?, recursive?, query: $select}]`.

See `tests/` for end-to-end examples.

## Portability caveats

The DSL renders standard SQL and does **not** validate that a target database
supports every feature — portability is the query author's responsibility:

* `LIMIT/OFFSET` vs `OFFSET/FETCH`, `RIGHT/FULL JOIN`, `INTERSECT/EXCEPT`,
  `NATURAL JOIN`, `NULLS FIRST/LAST`, `LATERAL`, `GROUPING SETS/ROLLUP/CUBE`,
  and the concatenation operator (`||` vs `+`) are not universal.
* CTEs and window functions are standard but require recent versions
  (e.g. MySQL ≥ 8, SQLite ≥ 3.25 for windows / ≥ 3.8.3 for CTEs).

Use `RenderOptions` to match the target dialect's placeholder style, identifier
quoting, pagination form, and concatenation operator.

## License

MIT
