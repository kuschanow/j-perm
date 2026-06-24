# j-perm-sql

A [j-perm](https://github.com/kuschanow/j-perm) plugin that builds and executes
**SQL queries** from j-perm constructs.

* SQL is described with a tree of `$`-constructs (`$select`, `$col`, `$val`,
  predicates, joins, …).
* A top-level operation renders that tree to a **parameterized** `(sql, params)`
  pair and hands it to a configurable executor (any ORM's raw-execute function):
  `op: sql` for read-only `SELECT`, and (opt-in) `op: sql_write` for
  `INSERT`/`UPDATE`/`DELETE`.
* The SQL constructs live in **isolated** named pipelines: they mean nothing
  outside those operations. `{"$select": …}` used as an ordinary value is just a
  dict.

> Scope: the full standard **`SELECT`** surface (read-only) via `install_sql`,
> plus standard **`INSERT`/`UPDATE`/`DELETE`** (row content only) via
> `install_sql_write`. Schema DDL (`CREATE`/`ALTER`/`DROP` of tables/columns)
> and non-universal DML (`RETURNING`, `ON CONFLICT`/upsert, `UPDATE … FROM`,
> `DELETE … USING`) are intentionally out of scope.

## Install

```bash
pip install j-perm-sql
```

Requires `j-perm >= 1.10.0`: 1.9.0 made `run_pipeline` a passthrough invoker
(which this plugin relies on), and 1.10.0 added the `nested_spec_pipeline`
compile hook and per-pipeline `CompiledSpec` execution that let `op: sql` be
compiled end-to-end (see [Compilation](#compilation)).

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

## Writing data (`INSERT`/`UPDATE`/`DELETE`)

Writing is a **separate, opt-in install** — you don't get it unless you ask for
it, and `op: sql` stays guaranteed read-only even when both are installed:

```python
from j_perm_sql import install_sql, install_sql_write

install_sql(engine, run_sql)          # read-only  op: sql      (optional)
install_sql_write(engine, run_sql)    # write      op: sql_write
```

`install_sql_write(engine, executor, *, paramstyle="qmark", dialect=None,
op="sql_write")` registers an isolated **write pipeline** (the full `SELECT`
surface *plus* the DML statements, so `WHERE` predicates, `SET` expressions and
`INSERT … SELECT` subqueries all work) and the `op: sql_write` operation. It is
independent of `install_sql` — either may be installed alone, in any order. It
selects the sync/async handler the same way (by `asyncio.iscoroutinefunction`).

```js
{"op": "sql_write", "query": <DML construct tree>, "to": "/dest/path"}
```

**`$insert`** — exactly one of `values` / `query`:

```python
{"$insert": {
  "into": "users",                    # or {"table": "users", "schema": "app"}
  "columns": ["name", "age"],         # optional
  "values": [[{"$val": "Ann"}, {"$val": 30}], ...],   # cells are operands ($val to bind)
  # ── OR ──
  "query": {"$select": {...}},        # INSERT … SELECT
}}
# → INSERT INTO "users" ("name", "age") VALUES (?, ?)   params=["Ann", 30]
```

**`$update`** — single table:

```python
{"$update": {
  "table": "users",                   # or {"table": "users", "schema": "app"}
  "set": {"name": {"$val": "Bob"},
          "visits": {"$add": [{"$col": "visits"}, {"$val": 1}]}},
  "where": {"$eq": [{"$col": "id"}, {"$val": 5}]},      # or "all": true
}}
# → UPDATE "users" SET "name" = ?, "visits" = ("visits" + ?) WHERE "id" = ?
```

**`$delete`** — single table:

```python
{"$delete": {
  "from": "sessions",                 # or {"table": "sessions", "schema": "app"}
  "where": {"$lt": [{"$col": "last_seen"}, {"$val": "2020-01-01"}]},  # or "all": true
}}
# → DELETE FROM "sessions" WHERE "last_seen" < ?
```

`set` values, `$insert` cells, and `where` predicates are ordinary read
constructs, so the full expression/predicate/subquery surface (including
correlated subqueries) is available, and data is always bound as parameters.

> **WHERE guard.** A `$update` / `$delete` **without** a `where` raises unless
> you pass an explicit `"all": true`. This prevents an accidental full-table
> update/delete.

## Parameterization & injection safety

Data values are **always bound as parameters**, never interpolated:

* `$val` (and the data sides of `$in`, `$between`, `$values`, `$update`'s `set`
  values, and `$insert`'s row cells) emit a placeholder and add the value to
  `params`.
* Identifiers (table/column/alias names) are validated against a conservative
  charset and quoted.
* Function names, CAST types, join types, sort directions, etc. are validated
  against whitelists.

```python
{"$eq": [{"$col": {"name": "name"}}, {"$val": {"$ref": "/user_input"}}]}
# → '"name" = ?'   with the (possibly malicious) value safely in params
```

**Parameters come from inside j-perm — no external param source.** Inside `$val`
the value expression is resolved with j-perm's normal value pipeline, so `$ref`,
`${…}` templates, and `@:` dest-pointers all work. The `params` list handed to
the executor is simply the values j-perm itself computed from the document; the
parameter binding exists only for injection safety and the driver's paramstyle.
The whole flow (source → SQL → bound values) is self-contained in one j-perm
run; the executor is just the pipe to the driver. This applies equally to read
(`op: sql`) and write (`op: sql_write`).

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

Both `install_sql` and `install_sql_write` inspect the executor: a coroutine
function registers the async handler (use with `engine.apply_async`); a regular
function registers the sync handler (use with `engine.apply`).

```python
async def run_sql(sql, params): ...
install_sql(engine, run_sql)                  # async
await engine.apply_async(spec, source=…, dest=…)
```

## Compilation

`op: sql` / `op: sql_write` are compilable. `engine.compile(spec)` compiles the
`query` subtree against the isolated SQL pipeline (the engine never needs to
understand the SQL constructs — it routes the nested spec through the registered
pipeline by name), and the rendered tree is dispatched through the compiled path
with per-node memoisation. Re-applying the same `CompiledSpec` keeps every node
compiled; only `$val` data is re-bound from the live context on each run.

```python
compiled = engine.compile([{"op": "sql", "to": "/rows", "query": query}])
compiled.apply(source={"wanted": 1}, dest={})    # renders + executes, fully compiled
compiled.apply(source={"wanted": 2}, dest={})    # reuses compiled nodes, re-binds values
```

This requires the `nested_spec_pipeline` compile hook and per-pipeline
`CompiledSpec` execution added in the core engine (see the "What gets compiled"
section of the main j-perm README).

## Construct reference

**Query**

| Construct | Form |
|---|---|
| `$select` | `{with?, distinct?, columns?, from?, joins?, where?, group_by?, having?, order_by?, limit?/offset? \| fetch?}` |
| `$union` / `$union_all` / `$intersect` / `$except` | `{"$union": [q1, q2, …], order_by?, limit?…}` |
| `$values` | `{"$values": [[…row…], …]}` (table source or `IN`) |

**Write (DML)** — only via `install_sql_write` / `op: sql_write`

| Construct | Form |
|---|---|
| `$insert` | `{into, columns?, values \| query}` (exactly one of `values`/`query`) |
| `$update` | `{table, set: {col: operand}, where? \| "all": true}` |
| `$delete` | `{from, where? \| "all": true}` |

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
