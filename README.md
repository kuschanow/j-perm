# J-Perm

A small, composable JSON-transformation DSL implemented in Python.

The library lets you describe transformations as **data** (a list of steps) and then apply them to an input document. It supports JSON Pointer paths, custom JMESPath expressions, interpolation with `${...}` syntax, special reference/evaluation values, and a rich set of built-in operations.

---

## Features

* JSON Pointer read/write with support for:

    * root pointers (`""`, `"/"`, `"."`)
    * relative `..` segments
    * list slices like `/items[1:3]`
* Interpolation templates:

    * `${/path/to/node}` — JSON Pointer lookup
    * `${int:/path}` / `${float:/path}` / `${bool:/path}` — simple type casters
    * `${? some.jmespath(expression) }` — JMESPath with custom functions
* Special values:

    * `$ref` — reference into the source document
    * `$eval` — nested DSL evaluation with optional `$select`
* Built-in operations:

    * `set`, `copy`, `copyD`, `delete`, `assert`
    * `foreach`, `if`, `distinct`
    * `replace_root`, `exec`, `update`
* Shorthand syntax for concise scripts (`~delete`, `~assert`, `field[]`, pointer assignments)
* Schema helper: approximate JSON Schema generation for a given DSL script.

---

## Core API

### `apply_actions`

```python
from j_perm import apply_actions

result = apply_actions(actions, dest, source)
```

**Signature:**

```python
apply_actions(
    actions: Any,
    *,
    dest: MutableMapping[str, Any] | List[Any],
    source: Mapping[str, Any] | List[Any],
) -> Mapping[str, Any]
```

* **`actions`** — DSL script (list or mapping).
  Internally normalized via `normalize_actions()` and shorthand expansion.
* **`dest`** — initial destination document to transform (typically a dict or list).
* **`source`** — source document or context; available to pointers, interpolation, `$ref`, `$eval`, and nested operations.
* Returns a **deep copy** of the final `dest`.

---

## Basic usage

```python
from j_perm import apply_actions

source = {
    "users": [
        {"name": "Alice", "age": 17},
        {"name": "Bob",   "age": 22}
    ]
}

actions = [
    # Start with empty list
    {"op": "replace_root", "value": []},

    # For each user - build a simplified object
    {
        "op": "foreach",
        "in": "/users",
        "as": "u",
        "do": [
            {
                "op": "set",
                "path": "/-",
                "value": {
                    "name": "${/u/name}",
                    "is_adult": {
                        "$eval": [
                            {"op": "replace_root", "value": False},
                            {
                                "op": "if",
                                "cond": "${?`${/u/age}` >= `18`}",
                                "then": [{"op": "replace_root", "value": True}]
                            }
                        ]
                    }
                }
            }
        ]
    }
]

result = apply_actions(actions, dest={}, source=source)
```

---

# Interpolation & expression system (`${...}`)

Interpolation is handled by the substitution utility used throughout operations such as `set`, `copy`, `exec`, `update`, schema building, etc.

### 1. JSON Pointer interpolation

* **Form:** `${/path/to/value}`
* Meaning: resolve `/path/to/value` against the current **source context** (the original `source` plus any loop variables, etc.) and inject the value.

Example:

```json
{
  "op": "set",
  "path": "/user/name",
  "value": "Hello, ${/user/first_name}!"
}
```

### 2. Casters

* `${int:/age}` → cast value at `/age` to `int`
* `${float:/height}` → cast to `float`
* `${bool:/flag}` → cast to `bool`

If the cast fails, an exception is raised.

### 3. JMESPath expressions

* **Form:** `${? <expression> }`
* Evaluated against the **source context**, with access to any custom JMESPath functions wired into the engine.

Example:

```json
{
  "op": "set",
  "path": "/expensiveNames",
  "value": "${? items[?price > `10`].name }"
}
```

### 4. Multiple templates in one string

Any string can contain multiple `${...}` segments, which are resolved left-to-right.

---

# Special values: `$ref` and `$eval`

Special values are resolved by `resolve_special()` before normal interpolation/substitution in operations like `set`, `update`, `exec`, `replace_root`.

### `$ref`

`{"$ref": "/path"}` means *“use the value at this path from the source”*.

Example:

```json
{
  "op": "set",
  "path": "/user",
  "value": { "$ref": "/rawUser" }
}
```

This behaves similarly to a `copy` from `/rawUser`, but can be nested into larger structures and combined with `$eval` and interpolation.

### `$eval`

`{"$eval": [...]}` means *“execute this nested DSL script and use its result here”*.

Example:

```json
{
  "op": "set",
  "path": "/flag",
  "value": {
    "$eval": [
      { "op": "replace_root", "value": false },
      {
        "op": "if",
        "cond": "${? some.expression }",
        "then": [{ "op": "replace_root", "value": true }]
      }
    ]
  }
}
```

The nested script has the same `source` context as the outer script, and its final result becomes the value injected at `/flag`.

---

# Shorthand syntax (shortcuts)

Shorthand is expanded by `normalize_actions()` into explicit operation steps.

### How shorthand works internally

* `actions` may be:

    * a list of steps,
    * a mapping without `"op"` — in that case it’s treated as shorthand and expanded.

Expansion is done by `_expand_shorthand()`.

### 1. Delete shorthand: `~delete`

```json
{ "~delete": "/path" }
```

If value is a list:

```json
{ "~delete": ["/a", "/b"] }
```

Expands into:

```json
{ "op": "delete", "path": "/a" }
{ "op": "delete", "path": "/b" }
```

### 2. Assert shorthand: `~assert`

If value is a mapping:

```json
{ "~assert": { "/x": 10, "/y": 20 } }
```

Expands into:

```json
{ "op": "assert", "path": "/x", "equals": 10 }
{ "op": "assert", "path": "/y", "equals": 20 }
```

If value is a string or list of strings:

```json
{ "~assert": "/x" }
# or
{ "~assert": ["/x", "/y"] }
```

Expands into assertions that only check **existence** at those paths.

### 3. Append shorthand: `field[]`

A key ending with `[]` means “append to list at this path”.

```json
{ "items[]": 123 }
```

Expands into:

```json
{ "op": "set", "path": "/items/-", "value": 123 }
```

### 4. Pointer assignment shorthand

If a value is a **string that starts with `/`**, it’s treated as a pointer and expanded into a `copy` operation:

```json
{ "name": "/user/fullName" }
```

Expands into:

```json
{
  "op": "copy",
  "path": "/name",
  "from": "/user/fullName",
  "ignore_missing": true
}
```

(See `_is_pointer_string()` and `_expand_shorthand()` for details. )

---

# Built-in operations: signatures & parameters

Below are all core operations with:

* the **step shape**,
* **required** vs **optional** parameters,
* **default values**.

All defaults are taken directly from the Python implementations.

---

## `set`

Set or append a value at a JSON Pointer path in `dest`.

### Signature

```jsonc
{
  "op": "set",          // required
  "path": "/pointer",   // required
  "value": <any>,       // required
  "create": true,       // optional, default: true
  "extend": true        // optional, default: true
}
```

### Parameters

* **`path`** (required)
  JSON Pointer path where the value is written.

    * If the path ends with `"/-"`, the value is **appended** to a list.
* **`value`** (required)
  Any value, including special `$ref` / `$eval` objects and interpolated strings.
  Resolved by `resolve_special()` and then `substitute()`.
* **`create`** (optional, default **`true`**)
  If `true`, missing parent containers are created.
* **`extend`** (optional, default **`true`**)
  If `path` is `"/-"` and `value` is a list:

    * `extend=true` ⇒ list is extended with elements of `value`
    * `extend=false` ⇒ `value` is appended as a single item (nested list)

---

## `copy`

Copy a value from `source` (or extended source context) into `dest`. Internally uses `set`.

### Signature

```jsonc
{
  "op": "copy",                // required
  "from": "/source/pointer",   // required
  "path": "/target/pointer",   // required
  "create": true,              // optional, default: true
  "extend": true,              // optional, default: true
  "ignore_missing": false,     // optional, default: false
  "default": <any>             // optional
}
```

### Parameters

* **`from`** (required)
  JSON Pointer into the **source** context (after interpolation).
* **`path`** (required)
  Destination path; same semantics as in `set`.
* **`create`** (optional, default **`true`**)
  Passed through to `set` — create missing parents.
* **`extend`** (optional, default **`true`**)
  Passed through to `set` — controls list extension on appends.
* **`ignore_missing`** (optional, default **`false`**)
  If `true` and `from` cannot be resolved, the operation is a no-op.
* **`default`** (optional)
  Used when `from` cannot be resolved and `ignore_missing` is **not** set.
  If provided, `default` is copied into `path` instead of raising.

---

## `copyD`

Copy a value from the **current `dest`** (self) into another path in `dest`. Internally uses `set`.

This is useful for rearranging or duplicating data that has already been built in `dest`, without going back to the `source`.

### Signature

```jsonc
{
  "op": "copyD",               // required
  "from": "/source/pointer",   // required
  "path": "/target/pointer",   // required
  "create": true,              // optional, default: true
  "ignore_missing": false,     // optional, default: false
  "default": <any>             // optional
}
```

### Parameters

* **`from`** (required)
  JSON Pointer evaluated against **`dest`** (not `source`). The pointer itself is first interpolated with `src` (source context), but resolution uses `dest`.
* **`path`** (required)
  Destination path in `dest`, same semantics as in `set`.
* **`create`** (optional, default **`true`**)
  Passed to `set` — whether to create missing parent containers.
* **`ignore_missing`** (optional, default **`false`**)
  If `true` and the `from` pointer cannot be resolved in `dest`, the operation becomes a no-op.
* **`default`** (optional)
  Used when `from` cannot be resolved and `ignore_missing` is **not** set.
  If provided, that default is deep-copied into `path` instead of raising.

---

## `delete`

Delete a node at a JSON Pointer path in `dest`.

### Signature

```jsonc
{
  "op": "delete",          // required
  "path": "/pointer",      // required
  "ignore_missing": true   // optional, default: true
}
```

### Parameters

* **`path`** (required)
  JSON Pointer to the node to delete.
  Must not end with `"-"`.
* **`ignore_missing`** (optional, default **`true`**)
  If `false`, a missing path raises an error;
  if `true`, missing path is silently ignored.

---

## `assert`

Assert node existence and optional equality in `dest`.

### Signature

```jsonc
{
  "op": "assert",      // required
  "path": "/pointer",  // required
  "equals": <any>      // optional
}
```

### Parameters

* **`path`** (required)
  JSON Pointer to check in `dest`.
  If it does not exist, an `AssertionError` is raised.
* **`equals`** (optional)
  If provided, the value at `path` is compared with `equals`;
  mismatch raises `AssertionError`.

---

## `foreach`

Iterate over an array (or mapping) in the source context and execute nested actions.

### Signature

```jsonc
{
  "op": "foreach",        // required
  "in": "/array/path",    // required
  "do": [ ... ],          // required
  "as": "item",           // optional, default: "item"
  "default": [],          // optional, default: []
  "skip_empty": true      // optional, default: true
}
```

### Parameters

* **`in`** (required)
  JSON Pointer to the array in the source context (after interpolation).
* **`do`** (required)
  Nested DSL script (list or single op) executed for each element.
* **`as`** (optional, default **`"item"`**)
  Name of the variable bound to the current element in the extended source context.
* **`default`** (optional, default **`[]`**)
  Used when the pointer in `"in"` cannot be resolved.
* **`skip_empty`** (optional, default **`true`**)
  If `true` and the resolved array is empty, the loop is skipped.

Additional behavior:

* If the resolved object is a **dict**, it’s converted to a list of `(key, value)` pairs.
* On exception in the body, `dest` is restored from a deep copy snapshot.

---

## `if`

Conditionally execute nested actions.

### Signature (path-based condition)

```jsonc
{
  "op": "if",              // required
  "path": "/pointer",      // required in this mode
  "equals": <any>,         // optional
  "exists": true,          // optional
  "then": [ ... ],         // optional
  "else": [ ... ],         // optional
  "do": [ ... ]            // optional (fallback success branch)
}
```

### Signature (expression-based condition)

```jsonc
{
  "op": "if",              // required
  "cond": "${?...}",       // required in this mode
  "then": [ ... ],         // optional
  "else": [ ... ],         // optional
  "do": [ ... ]            // optional (fallback success branch)
}
```

### Parameters

At least **one** of `path` or `cond` must be supplied:

* **`path`** (required in path-mode)

    * If combined with `equals`, condition is `dest[path] == equals` and path must exist.
    * If combined with `exists` (truthy), condition is “path exists”.
    * Else, condition is `bool(dest[path])` (path must exist).
* **`equals`** (optional, path-mode only)
  Expected value for equality check.
* **`exists`** (optional, path-mode only)
  If truthy, check is presence of path only.
* **`cond`** (required in expression-mode)
  Arbitrary interpolated value or expression; `bool(cond)` is used.

Branches:

* **`then`** (optional)
  Executed when condition is **true**.
* **`else`** (optional)
  Executed when condition is **false**.
* **`do`** (optional)
  If condition is **true** and `"then"` is missing, `"do"` is used as the success branch.

If no branch is present for the chosen condition result, `dest` is returned unchanged.

Error handling:

* Before running branch actions, a deep copy snapshot of `dest` is taken.
* On exception, `dest` is restored to the snapshot.

---

## `distinct`

Remove duplicates in a list at the given path, preserving order.

### Signature

```jsonc
{
  "op": "distinct",       // required
  "path": "/list/path",   // required
  "key": "/key/pointer"   // optional
}
```

### Parameters

* **`path`** (required)
  JSON Pointer to a **list** in `dest`.
  If target is not a list, a `TypeError` is raised.
* **`key`** (optional)
  JSON Pointer evaluated per item to compute the *deduplication key*.

    * If provided: uniqueness is based on `jptr_get(item, key_path)`.
    * If omitted: the whole `item` is used as the key.

---

## `replace_root`

Replace the whole `dest` root value with a new one.

### Signature

```jsonc
{
  "op": "replace_root",   // required
  "value": <any>          // required
}
```

### Parameters

* **`value`** (required)
  Value to become the new root.

    * Special values (`$ref`, `$eval`) are resolved via `resolve_special()`.
    * Strings/lists/dicts are then passed through interpolation `substitute()`.
    * The final value is deep-copied.

Result of `replace_root` is the new `dest`.

---

## `exec`

Execute a nested DSL script held inline or at a pointer.

Exactly one of `from` or `actions` must be provided.

### Signature (script from pointer)

```jsonc
{
  "op": "exec",           // required
  "from": "/script/path", // required in this mode
  "default": <any>,       // optional
  "merge": false          // optional, default: false
}
```

### Signature (inline script)

```jsonc
{
  "op": "exec",           // required
  "actions": [ ... ],     // required in this mode
  "merge": false          // optional, default: false
}
```

### Parameters

* **`from`** (required in pointer-mode)
  Pointer (possibly interpolated) to the actions in the source context.
  If resolution fails:

    * and `default` is present ⇒ use `default` as the script (after resolving specials and interpolation if it’s str/list/dict),
    * else ⇒ raise `ValueError`.
* **`actions`** (required in inline-mode)
  Inline DSL script (list or mapping). Special values and templates are resolved.
* **`merge`** (optional, default **`false`**)

    * `merge=false` (default): nested script runs with `dest={}`, result **replaces** current `dest`.
    * `merge=true`: nested script runs **on current dest** and the result is returned (like a sub-call to `apply_actions` on same `dest`).
* **`default`** (optional, pointer-mode only)
  Fallback script if `from` cannot be resolved.

---

## `update`

Update a mapping at the given path using either inline value or source mapping.

Exactly one of `from` or `value` must be provided.

### Signature

```jsonc
{
  "op": "update",             // required
  "path": "/target/path",     // required
  "from": "/source/path",     // required in from-mode
  "value": { ... },           // required in value-mode
  "default": { ... },         // optional (from-mode only)
  "create": true,             // optional, default: true
  "deep": false               // optional, default: false
}
```

### Parameters

* **`path`** (required)
  JSON Pointer path to the **mapping** to update.
* **`from`** (required in from-mode)
  Pointer to mapping in source; deep-copied.
  If resolution fails:

    * and `default` is provided ⇒ use `default`,
    * else ⇒ raise.
* **`value`** (required in value-mode)
  Inline mapping; resolved through `resolve_special` and `substitute` if needed.
* **`default`** (optional, from-mode only)
  Mapping used when `from` cannot be resolved.
* **`create`** (optional, default **`true`**)
  If `true`, missing containers at `path` are created (including the leaf if needed).
* **`deep`** (optional, default **`false`**)

    * `false`: shallow update via `dict.update()`.
    * `true`: recursive deep-merge of nested mappings; leaves and non-mappings are overwritten via deep copy.

Constraints:

* The resolved update value **must be a mapping**; otherwise `TypeError`.
* The target at `path` must be a mutable mapping; otherwise `TypeError`.

---

# Schema generation

```python
from j_perm import build_schema

schema = build_schema(script)
```

`schema` is a JSON-Schema-like structure inferred by scanning the script:

* Tracks `replace_root`, `set`, `update`, nested `$eval`, etc.
* Infers basic JSON types from literal values.

---

## Extending with custom operations

```python
from j_perm import register_op

@register_op("my_op")
def my_op(step, dest, src):
    # implement your logic
    return dest
```

Any registered operation can be used in scripts via:

```json
{ "op": "my_op", ... }
```

---

## License

This package is provided as-is; feel free to adapt it to your project structure.
