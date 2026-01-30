# J-Perm

A small, composable JSON-transformation DSL implemented in Python.

The library lets you describe transformations as **data** (a list of steps) and then apply them to an input document. It supports JSON Pointer paths, custom JMESPath expressions, interpolation with `${...}` syntax, special reference/evaluation values, and a rich set of built-in operations.

J-Perm is built around a **pluggable architecture**: operations, special constructs, JMESPath functions, and casters are all registered independently and composed into an execution engine.

---

## Features

* JSON Pointer read/write with support for:

  * root pointers (`""`, `"/"`, `"."`)
  * relative `..` segments
  * list slices like `/items[1:3]`
* Interpolation templates:

  * `${/path/to/node}` — JSON Pointer lookup
  * `${int:/path}` / `${float:/path}` / `${bool:/path}` — type casters
  * `${? some.jmespath(expression) }` — JMESPath with custom functions
* Special values:

  * `$ref` — reference into the source document
  * `$eval` — nested DSL evaluation with optional `$select`
* Built-in operations:

  * `set`, `copy`, `copyD`, `delete`, `assert`
  * `foreach`, `if`, `distinct`
  * `replace_root`, `exec`, `update`
* Shorthand syntax for concise scripts (`~delete`, `~assert`, `field[]`, pointer assignments)
* Schema helper: approximate JSON Schema generation for a given DSL script
* Fully extensible via registries:

  * operations
  * special constructs
  * JMESPath functions
  * casters

---

## Architecture overview

J-Perm is composed from four independent registries:

| Registry          | Purpose                                       |
| ----------------- | --------------------------------------------- |
| `OpRegistry`      | Registers DSL operations (`op`)               |
| `SpecialRegistry` | Registers special values (`$ref`, `$eval`, …) |
| `JpFuncRegistry`  | Registers custom JMESPath functions           |
| `CasterRegistry`  | Registers `${type:...}` casters               |

All registries can be imported directly from `j_perm`.

At runtime, these parts are wired together into an execution engine that evaluates the DSL.

---

## Core API

### `ActionEngine.apply_actions`

```python
from j_perm import ActionEngine, Handlers, SpecialResolver, TemplateSubstitutor
```

Typical setup:

```python
substitutor = TemplateSubstitutor()
special = SpecialResolver()
handlers = Handlers()
normalizer = Normalizer()

engine = ActionEngine(
  handlers=handlers,
  special=special,
  substitutor=substitutor,
  normalizer=normalizer,
)
```

### Signature

```python
apply_actions(
  actions: Any,
*,
dest: MutableMapping[str, Any] | List[Any],
source: Mapping[str, Any] | List[Any],
) -> Mapping[str, Any]
```

* **`actions`** — DSL script (list or mapping)
* **`dest`** — initial destination document
* **`source`** — source context available to pointers, interpolation, `$ref`, `$eval`
* Returns a **deep copy** of the final `dest`

---

## Basic usage

```python
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

result = engine.apply_actions(actions, dest={}, source=source)
```

---

## Interpolation & expression system (`${...}`)

Interpolation is handled by `TemplateSubstitutor` and is used throughout operations such as `set`, `copy`, `exec`, `update`, schema building, etc.

### JSON Pointer interpolation

```text
${/path/to/value}
```

Resolves a JSON Pointer against the current source context.

---

### Casters

```text
${int:/age}
${float:/height}
${bool:/flag}
```

Casters are registered via `CasterRegistry` and can be extended by users.

---

### JMESPath expressions

```text
${? items[?price > `10`].name }
```

JMESPath expressions are evaluated against the source context with access to custom JMESPath functions registered in `JpFuncRegistry`.

---

### Multiple templates

Any string may contain multiple `${...}` expressions, resolved left-to-right.

---

## Special values: `$ref` and `$eval`

Special values are resolved by `SpecialResolver`.

### `$ref`

```json
{ "$ref": "/path" }
```

Resolves a pointer against the source context and injects the value.

---

### `$eval`

```json
{ "$eval": [ ... ] }
```

Executes a nested DSL script using the same engine configuration and injects its result.

---

## Shorthand syntax

Shorthand syntax is expanded during normalization:

* `~delete`
* `~assert`
* `field[]`
* pointer assignments

These are converted into explicit operation steps before execution.

---

# Built-ins

## Built-in casters (`${type:...}`)

Casters are registered in `CasterRegistry` and used in templates as `${name:<expr>}`.
The `<expr>` part is first expanded (templates inside are allowed), then cast is applied.

### `int`

**Form:** `${int:/path}`
**Behavior:** `int(value)`

### `float`

**Form:** `${float:/path}`
**Behavior:** `float(value)`

### `str`

**Form:** `${str:/path}`
**Behavior:** `str(value)`

### `bool`

**Form:** `${bool:/path}`
**Behavior:** compatible with the old implementation:

* if value is `int` or `str` → `bool(int(value))`
* otherwise → `bool(value)`

Examples:

```text
${bool:1}      -> True
${bool:"0"}    -> False
${bool:"2"}    -> True
${bool:""}     -> False (falls back to bool("") == False)
```

---

## Built-in special constructs (`$ref`, `$eval`)

Special values are resolved by `SpecialResolver` while walking value trees.
If a mapping contains a known special key, that handler takes over and the whole mapping is replaced by the resolved value.

### `$ref`

**Shape:**

```jsonc
{ "$ref": "<pointer or template>", "$default": <optional> }
```

**Behavior:**

* resolve `"$ref"` through template substitution (so it may contain `${...}`)
* treat it as a pointer and read from **source context**
* if pointer fails:

  * if `"$default"` exists → deep-copy and return default
  * else → re-raise (error)

Example:

```json
{ "op": "set", "path": "/user", "value": { "$ref": "/rawUser" } }
```

### `$eval`

**Shape:**

```jsonc
{ "$eval": <actions>, "$select": "<optional pointer>" }
```

**Behavior:**

* run nested DSL using the same engine configuration
* if `"$select"` is present → select a sub-value from the nested result

Example:

```json
{
  "op": "set",
  "path": "/flag",
  "value": {
    "$eval": [
      { "op": "replace_root", "value": false },
      { "op": "replace_root", "value": true }
    ],
    "$select": ""
  }
}
```

---

## Built-in JMESPath functions

Custom JMESPath functions are registered in `JpFuncRegistry` and available inside `${? ... }`.

### `subtract(a, b)`

**Signature:** `subtract(number, number) -> number`
**Behavior:** returns `a - b`

Example:

```text
${? subtract(price, tax) }
```

> Если у тебя есть другие встроенные JMES-функции в проекте (кроме `subtract`), скажи их имена — я добавлю их описания в README в том же формате.

---

## Built-in operations

All operations are registered in `OpRegistry` under their `op` name.
They are executed by `ActionEngine.apply_actions()` after normalization/shorthand expansion.

### Common notes

* Many operations accept values that may contain:

  * special constructs (`$ref`, `$eval`, and user-added ones)
  * templates (`${...}`)
* Unless stated otherwise: values are typically resolved as:

  1. `SpecialResolver.resolve(...)`
  2. `TemplateSubstitutor.substitute(...)`
  3. deep-copied before writing into `dest`

---

### `set`

Set or append a value at a JSON Pointer path in `dest`.

**Shape:**

```jsonc
{
  "op": "set",
  "path": "/pointer",
  "value": <any>,
  "create": true,   // default: true
  "extend": true    // default: true
}
```

**Semantics:**

* writes resolved `value` into `dest[path]`
* if `path` ends with `"/-"` → append to list
* if appending and `value` is a list:

  * `extend=true` → extend the list
  * `extend=false` → append list as one item
* `create=true` → create missing parent containers

---

### `copy`

Copy value from **source context** to `dest` (internally uses `set`).

**Shape:**

```jsonc
{
  "op": "copy",
  "from": "/source/pointer",
  "path": "/target/pointer",
  "create": true,          // default: true
  "extend": true,          // default: true
  "ignore_missing": false, // default: false
  "default": <any>         // optional
}
```

**Semantics:**

* `"from"` may be templated, then used as a pointer into source
* if missing:

  * `ignore_missing=true` → no-op
  * else if `"default"` provided → use default
  * else → error

---

### `copyD`

Copy value from **dest** into another location in `dest` (self-copy).

**Shape:**

```jsonc
{
  "op": "copyD",
  "from": "/dest/pointer",
  "path": "/target/pointer",
  "create": true,          // default: true
  "ignore_missing": false, // default: false
  "default": <any>         // optional
}
```

**Semantics:**

* `"from"` pointer is resolved against current `dest`
* pointer string itself may be templated with source context

---

### `delete`

Delete a node at a pointer in `dest`.

**Shape:**

```jsonc
{
  "op": "delete",
  "path": "/pointer",
  "ignore_missing": true   // default: true
}
```

**Notes:**

* path must not end with `"-"`
* if missing and `ignore_missing=false` → error

---

### `assert`

Assert node existence and optional equality in `dest`.

**Shape:**

```jsonc
{
  "op": "assert",
  "path": "/pointer",
  "equals": <any> // optional
}
```

**Semantics:**

* if path missing → `AssertionError`
* if `equals` provided and not equal → `AssertionError`

---

### `foreach`

Iterate over an array (or mapping) from source context and execute nested actions.

**Shape:**

```jsonc
{
  "op": "foreach",
  "in": "/array/path",
  "do": [ ... ],
  "as": "item",      // default: "item"
  "default": [],     // default: []
  "skip_empty": true // default: true
}
```

**Semantics:**

* resolve `"in"` pointer against source context
* if missing → use `"default"`
* if resolved value is a dict → iterate over items as pairs
* for each element:

  * extend source context with variable name `"as"`
  * execute `"do"` with same engine
* on exception inside body → restore `dest` from snapshot

---

### `if`

Conditionally execute nested actions.

**Path-based mode:**

```jsonc
{
  "op": "if",
  "path": "/pointer",
  "equals": <any>,   // optional
  "exists": true,    // optional
  "then": [ ... ],   // optional
  "else": [ ... ],   // optional
  "do":   [ ... ]    // optional fallback success branch
}
```

**Expression-based mode:**

```jsonc
{
  "op": "if",
  "cond": "${?...}",
  "then": [ ... ],
  "else": [ ... ],
  "do":   [ ... ]
}
```

**Semantics:**

* one of `path` or `cond` must be present
* `then` runs on true, `else` runs on false
* `do` is used as “then” if `then` is missing
* snapshot + restore on exceptions inside chosen branch

---

### `distinct`

Remove duplicates from a list at `dest[path]`, preserving order.

**Shape:**

```jsonc
{
  "op": "distinct",
  "path": "/list/path",
  "key": "/key/pointer" // optional
}
```

**Semantics:**

* target must be a list
* if `key` provided → key pointer is evaluated per item

---

### `replace_root`

Replace the whole destination root with a new value.

**Shape:**

```jsonc
{
  "op": "replace_root",
  "value": <any>
}
```

**Semantics:**

* resolve specials/templates inside `value`
* deep-copy, then replace entire `dest`

---

### `exec`

Execute a nested script held inline or referenced from source context.

**Pointer mode:**

```jsonc
{
  "op": "exec",
  "from": "/script/path",
  "default": <any>,   // optional
  "merge": false      // default: false
}
```

**Inline mode:**

```jsonc
{
  "op": "exec",
  "actions": [ ... ],
  "merge": false      // default: false
}
```

**Semantics:**

* exactly one of `from` / `actions`
* if `from` cannot be resolved:

  * if `default` present → use it (after specials/templates)
  * else → error
* `merge=false`:

  * run nested script with `dest={}`
  * replace current `dest` with result
* `merge=true`:

  * run nested script on current dest (like a sub-call)

---

### `update`

Update a mapping at `path` using either source mapping (`from`) or inline mapping (`value`).

**Shape:**

```jsonc
{
  "op": "update",
  "path": "/target/path",
  "from": "/source/path", // required in from-mode
  "value": { ... },       // required in value-mode
  "default": { ... },     // optional (from-mode only)
  "create": true,         // default: true
  "deep": false           // default: false
}
```

**Semantics:**

* exactly one of `from` / `value`
* update payload must be a mapping, else `TypeError`
* target at `path` must be mutable mapping, else `TypeError`
* `deep=false` → shallow `dict.update`
* `deep=true` → recursive merge for nested mappings

---

## Shorthand normalization

In addition to explicit DSL steps, J-Perm supports a *shorthand syntax* for more concise scripts.
Shorthand forms are expanded into regular operation steps **before execution**.

Shorthand expansion is implemented as a pluggable normalization layer, similar to operations and special constructs.

### How it works

During normalization, each mapping entry is processed by a chain of registered *shorthand rules*:

1. Each rule decides whether it can handle a given `(key, value)` pair.
2. If a rule matches, it expands the entry into one or more explicit operation steps.
3. The first matching rule wins.
4. If no rule matches, normalization fails with an error.

The resulting list of steps is then executed by the engine as a normal DSL script.

---

### Built-in shorthand rules

The following shorthand forms are enabled by default.

#### Delete shorthand (`~delete`)

```json
{ "~delete": ["/a", "/b"] }
```

Expands into:

```json
{ "op": "delete", "path": "/a" }
{ "op": "delete", "path": "/b" }
```

---

#### Assert shorthand (`~assert`)

Mapping form:

```json
{ "~assert": { "/x": 10, "/y": 20 } }
```

Expands into:

```json
{ "op": "assert", "path": "/x", "equals": 10 }
{ "op": "assert", "path": "/y", "equals": 20 }
```

List / string form:

```json
{ "~assert": ["/x", "/y"] }
```

Expands into existence-only assertions.

---

#### Append shorthand (`field[]`)

A key ending with `[]` means *append to a list* at that path:

```json
{ "items[]": 123 }
```

Expands into:

```json
{ "op": "set", "path": "/items/-", "value": 123 }
```

---

#### Pointer assignment shorthand

If a value is a string that starts with `/`, it is treated as a source pointer:

```json
{ "name": "/user/fullName" }
```

Expands into:

```json
{
  "op": "copy",
  "from": "/user/fullName",
  "path": "/name",
  "ignore_missing": true
}
```

---

## Extending J-Perm

### Custom operations

```python
from ..op_handler import OpRegistry

@OpRegistry.register("my_op")
def my_op(step, dest, src, engine):
  return dest
```

---

### Custom special constructs

```python
from j_perm import SpecialRegistry

@SpecialRegistry.register("$upper")
def sp_upper(node, src, resolver):
  value = resolver.substitutor.substitute(node["$upper"], src)
  return str(value).upper()
```

---

### Custom JMESPath functions

```python
from j_perm import JpFuncRegistry
from jmespath import functions as jp_funcs

@JpFuncRegistry.register("subtract")
@jp_funcs.signature({"types": ["number"]}, {"types": ["number"]})
def _subtract(self, a, b):
  return a - b
```

Usage in DSL:

```text
${? subtract(price, tax) }
```

---

### Custom casters

```python
from j_perm import CasterRegistry

@CasterRegistry.register("json")
def cast_json(x):
  return json.loads(x)
```

Usage:

```text
${json:/raw_payload}
```

---

### Custom shorthand rules

```python
from j_perm import ShorthandRegistry, ExpandResult

@ShorthandRegistry.register("name", priority=10)
def my_shorthand_rule(key: str, value: Any) -> ExpandResult | None:
    if key.startswith("my_prefix_"):
        # expand into steps
        steps = [ ... ]
        return steps
```

---

## Plugin loading

Registries collect definitions **at import time**.
To enable “use all registered components”, ensure that modules defining custom ops, specials, casters, or JMESPath functions are imported before engine construction.

A common pattern is to import all plugins in one place:

```python
import my_project.jperm_plugins
```

---

## License

This package is provided as-is; feel free to adapt it to your project structure.
