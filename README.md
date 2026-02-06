# J-Perm

A composable JSON transformation DSL with a powerful, extensible architecture.

J-Perm lets you describe data transformations as **executable specifications** — a list of steps that can be applied to input documents. It supports
JSON Pointer addressing, template interpolation with `${...}` syntax, special constructs (`$ref`, `$eval`), and a rich set of built-in operations.

**Key features:**

- 🎯 **Declarative** — transformations are data, not code
- 🔌 **Pluggable architecture** — stages, handlers, matchers form a composable pipeline
- 🌳 **Hierarchical registries** — organize operations and value processors in trees
- 📦 **Self-contained** — zero dependencies on external registries
- 🎨 **Shorthand syntax** — write concise scripts that expand to full operations

---

## Quick Example

```python
from j_perm import build_default_engine

engine = build_default_engine()

# Source data
source = {
    "users": [
        {"name": "Alice", "age": "17"},
        {"name": "Bob", "age": "22"}
    ]
}

# Transformation spec (using shorthands)
spec = {
    "op": "foreach",
    "in": "/users",
    "do": {
        "/adults[]": {
            "$eval": [
                {"/name": "/item/name", "/age": "${int:/item/age}"},
                {"op": "if", "path": "/age", "cond": "${?dest.age >= `18`}", "else": {"~delete": "/age"}}
            ]
        }
    }
}

result = engine.apply(spec, source=source, dest={})
# → {"adults": [{"name": "Bob", "age": 22}]}
```

---

## Installation

```bash
pip install j-perm
```

*(or copy the package into your project)*

---

## Architecture Overview

J-Perm is built on a **pipeline architecture** with two main levels:

```
┌─────────────────────────────────────────────────────────┐
│  spec (user input)                                      │
│    │                                                    │
│    ▼                                                    │
│  ┌──────────────────────────────────────────────────┐   │
│  │ STAGES (batch preprocessing, priority order)     │   │
│  │  • ShorthandExpansion → expand ~delete, etc      │   │
│  │  • YourCustomStage                               │   │
│  └──────────────────────────────────────────────────┘   │
│    │                                                    │
│    ▼                                                    │
│  List[step]                                             │
│    │                                                    │
│    ▼  for each step:                                    │
│  ┌──────────────────────────────────────────────────┐   │
│  │ MIDDLEWARES (per-step, priority order)           │   │
│  │  • Validation, logging, etc.                     │   │
│  └──────────────────────────────────────────────────┘   │
│    │                                                    │
│    ▼                                                    │
│  ┌──────────────────────────────────────────────────┐   │
│  │ REGISTRY (hierarchical dispatch tree)            │   │
│  │  • SetHandler, CopyHandler, ForeachHandler, ...  │   │
│  └──────────────────────────────────────────────────┘   │
│    │                                                    │
│    │  handlers call ctx.engine.process_value(...)       │
│    └─────────────────────────────────────┐              │
│                                          ▼              │
│  ┌──────────────────────────────────────────────────┐   │
│  │ VALUE PIPELINE (stabilization loop)              │   │
│  │  • SpecialResolveHandler ($ref, $eval)           │   │
│  │  • TemplSubstHandler (${...})                    │   │
│  │  • RecursiveDescentHandler (containers)          │   │
│  │  • IdentityHandler (scalars)                     │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Core Components

| Component              | Purpose                                                          |
|------------------------|------------------------------------------------------------------|
| **Engine**             | Orchestrates pipelines, manages context, runs stabilization loop |
| **Pipeline**           | Runs stages → middlewares → registry dispatch for each step      |
| **StageRegistry**      | Tree of batch preprocessors (run-all, priority order)            |
| **ActionTypeRegistry** | Tree of action handlers (first-match or run-all)                 |
| **ValueResolver**      | Abstraction for addressing (JSON Pointer implementation)         |

---

## Core API

### Building an Engine

```python
from j_perm import build_default_engine

# Default engine with all built-ins
engine = build_default_engine()

# Custom specials (None = use defaults: $ref, $eval)
engine = build_default_engine(
    specials={"$ref": my_ref_handler, "$custom": my_handler},
    casters={"int": lambda x: int(x), "json": lambda x: json.loads(x)},
    jmes_options=jmespath.Options(custom_functions=CustomFunctions())
)
```

### Applying Transformations

```python
result = engine.apply(
    spec,  # DSL script (dict or list)
    source=source,  # Source context (for pointers, templates)
    dest=dest,  # Initial destination (default: {})
)
```

**Returns:** Deep copy of the final `dest` after all transformations.

---

## Features

### 1. JSON Pointer Addressing

J-Perm uses **RFC 6901 JSON Pointer** with extensions:

```python
from j_perm import PointerResolver

resolver = PointerResolver()

# Basic pointers
resolver.get("/users/0/name", data)  # → "Alice"

# Root references (work on scalars too!)
resolver.get(".", 42)  # → 42
resolver.get("/", "text")  # → "text"

# Parent navigation
resolver.get("/a/b/../c", data)  # → data["a"]["c"]

# List slices
resolver.get("/items[1:3]", data)  # → [item1, item2]

# Append notation
resolver.set("/items/-", data, "new")  # Append to list
```

**Key feature:** Unlike standard JSON Pointer, `PointerResolver` works on **any type** (scalars, lists, dicts) for root references.

---

### 2. Template Interpolation (`${...}`)

Templates are resolved by `TemplSubstHandler` in the value pipeline.

#### JSON Pointer lookup

```python
"${/user/name}"  # → Resolve pointer from source
```

#### Type casters (built-in)

```python
"${int:/age}"  # → int(value)
"${float:/price}"
"${bool:/flag}"  # → bool(int(value)) if int/str, else bool(value)
"${str:/id}"
```

#### JMESPath queries

```python
"${?source.items[?price > `10`].name}"  # → Query source with JMESPath
"${?dest.total}"                         # → Query destination
"${?add(dest.x, source.y)}"              # → Mix source and dest
```

**Built-in JMESPath functions:** `add(a, b)`, `subtract(a, b)`

**Data structure:** JMESPath expressions use explicit namespaces:
- `source.*` – access source document
- `dest.*` – access destination document

#### Nested templates

```python
"${${/path_to_field}}"  # → Resolve inner template first
```

#### Escaping

```text
$${ → ${ (literal)
$$  → $  (literal)
```

---

### 3. Special Constructs

Special values are resolved by `SpecialResolveHandler`.

#### `$ref` — Reference resolution

```json
{
    "$ref": "/path/to/value",
    "$default": "fallback"
}
```

- Resolves pointer from **source** context
- Returns deep copy (no aliasing)
- Supports `$default` fallback

#### `$eval` — Nested evaluation

```json
{
    "$eval": [
        {
            "op": "set",
            "path": "/x",
            "value": 1
        }
    ],
    "$select": "/x"
}
```

- Executes nested DSL with `dest={}`
- Optionally selects sub-path from result

---

### 4. Shorthand Syntax

Shorthands are expanded by **priority-ordered StageProcessors** before execution.

#### `~assert` / `~assertD`

```json
{
    "~assert": {
        "/x": 10,
        "/y": 20
    }
}
```

Expands to:

```json
[
    {
        "op": "assert",
        "path": "/x",
        "equals": 10
    },
    {
        "op": "assert",
        "path": "/y",
        "equals": 20
    }
]
```

#### `~delete`

```json
{
    "~delete": [
        "/tmp",
        "/cache"
    ]
}
```

Expands to:

```json
[
    {
        "op": "delete",
        "path": "/tmp"
    },
    {
        "op": "delete",
        "path": "/cache"
    }
]
```

#### Append notation (`field[]`)

```json
{
    "/items[]": 123
}
```

Expands to:

```json
{
    "op": "set",
    "path": "/items/-",
    "value": 123
}
```

#### Pointer assignment

```json
{
    "/name": "/user/fullName"
}
```

Expands to:

```json
{
    "op": "copy",
    "from": "/user/fullName",
    "path": "/name",
    "ignore_missing": true
}
```

#### Literal assignment

```json
{
    "/status": "active"
}
```

Expands to:

```json
{
    "op": "set",
    "path": "/status",
    "value": "active"
}
```

**Priority order:** `~assert` (100) → `~delete` (50) → `assign/copy` (0)

---

## Built-in Operations

All operations are registered as `ActionHandler` instances in the main registry.

### `set`

Write value to destination path.

```json
{
    "op": "set",
    "path": "/target",
    "value": "...",
    "create": true,
    // Auto-create parents (default: true)
    "extend": true
    // Extend lists on append (default: true)
}
```

**Special:** `path` ending with `/-` appends to list.

---

### `copy`

Copy value from source to destination.

```json
{
    "op": "copy",
    "from": "/source/path",
    "path": "/dest/path",
    "ignore_missing": false,
    // Skip if missing (default: false)
    "default": "..."
    // Fallback value
}
```

---

### `copyD`

Copy within destination (self-copy).

```json
{
    "op": "copyD",
    "from": "/dest/src",
    "path": "/dest/target"
}
```

---

### `delete`

Remove value at path.

```json
{
    "op": "delete",
    "path": "/remove",
    "ignore_missing": true
    // Don't error if missing (default: true)
}
```

---

### `foreach`

Iterate over array/mapping.

```json
{
    "op": "foreach",
    "in": "/items",
    "as": "item",
    // Variable name (default: "item")
    "do": [
        ...
    ],
    // Nested actions
    "skip_empty": true,
    // Skip if empty (default: true)
    "default": []
    // Fallback if missing
}
```

**Note:** If source is a dict, iterates over `(key, value)` tuples.

---

### `if`

Conditional execution.

**Path mode:**

```json
{
    "op": "if",
    "path": "/check",
    "equals": "value",
    // Optional
    "exists": true,
    // Optional
    "then": [
        ...
    ],
    // Success branch
    "else": [
        ...
    ]
    // Failure branch
}
```

**Expression mode:**

```json
{
    "op": "if",
    "cond": "${?source.age >= `18`}",
    "then": [
        ...
    ]
}
```

---

### `exec`

Execute nested script.

**From source:**

```json
{
    "op": "exec",
    "from": "/script",
    "merge": false
    // Replace dest (default) or merge into it
}
```

**Inline:**

```json
{
    "op": "exec",
    "actions": [
        ...
    ]
}
```

---

### `update`

Merge mapping into target.

```json
{
    "op": "update",
    "path": "/obj",
    "value": {
        "b": 2
    },
    // Or "from": "/source/obj"
    "deep": false
    // Recursive merge (default: false)
}
```

---

### `distinct`

Remove duplicates from list.

```json
{
    "op": "distinct",
    "path": "/items",
    "key": "/id"
    // Optional: compare by nested field
}
```

---

### `replace_root`

Replace entire destination.

```json
{
    "op": "replace_root",
    "value": {
        "new": "root"
    }
}
```

---

### `assert` / `assertD`

Assert value existence/equality.

```json
{
    "op": "assert",
    // Check source
    "path": "/required",
    "equals": "value"
    // Optional
}
```

```json
{
    "op": "assertD",
    // Check destination
    "path": "/expected"
}
```

---

## Extending J-Perm

### Custom Operations

Create a new `ActionHandler` and register it:

```python
from j_perm import ActionHandler, ActionNode, OpMatcher, ExecutionContext


class MyOpHandler(ActionHandler):
    def execute(self, step, ctx: ExecutionContext):
        # Your logic here
        return ctx.dest


# Register in main registry (in build_default_engine or custom factory)
registry.register(ActionNode(
    name="my_op",
    priority=10,
    matcher=OpMatcher("my_op"),
    handler=MyOpHandler(),
))
```

---

### Custom Special Constructs

Add a `SpecialFn` to the specials dict:

```python
def my_special(node, ctx):
    value = ctx.engine.process_value(node["$mySpecial"], ctx)
    return value.upper()


engine = build_default_engine(specials={
    "$ref": ref_handler,
    "$eval": eval_handler,
    "$mySpecial": my_special,
})
```

---

### Custom Stages

Create a `StageProcessor` for batch preprocessing:

```python
from j_perm import StageProcessor, StageNode, StageRegistry


class ValidateStage(StageProcessor):
    def apply(self, steps, ctx):
        # Validate/transform steps
        return steps


# Register in main pipeline stages
stages = build_default_shorthand_stages()
stages.register(StageNode(
    name="validate",
    priority=200,  # Higher = runs earlier
    processor=ValidateStage(),
))

# Use in custom engine
main_pipeline = Pipeline(stages=stages, registry=main_registry)
```

---

### Custom Casters

Provide custom casters to `TemplSubstHandler`:

```python
from j_perm import TemplSubstHandler

custom_casters = {
    "int": lambda x: int(x),
    "json": lambda x: json.loads(x),
}

handler = TemplSubstHandler(casters=custom_casters)
```

Or use the default (built-in `int`, `float`, `bool`, `str`).

---

### Custom Matchers

Implement `ActionMatcher` or `StageMatcher`:

```python
from j_perm import ActionMatcher


class PrefixMatcher(ActionMatcher):
    def __init__(self, prefix):
        self.prefix = prefix

    def matches(self, step):
        return isinstance(step, dict) and
            step.get("op", "").startswith(self.prefix)
```

---

## Advanced Topics

### Value Stabilization Loop

When handlers call `ctx.engine.process_value(value, ctx)`, the value pipeline runs repeatedly until:

1. Output equals input (stable)
2. `value_max_depth` iterations reached (default: 50)

This resolves nested templates and special constructs:

```python
# Input: {"$ref": "/path_to_template"}
# Pass 1: {"$ref": ...} → "${/nested}"
# Pass 2: "${/nested}" → "final"
# Pass 3: "final" → "final" (stable ✓)
```

---

### Hierarchical Registries

Both `StageRegistry` and `ActionTypeRegistry` support tree structures:

```python
# Group related operations
math_registry = ActionTypeRegistry()
math_registry.register(ActionNode("add", 10, AddMatcher(), AddHandler()))
math_registry.register(ActionNode("sub", 10, SubMatcher(), SubHandler()))

# Mount as sub-tree
main_registry.register_group(
    "math",
    math_registry,
    matcher=OpMatcher("math"),
    priority=50,
)
```

---

### Priority and Execution Order

**Stages:** All matching stages run in priority order (high → low).

**Actions:** First matching handler executes (unless `exclusive=False`).

**Shorthands:**

1. `AssertShorthandProcessor` (100) — extracts `~assert`, `~assertD`
2. `DeleteShorthandProcessor` (50) — extracts `~delete`
3. `AssignShorthandProcessor` (0) — fallback for all remaining keys

---

### Unescape Rules

After value stabilization, registered `UnescapeRule` callables strip escape sequences:

```python
from j_perm import UnescapeRule

# Built-in: template_unescape (strips $${ → ${, $$ → $)
# Registered at priority 0

# Add custom unescape
engine.unescape_rules.append(
    UnescapeRule(name="custom", priority=10, unescape=my_unescape_fn)
)
```

---

## API Reference

### Core Classes

```python
from j_perm import (
    # Core infrastructure
    ExecutionContext,
    ValueResolver,
    Engine,
    Pipeline,

    # Stage system
    StageProcessor,
    StageMatcher,
    StageNode,
    StageRegistry,

    # Action system
    ActionHandler,
    ActionMatcher,
    ActionNode,
    ActionTypeRegistry,

    # Middleware
    Middleware,

    # Unescape
    UnescapeRule,
)
```

### Handlers

```python
from j_perm import (
    # Value handlers
    TemplMatcher,
    TemplSubstHandler,
    SpecialMatcher,
    SpecialResolveHandler,
    ContainerMatcher,
    RecursiveDescentHandler,
    IdentityHandler,

    # Special construct functions
    ref_handler,
    eval_handler,

    # Operation handlers
    SetHandler,
    CopyHandler,
    CopyDHandler,
    DeleteHandler,
    ForeachHandler,
    IfHandler,
    ExecHandler,
    UpdateHandler,
    DistinctHandler,
    ReplaceRootHandler,
    AssertHandler,
    AssertDHandler,
)
```

### Utilities

```python
from j_perm import (
    # Matchers
    OpMatcher,
    AlwaysMatcher,

    # Resolver
    PointerResolver,

    # Shorthand stages
    AssertShorthandProcessor,
    DeleteShorthandProcessor,
    AssignShorthandProcessor,

    # Factory
    build_default_engine,
    build_default_shorthand_stages,
)
```

---

## Examples

### Example 1: Data Filtering

```python
spec = {
    "op": "foreach",
    "in": "/products",
    "do": {
        "op": "if",
        "cond": "${?source.item.price < `100`}",
        "then": {"/affordable[]": "/item"}
    }
}
```

### Example 2: Conditional Copy with Default

```python
spec = {
    "/result": {
        "$ref": "/maybe_missing",
        "$default": "not found"
    }
}
```

### Example 3: Nested Evaluation

```python
spec = {
    "/computed": {
        "$eval": [
            {"op": "set", "path": "/x", "value": "${int:/a}"},
            {"op": "set", "path": "/y", "value": "${int:/b}"},
            {"op": "replace_root", "value": "${?add(dest.x, dest.y)}"}
        ]
    }
}
```

### Example 4: Mixed Shorthands

```python
spec = {
    "~assert": {"/user/id": 123},
    "~delete": "/temp",
    "/output": "/user/name"
}
```

---

## License

MIT (or adapt to your project as needed)

---

## Contributing

Issues and pull requests welcome!
