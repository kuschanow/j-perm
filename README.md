# J-Perm

A composable JSON transformation DSL with a powerful, extensible architecture.

J-Perm lets you describe data transformations as **executable specifications** — a list of steps that can be applied to input documents. It supports
JSON Pointer addressing with slicing (arrays and strings), template interpolation with `${...}` syntax, special constructs (`$ref`, `$eval`, `$cast`), logical and comparison operators, mathematical operations, comprehensive string manipulation (11 operations), regular expressions (5 operations), and a rich set of built-in operations.

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

# Custom specials (None = use defaults: $ref, $eval, $cast, $and, $or, $not)
engine = build_default_engine(
    specials={"$ref": my_ref_handler, "$custom": my_handler},
    casters={"int": lambda x: int(x), "json": lambda x: json.loads(x)},  # Used in ${type:...} AND $cast
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

# Slices (work on lists and strings)
resolver.get("/items[1:3]", data)  # → [item1, item2] for lists
resolver.get("/text[0:5]", {"text": "hello world"})  # → "hello" for strings
resolver.get("/text[-5:]", {"text": "hello world"})  # → "world" (negative indices)

# Append notation
resolver.set("/items/-", data, "new")  # Append to list
```

**Key feature:** Unlike standard JSON Pointer, `PointerResolver` works on **any type** (scalars, lists, dicts) for root references.

#### Data Source Prefixes

J-Perm supports **prefixes** to specify which context to read from:

| Prefix | Source | Description |
|--------|--------|-------------|
| `/path` | **source** | Default - read from source context |
| `@:/path` | **dest** | Read from destination being built |
| `_:/path` | **metadata** | Read from execution metadata |

**Example: Accessing destination in templates**

```python
# Build incrementally, referencing previous values
spec = [
    {"/name": "Alice"},
    {"/greeting": "Hello, ${@:/name}!"}  # Reference dest value
]

result = engine.apply(spec, source={}, dest={})
# → {"name": "Alice", "greeting": "Hello, Alice!"}
```

**Example: Comparing source vs dest**

```python
spec = [
    {"/dest_value": "from_dest"},
    {"/comparison": "Source: ${/source_value}, Dest: ${@:/dest_value}"}
]

result = engine.apply(
    spec,
    source={"source_value": "from_source"},
    dest={}
)
# → {"dest_value": "from_dest", "comparison": "Source: from_source, Dest: from_dest"}
```

**Example: Using in conditionals**

```python
# Check destination state in conditions
spec = [
    {"/status": "ready"},
    {
        "op": "if",
        "path": "@:/status",
        "equals": "ready",
        "then": [{"/message": "System is ready"}]
    }
]
```

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

**Note:** Type casters can also be used via the `$cast` construct (see Special Constructs section).

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

#### `$cast` — Type casting

```json
{
    "$cast": {
        "value": "42",
        "type": "int"
    }
}
```

- Applies a registered type caster to a value
- `value` — the value to cast (supports templates, `$ref`, etc.)
- `type` — name of the registered caster (built-in: `int`, `float`, `bool`, `str`)
- Alternative to template syntax `${type:...}`

**Examples:**

```python
# Cast string to int
{"/age": {"$cast": {"value": "25", "type": "int"}}}

# Cast with template substitution
{"/count": {"$cast": {"value": "${/raw_count}", "type": "int"}}}

# Cast with $ref
{"/price": {"$cast": {"value": {"$ref": "/data/price"}, "type": "float"}}}

# Dynamic type selection
{"/result": {"$cast": {"value": "123", "type": "${/target_type}"}}}
```

**Custom casters:**

```python
# Define custom caster
def custom_upper(x):
    return str(x).upper()

engine = build_default_engine(casters={"upper": custom_upper})

# Use in spec
{"/name": {"$cast": {"value": "alice", "type": "upper"}}}  # → "ALICE"
```

#### `$and` — Logical AND with short-circuit

```json
{
    "$and": [
        [{"op": "assert", "path": "/x", "return": true}],
        [{"op": "copy", "from": "/y", "path": "/"}]
    ]
}
```

- Executes actions in order with empty `dest` for each
- Returns last result if all are truthy
- Short-circuits and returns first falsy result

#### `$or` — Logical OR with short-circuit

```json
{
    "$or": [
        [{"op": "assert", "path": "/x", "return": true}],
        [{"op": "copy", "from": "/y", "path": "/"}]
    ]
}
```

- Executes actions in order with empty `dest` for each
- Returns first truthy result
- Returns last result if all are falsy

#### `$not` — Logical negation

```json
{
    "$not": [{"op": "assert", "path": "/missing", "return": true}]
}
```

- Executes action with empty `dest`
- Returns logical negation of the result

#### Comparison Operators

J-Perm provides comparison operators that work with any values:

**`$gt` — Greater than**

```json
{"$gt": [10, 5]}  → true
{"$gt": ["${/age}", 18]}  → true if age > 18
```

**`$gte` — Greater than or equal**

```json
{"$gte": [10, 10]}  → true
{"$gte": [{"$ref": "/count"}, 100]}  → true if count >= 100
```

**`$lt` — Less than**

```json
{"$lt": [5, 10]}  → true
{"$lt": ["${/price}", 50]}  → true if price < 50
```

**`$lte` — Less than or equal**

```json
{"$lte": [10, 10]}  → true
{"$lte": [{"$ref": "/temperature"}, 30]}  → true if temperature <= 30
```

**`$eq` — Equal**

```json
{"$eq": [10, 10]}  → true
{"$eq": ["${/status}", "active"]}  → true if status == "active"
```

**`$ne` — Not equal**

```json
{"$ne": [10, 5]}  → true
{"$ne": ["${/role}", "admin"]}  → true if role != "admin"
```

**Usage in conditions:**

```python
spec = [
    {"/age": 25},
    {
        "op": "if",
        "cond": {"$gte": [{"$ref": "@:/age"}, 18]},
        "then": [{"/is_adult": True}],
        "else": [{"/is_adult": False}],
    },
]

result = engine.apply(spec, source={}, dest={})
# → {"age": 25, "is_adult": True}
```

**Features:**
- All operators accept exactly 2 values in a list
- Values are processed through `process_value` (support templates, `$ref`, `$cast`, etc.)
- Can be nested and combined with logical operators

#### Mathematical Operators

J-Perm provides mathematical operators with support for 1+ operands:

**`$add` — Addition**

```json
{"$add": [10]}           → 10
{"$add": [10, 5]}        → 15
{"$add": [1, 2, 3, 4]}   → 10  (1 + 2 + 3 + 4)
```

**`$sub` — Subtraction**

```json
{"$sub": [10]}           → 10
{"$sub": [10, 5]}        → 5
{"$sub": [100, 20, 10]}  → 70  ((100 - 20) - 10)
```

**`$mul` — Multiplication**

```json
{"$mul": [5]}            → 5
{"$mul": [10, 5]}        → 50
{"$mul": [2, 3, 4]}      → 24  ((2 * 3) * 4)
```

**`$div` — Division**

```json
{"$div": [10]}           → 10
{"$div": [10, 5]}        → 2.0
{"$div": [100, 2, 5]}    → 10.0  ((100 / 2) / 5)
```

**`$pow` — Exponentiation**

```json
{"$pow": [2]}            → 2
{"$pow": [2, 3]}         → 8
{"$pow": [2, 3, 2]}      → 64  ((2 ** 3) ** 2)
```

**`$mod` — Modulo**

```json
{"$mod": [10]}           → 10
{"$mod": [10, 3]}        → 1
{"$mod": [100, 7, 3]}    → 2  ((100 % 7) % 3)
```

**Nested expressions:**

```python
# Calculate: (price * quantity) + shipping
spec = {
    "/total": {
        "$add": [
            {"$mul": [{"$ref": "/price"}, {"$ref": "/quantity"}]},
            {"$ref": "/shipping"}
        ]
    }
}

# Complex: ((10 + 5) * 2) - 3 = 27
spec = {
    "/result": {
        "$sub": [
            {"$mul": [{"$add": [10, 5]}, 2]},
            3
        ]
    }
}
```

**Features:**
- Accept 1+ operands (1 operand: returns the value itself)
- 2+ operands: apply operation left-to-right
- Values are processed through `process_value` (support templates, `$ref`, `$cast`, etc.)
- Can be nested to create complex expressions
- Work seamlessly with comparison operators in conditions

#### Membership Test Operator

**`$in` — Python-style membership test**

Works with strings (substring), lists (element), and dicts (key):

```json
{"$in": ["world", "hello world"]}  → true (substring)
{"$in": [2, [1, 2, 3]]}             → true (element in list)
{"$in": ["key", {"key": "val"}]}    → true (key in dict)
```

---

### 4. String Operations

J-Perm provides comprehensive string manipulation constructs:

#### Split and Join

```python
# Split string by delimiter
{"$str_split": {"string": "a,b,c", "delimiter": ","}}  → ["a", "b", "c"]
{"$str_split": {"string": "a:b:c", "delimiter": ":", "maxsplit": 1}}  → ["a", "b:c"]

# Join array into string
{"$str_join": {"array": ["a", "b", "c"], "separator": "-"}}  → "a-b-c"
{"$str_join": {"array": [1, 2, 3], "separator": ","}}  → "1,2,3"
```

#### Slicing

```python
# Extract substring
{"$str_slice": {"string": "hello", "start": 1, "end": 4}}  → "ell"
{"$str_slice": {"string": "hello", "start": 2}}  → "llo"
{"$str_slice": {"string": "hello", "end": 3}}  → "hel"
{"$str_slice": {"string": "hello", "start": -3}}  → "llo"
```

**Note:** String slicing is also supported in JSON Pointer syntax:
```python
{"$ref": "/text[0:5]"}    # first 5 characters
{"$ref": "/text[6:]"}     # from 6th character to end
{"$ref": "/text[-5:]"}    # last 5 characters
```

#### Case Conversion

```python
{"$str_upper": "hello"}  → "HELLO"
{"$str_lower": "HELLO"}  → "hello"
```

#### Trimming

```python
# Strip whitespace (default)
{"$str_strip": "  hello  "}  → "hello"
{"$str_lstrip": "  hello  "}  → "hello  "
{"$str_rstrip": "  hello  "}  → "  hello"

# Strip specific characters
{"$str_strip": {"string": "***hello***", "chars": "*"}}  → "hello"
{"$str_lstrip": {"string": "___hello", "chars": "_"}}  → "hello"
{"$str_rstrip": {"string": "hello___", "chars": "_"}}  → "hello"
```

#### Replace

```python
{"$str_replace": {"string": "hello", "old": "ll", "new": "rr"}}  → "herro"
{"$str_replace": {"string": "aaa", "old": "a", "new": "b", "count": 2}}  → "bba"
```

#### String Checks

```python
{"$str_contains": {"string": "hello world", "substring": "world"}}  → true
{"$str_startswith": {"string": "hello", "prefix": "he"}}  → true
{"$str_endswith": {"string": "hello", "suffix": "lo"}}  → true
```

---

### 5. Regular Expressions

J-Perm supports powerful regex operations using Python's `re` module:

#### Match and Search

```python
# Check if entire string matches pattern
{"$regex_match": {"pattern": "^\\d+$", "string": "123"}}  → true
{"$regex_match": {"pattern": "^\\d+$", "string": "abc"}}  → false

# Find first occurrence
{"$regex_search": {"pattern": "\\d+", "string": "abc123def"}}  → "123"
{"$regex_search": {"pattern": "\\d+", "string": "abc"}}  → null
```

#### Find All Matches

```python
{"$regex_findall": {"pattern": "\\d+", "string": "a1b2c3"}}  → ["1", "2", "3"]
{"$regex_findall": {"pattern": "\\d+", "string": "abc"}}  → []
```

#### Replace with Regex

```python
# Simple replacement
{"$regex_replace": {"pattern": "\\d+", "replacement": "X", "string": "a1b2c3"}}  → "aXbXcX"

# With backreferences
{"$regex_replace": {
    "pattern": "(\\w+)@(\\w+)",
    "replacement": "\\1 AT \\2",
    "string": "user@domain"
}}  → "user AT domain"

# Limited replacements
{"$regex_replace": {"pattern": "\\d+", "replacement": "X", "string": "a1b2c3", "count": 2}}  → "aXbXc3"
```

#### Extract Capture Groups

```python
{"$regex_groups": {"pattern": "(\\w+)@(\\w+)", "string": "user@domain"}}  → ["user", "domain"]
{"$regex_groups": {"pattern": "(\\d+)-(\\d+)", "string": "123-456"}}  → ["123", "456"]
```

**Optional `flags` parameter:**
All regex constructs accept optional `flags` parameter (e.g., `re.IGNORECASE = 2`):
```python
{"$regex_match": {"pattern": "^hello$", "string": "HELLO", "flags": 2}}  → true
```

---

### 4. Functions and Error Handling

J-Perm supports defining reusable functions and controlled error handling.

#### `$def` — Define a function

```json
{
    "$def": "myFunction",
    "params": ["arg1", "arg2"],
    "body": [
        {"/result": "${arg1}"},
        {"/total": "${int:${arg2}}"}
    ],
    "return": "/total",
    "on_failure": [
        {"/error": "Function failed"}
    ]
}
```

- `params` — list of parameter names (optional, default: [])
- `body` — actions to execute when function is called
- `return` — path in local context to return (optional, default: entire dest)
- `on_failure` — error handler actions (optional)

**Special variable `_`:** Inside function body, the original source is accessible via `/_/path`:

```json
{
    "$def": "getFromSource",
    "body": [{"/value": {"$ref": "/_/data"}}]
}
```

When called, functions can access:
- **Parameters** via `/${param_name}`
- **Original source** via `/_/path` (source from engine.apply)
- **Current dest** via `@:/path`

Example:

```python
spec = [
    {
        "$def": "combineData",
        "params": ["userInput"],
        "body": [
            {"/input": "${userInput}"},
            {"/global": {"$ref": "/_/config"}},  # From original source
            {"/result": "Input: ${@:/input}, Config: ${@:/global/setting}"}
        ],
        "return": "/result"
    },
    {"/output": {"$func": "combineData", "args": ["test"]}}
]

result = engine.apply(
    spec,
    source={"config": {"setting": "production"}},
    dest={}
)
# → {"output": "Input: test, Config: production"}
```

#### `$func` — Call a function

```json
{
    "$func": "myFunction",
    "args": [10, 20]
}
```

- `args` — list of arguments to pass (optional, default: [])

Functions are stored in the execution context metadata and can be called multiple times within the same transformation.

#### `$raise` — Raise an error

```json
{
    "$raise": "Invalid data: ${/error_details}"
}
```

Raises a `JPermError` with the specified message. The error can be:
- Caught by `on_failure` handlers in function definitions
- Used for validation and control flow
- Combined with templates for dynamic error messages

**Example with error handling:**

```json
[
    {
        "$def": "validateAge",
        "params": ["age"],
        "body": [
            {
                "op": "if",
                "cond": {"$not": [{"op": "assert", "value": "${int:${age}}", "return": true}]},
                "then": [{"$raise": "Age must be a number"}]
            },
            {
                "op": "if",
                "cond": "${?source.age < `0`}",
                "then": [{"$raise": "Age cannot be negative"}]
            }
        ],
        "on_failure": [
            {"/validation_error": "@:/"}
        ]
    },
    {"/result": {"$func": "validateAge", "args": [25]}}
]
```

---

### 5. Shorthand Syntax

Shorthands are expanded by **priority-ordered StageProcessors** before execution.

#### `~assert`

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

**Priority order:** `~assert` (100) → `~delete` (50) → pointer/literal assignment (0)

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

**Special variable `_`:** Inside foreach body, you can access:
- **Current item** via `/${var}` (e.g., `/item`)
- **Original source** via `/_/path` (source from engine.apply)
- **Current dest** via `@:/path`

Example:

```python
# Enrich items with global config from source
spec = {
    "op": "foreach",
    "in": "/products",
    "as": "product",
    "do": {
        "/results[]": {
            "$eval": [
                {"/name": "${product/name}"},
                {"/price": "${product/price}"},
                {"/tax_rate": {"$ref": "/_/config/tax"}},  # From original source
                {"/total": "${?source.price * source.tax_rate}"}
            ]
        }
    }
}

result = engine.apply(
    spec,
    source={
        "products": [{"name": "A", "price": 100}, {"name": "B", "price": 200}],
        "config": {"tax": 1.2}
    },
    dest={}
)
# → {"results": [{"name": "A", "price": 100, "tax_rate": 1.2, "total": 120}, ...]}
```

---

### `while`

Loop while condition holds.

**Path mode:**

```json
{
    "op": "while",
    "path": "/counter",
    "equals": 0,
    // Or "exists": true
    "do": [
        ...
    ],
    "do_while": false
    // Execute at least once (default: false)
}
```

**Expression mode:**

```json
{
    "op": "while",
    "cond": "${?dest.counter < `10`}",
    "do": [
        ...
    ]
}
```

**Note:** Condition is checked against destination state. Use `do_while: true` to execute body at least once before checking condition.

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

### `assert`

Assert value existence/equality.

**Basic usage:**

```json
{
    "op": "assert",
    // Check source
    "path": "/required",
    "equals": "value"
    // Optional
}
```

**With direct value:**

```json
{
    "op": "assert",
    "value": "${?source.computed}",
    // Check computed value instead of path
    "equals": "expected"
}
```

**With return mode:**

```json
{
    "op": "assert",
    "path": "/optional",
    "return": true,
    // Return value instead of raising error
    "to_path": "/result"
    // Optional: write result to destination
}
```

- `return: true` — returns value on success, `false` on failure (instead of raising error)
- `to_path` — destination path for return value
- `value` — alternative to `path`, checks direct value

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

Casters are used in both template syntax (`${type:...}`) and the `$cast` construct.

Provide custom casters via `build_default_engine`:

```python
import json
from j_perm import build_default_engine

custom_casters = {
    "int": lambda x: int(x),
    "float": lambda x: float(x),
    "json": lambda x: json.loads(x),
    "upper": lambda x: str(x).upper(),
}

engine = build_default_engine(casters=custom_casters)

# Now you can use them in both ways:
spec = [
    {"/age": "${int:/raw_age}"},              # Template syntax
    {"/data": {"$cast": {"value": "{}", "type": "json"}}},  # $cast construct
    {"/name": {"$cast": {"value": "alice", "type": "upper"}}},
]
```

Or use the default built-in casters: `int`, `float`, `bool`, `str`.

---

### Using Construct Groups

J-Perm provides pre-organized groups of construct handlers for convenient registration:

```python
from j_perm import build_default_engine
from j_perm import (
    CORE_HANDLERS,         # $ref, $eval
    LOGICAL_HANDLERS,      # $and, $or, $not
    COMPARISON_HANDLERS,   # $gt, $gte, $lt, $lte, $eq, $ne, $in
    MATH_HANDLERS,         # $add, $sub, $mul, $div, $pow, $mod
    STRING_HANDLERS,       # All string operations (11 constructs)
    REGEX_HANDLERS,        # All regex operations (5 constructs)
    get_all_handlers,      # Function to get all handlers with casters
)

# Build engine with specific groups only
engine = build_default_engine(specials={
    **CORE_HANDLERS,
    **STRING_HANDLERS,
    **REGEX_HANDLERS,
})

# Or extend default engine with additional handlers
from j_perm.casters import BUILTIN_CASTERS

all_handlers = get_all_handlers(casters=BUILTIN_CASTERS)
engine = build_default_engine(specials={
    **all_handlers,
    "$custom": my_custom_handler,
})

# Import individual handlers
from j_perm import (
    str_split_handler,
    str_join_handler,
    regex_match_handler,
    add_handler,
    # ... etc
)
```

**Available groups:**
- `CORE_HANDLERS` — Core constructs (`$ref`, `$eval`)
- `LOGICAL_HANDLERS` — Logical operators (`$and`, `$or`, `$not`)
- `COMPARISON_HANDLERS` — Comparison operators (`$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`, `$in`)
- `MATH_HANDLERS` — Mathematical operators (`$add`, `$sub`, `$mul`, `$div`, `$pow`, `$mod`)
- `STRING_HANDLERS` — String operations (11 constructs)
- `REGEX_HANDLERS` — Regular expression operations (5 constructs)
- `ALL_HANDLERS_NO_CAST` — All handlers except `$cast`
- `get_all_handlers(casters)` — Function returning all handlers including `$cast`

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

## Async Support

J-Perm provides full support for asynchronous operations through parallel async infrastructure.

### Overview

All core components have async counterparts that work seamlessly with Python's `async`/`await`:

- **Sync pipeline** (`engine.apply()`) - for synchronous handlers
- **Async pipeline** (`engine.apply_async()`) - for async handlers and I/O operations
- **Mixed mode** - sync and async handlers can coexist in the same pipeline

### Async Base Classes

```python
from j_perm import AsyncActionHandler, AsyncStageProcessor, AsyncMiddleware

class AsyncHttpHandler(AsyncActionHandler):
    """Async handler for HTTP requests."""

    async def execute(self, step, ctx):
        url = await ctx.engine.process_value_async(step["url"], ctx)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                ctx.dest["response"] = data

        return ctx.dest
```

### Using Async Engine

```python
import asyncio
from j_perm import build_default_engine, ActionNode, OpMatcher

# Build engine as usual
engine = build_default_engine()

# Register async handler
engine.main_pipeline.registry.register(ActionNode(
    name="http",
    priority=10,
    matcher=OpMatcher("http"),
    handler=AsyncHttpHandler(),
))

# Use async apply
async def main():
    spec = [
        {"op": "http", "url": "https://api.example.com/data"},
        {"/result": "${@:/response/value}"}
    ]

    result = await engine.apply_async(spec, source={}, dest={})
    print(result)

asyncio.run(main())
```

### Async Methods

| Method | Description |
|--------|-------------|
| `engine.apply_async()` | Async version of `apply()` |
| `engine.apply_to_context_async()` | Async version of `apply_to_context()` |
| `engine.process_value_async()` | Async value stabilization |
| `engine.run_pipeline_async()` | Run named pipeline asynchronously |
| `pipeline.run_async()` | Async pipeline execution |
| `registry.run_all_async()` | Async stage execution (for `StageRegistry`) |

### Mixing Sync and Async

The async pipeline automatically handles both sync and async components:

```python
# Sync handler
class SyncSetHandler(ActionHandler):
    def execute(self, step, ctx):
        ctx.dest["sync"] = True
        return ctx.dest

# Async handler
class AsyncFetchHandler(AsyncActionHandler):
    async def execute(self, step, ctx):
        data = await fetch_data()  # async I/O
        ctx.dest["async"] = data
        return ctx.dest

# Both work in apply_async()
result = await engine.apply_async([
    {"op": "set", ...},      # sync handler
    {"op": "fetch", ...},    # async handler
], source={}, dest={})
```

### When to Use Async

Use async handlers for:

- **Network I/O** - HTTP requests, API calls, webhooks
- **Database operations** - async DB queries
- **File I/O** - async file reads/writes
- **External services** - Cloud APIs, microservices
- **Concurrent operations** - when you need to parallelize work

Sync handlers are fine for:

- **Pure transformations** - data mapping, filtering
- **Simple operations** - set, copy, delete
- **CPU-bound work** - computations without I/O

### Example: Async HTTP Handler

```python
import aiohttp
from j_perm import AsyncActionHandler, ActionNode, OpMatcher

class HttpGetHandler(AsyncActionHandler):
    """Fetch data from HTTP endpoint."""

    async def execute(self, step, ctx):
        # Process URL with template support
        url = await ctx.engine.process_value_async(step["url"], ctx)
        headers = await ctx.engine.process_value_async(
            step.get("headers", {}), ctx
        )

        # Make async HTTP request
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

        # Write result to destination
        path = await ctx.engine.process_value_async(step["path"], ctx)
        ctx.engine.resolver.set(path, ctx.dest, data)

        return ctx.dest

# Register and use
engine.main_pipeline.registry.register(ActionNode(
    name="http_get",
    priority=10,
    matcher=OpMatcher("http_get"),
    handler=HttpGetHandler(),
))

# Usage
spec = {
    "op": "http_get",
    "url": "https://api.github.com/users/${/username}",
    "headers": {"Accept": "application/json"},
    "path": "/user_data"
}

result = await engine.apply_async(spec, source={"username": "octocat"}, dest={})
```

### Async Stages and Middlewares

You can also create async stages and middlewares:

```python
from j_perm import AsyncStageProcessor, AsyncMiddleware

class AsyncValidationStage(AsyncStageProcessor):
    """Async validation of steps."""

    async def apply(self, steps, ctx):
        # Async validation logic
        await validate_steps(steps)
        return steps

class AsyncLoggingMiddleware(AsyncMiddleware):
    """Log each step asynchronously."""

    name = "async_logger"
    priority = 10

    async def process(self, step, ctx):
        await log_step(step)  # async logging
        return step
```

**Note:** Stages and middlewares remain sync by default. Only use async versions when you have actual async I/O in preprocessing/middleware logic.

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

1. `AssertShorthandProcessor` (100) — extracts `~assert`
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
    AsyncStageProcessor,  # Async version
    StageMatcher,
    StageNode,
    StageRegistry,

    # Action system
    ActionHandler,
    AsyncActionHandler,  # Async version
    ActionMatcher,
    ActionNode,
    ActionTypeRegistry,

    # Middleware
    Middleware,
    AsyncMiddleware,  # Async version

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
    make_cast_handler,  # Factory for $cast handler
    and_handler,
    or_handler,
    not_handler,

    # Comparison operators
    gt_handler,
    gte_handler,
    lt_handler,
    lte_handler,
    eq_handler,
    ne_handler,
    in_handler,

    # Mathematical operators
    add_handler,
    sub_handler,
    mul_handler,
    div_handler,
    pow_handler,
    mod_handler,

    # String operations
    str_split_handler,
    str_join_handler,
    str_slice_handler,
    str_upper_handler,
    str_lower_handler,
    str_strip_handler,
    str_lstrip_handler,
    str_rstrip_handler,
    str_replace_handler,
    str_contains_handler,
    str_startswith_handler,
    str_endswith_handler,

    # Regex operations
    regex_match_handler,
    regex_search_handler,
    regex_findall_handler,
    regex_replace_handler,
    regex_groups_handler,

    # Function handlers
    DefMatcher,
    CallMatcher,
    DefHandler,
    CallHandler,
    RaiseMatcher,
    RaiseHandler,
    JPermError,

    # Operation handlers
    SetHandler,
    CopyHandler,
    DeleteHandler,
    ForeachHandler,
    WhileHandler,
    IfHandler,
    ExecHandler,
    UpdateHandler,
    DistinctHandler,
    AssertHandler,
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

    # Casters
    BUILTIN_CASTERS,  # Built-in type casters (int, float, bool, str)

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
            {"op": "set", "path": "/y", "value": "${int:/b}"}
        ],
        "$select": "${?add(dest.x, dest.y)}"
    }
}
```

### Example 4: Mixed Shorthands

```python
spec = [
    {"~assert": {"/user/id": 123}},
    {"~delete": "/temp"},
    {"/output": "/user/name"}
]
```

### Example 5: Functions with Error Handling

```python
spec = [
    # Define a validation function with error recovery
    {
        "$def": "validateAge",
        "params": ["age"],
        "body": [
            # Validate age is a number
            {
                "op": "if",
                "cond": {"$not": [{"op": "assert", "value": "${int:${age}}", "return": True}]},
                "then": [{"$raise": "Age must be a number"}]
            },
            # Validate age is positive
            {
                "op": "if",
                "cond": "${?source.age < `0`}",
                "then": [{"$raise": "Age cannot be negative: ${age}"}]
            },
            {"/valid": True}
        ],
        "return": "/valid",
        "on_failure": [
            # Capture the error in dest
            {"/validation_failed": True},
            {"/last_error": "Validation error occurred"}
        ]
    },
    # Use function to validate user input
    {"/user_age_valid": {"$func": "validateAge", "args": [25]}},
    # Access dest values with @: prefix
    {
        "op": "if",
        "path": "@:/validation_failed",
        "equals": True,
        "then": [{"/message": "Please check your input: ${@:/last_error}"}]
    }
]
```

---

## License

MIT (or adapt to your project as needed)

---

## Contributing

Issues and pull requests welcome!
