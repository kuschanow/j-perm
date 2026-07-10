# J-Perm

A composable JSON transformation DSL with a powerful, extensible architecture.

J-Perm lets you describe data transformations as **executable specifications** — a list of steps that can be applied to input documents. It supports
JSON Pointer addressing with slicing (arrays and strings), template interpolation with `${...}` syntax, special constructs (`$ref`, `$eval`, `$cast`, `$raw`), logical and comparison operators (`$and`, `$or`, `$not`), comparison operators (6 operators plus `$in` and `$exists`), mathematical operations (6 operators), comprehensive string manipulation (11 operations), regular expressions (5 operations), user-defined functions (`$def`, `$func`, `$raise`) with loop/function/script control flow (`$break`, `$continue`, `$return`, `$exit`), error handling (`try-except-finally`), and a rich set of built-in operations — all with configurable security limits to prevent DoS attacks. Specs can be written as JSON op-dicts or in a compact [text syntax](#9-text-syntax) (the two mix freely in one list).

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

# Transformation spec using foreach and the &: prefix for the loop variable
spec = {
    "op": "foreach",
    "in": "/users",
    "as": "item",
    "do": {
        "op": "if",
        "cond": "${?args.item.age >= `18`}",
        "then": {"/adults[]": "&:/item"},
    },
}

result = engine.apply(spec, source=source, dest={})
# → {"adults": [{"name": "Bob", "age": "22"}]}
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

# Default engine with all built-ins and default security limits
engine = build_default_engine()

# Custom specials (None = use defaults: $ref, $eval, $cast, $and, $or, $not, comparison, math, string, regex)
engine = build_default_engine(
    specials={"$ref": my_ref_handler, "$custom": my_handler},
    casters={"int": lambda x: int(x), "json": lambda x: json.loads(x)},  # Used in ${type:...} AND $cast
    jmes_options=jmespath.Options(custom_functions=CustomFunctions())
)

# Custom security limits (see Security and Limits section)
engine = build_default_engine(
    max_operations=10_000,
    max_function_recursion_depth=50,
    max_loop_iterations=1_000,
    regex_timeout=1.0,
    pow_max_exponent=100,
    # ... see factory.py for all available limits
)

# Logging / debugging (see Logging and Debugging section)
engine = build_default_engine(
    trace_logging=True,      # DEBUG-log every executed step
    trace_repr_max=None,     # show steps without truncation (default: 200)
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

`apply` builds the execution context for you and swallows a `$exit`
[`ExitSignal`](#exit--terminate-the-whole-script-early) as a clean early finish.
If you need to bring your **own** context (e.g. to thread `metadata` / `temp`
side channels), the engine offers two families:

| Method | Owns context? | `$exit` behavior |
|---|---|---|
| `engine.apply(spec, *, source, dest)` | builds it | swallowed (clean finish) |
| `engine.apply_to_context(spec, ctx)` | you provide | **propagates** `ExitSignal` (also the internal nested-body runner) |
| `engine.run_script_in_context(spec, ctx)` | you provide | swallowed (clean finish) |

Each has a compiled counterpart (`apply_compiled` / `apply_compiled_to_context` /
`run_compiled_in_context`) and an `_async` twin. Use `run_script_in_context` when
you drive a whole script through your own context and want `$exit` to stop
cleanly; use `apply_to_context` only for nested/embedded runs where `$exit` must
keep propagating outward (or catch `ExitSignal` yourself).

---

## Security and Limits

J-Perm includes comprehensive protection against DoS attacks through configurable limits. All limits can be customized via `build_default_engine()` parameters.

### Global Limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_operations` | 1,000,000 | Maximum total operations across entire transformation |
| `max_function_recursion_depth` | 100 | Maximum depth for recursive function calls |

**Example: Preventing infinite recursion**

```python
engine = build_default_engine(max_function_recursion_depth=50)

# This will raise RuntimeError if recursion exceeds 50 levels
spec = [
    {"$def": "factorial", "params": ["n"], "body": [
        {"op": "if", "cond": {"$eq": [{"$ref": "&:/n"}, 0]},
         "then": [{"/result": 1}],
         "else": [{"/result": {"$mul": [
             {"$ref": "&:/n"},
             {"$func": "factorial", "args": [{"$sub": [{"$ref": "&:/n"}, 1]}]}
         ]}}]}
    ], "return": "/result"},
    {"/output": {"$func": "factorial", "args": [100]}}  # Too deep!
]
```

### Loop and Iteration Limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_loop_iterations` | 10,000 | Maximum iterations for `while` loops |
| `max_foreach_items` | 100,000 | Maximum items to process in `foreach` |

**Example: Preventing infinite loops**

```python
engine = build_default_engine(max_loop_iterations=1000)

# This will raise RuntimeError if loop exceeds 1000 iterations
spec = {
    "op": "while",
    "cond": {"$lt": [{"$ref": "@:/counter"}, 999999]},  # Never stops!
    "do": [{"/counter": {"$add": [{"$ref": "@:/counter"}, 1]}}]
}
```

### Mathematical Operation Limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pow_max_base` | 1,000,000 | Maximum base value for `$pow` |
| `pow_max_exponent` | 1,000 | Maximum exponent value for `$pow` |
| `mul_max_operand` | 1,000,000,000 | Maximum numeric operand in `$mul` |
| `mul_max_string_result` | 1,000,000 | Maximum string length from `$mul` (e.g., `"x" * n`) |
| `add_max_number_result` | 1e15 | Maximum numeric result from `$add` |
| `add_max_string_result` | 100,000,000 | Maximum string length from `$add` (concatenation) |
| `sub_max_number_result` | 1e15 | Maximum numeric result from `$sub` |

**Example: Preventing CPU exhaustion**

```python
engine = build_default_engine(
    pow_max_base=1000,
    pow_max_exponent=10
)

# This will raise ValueError: exponent exceeds limit
spec = {"/result": {"$pow": [2, 1000]}}  # 2^1000 would consume massive CPU

# This will raise ValueError: base exceeds limit
spec = {"/result": {"$pow": [999999, 2]}}
```

### String Operation Limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `str_max_split_results` | 100,000 | Maximum results from `$str_split` |
| `str_max_join_result` | 10,000,000 | Maximum length of `$str_join` result |
| `str_max_replace_result` | 10,000,000 | Maximum length of `$str_replace` result |

**Example: Preventing memory exhaustion**

```python
engine = build_default_engine(str_max_split_results=1000)

# This will raise ValueError if split produces more than 1000 results
spec = {"/words": {"$str_split": {"string": "${/large_text}", "delimiter": " "}}}
```

### Regex Protection (ReDoS Prevention)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `regex_timeout` | 2.0 | Timeout in seconds for regex operations |
| `regex_allowed_flags` | None | Bitmask of allowed regex flags (None = default safe flags: IGNORECASE, MULTILINE, DOTALL, VERBOSE, ASCII; -1 = all flags allowed) |

**Example: Preventing ReDoS attacks**

```python
engine = build_default_engine(regex_timeout=1.0)

# This will raise TimeoutError if regex takes more than 1 second
spec = {
    "/result": {
        "$regex_match": {
            "pattern": "(a+)+b",  # Catastrophic backtracking pattern
            "string": "aaaaaaaaaaaaaaaaaaaaaaaac"  # No match, tries all combinations
        }
    }
}
```

**Restricting regex flags:**

```python
import re

# Only allow case-insensitive and multiline flags
engine = build_default_engine(
    regex_allowed_flags=re.IGNORECASE | re.MULTILINE
)

# This will raise ValueError: prohibited regex flags
spec = {
    "/result": {
        "$regex_match": {
            "pattern": "test",
            "string": "TEST",
            "flags": re.DOTALL  # Not allowed!
        }
    }
}

# Allow all flags (not recommended for untrusted input)
engine = build_default_engine(regex_allowed_flags=-1)
```

### Customizing Limits

All limits can be configured when building the engine:

```python
from j_perm import build_default_engine

# Conservative limits for untrusted input
secure_engine = build_default_engine(
    max_operations=10_000,
    max_function_recursion_depth=10,
    max_loop_iterations=100,
    max_foreach_items=1_000,
    regex_timeout=0.5,
    pow_max_exponent=100,
    str_max_join_result=100_000,
)

# Relaxed limits for trusted environments
permissive_engine = build_default_engine(
    max_operations=10_000_000,
    max_function_recursion_depth=1000,
    max_loop_iterations=1_000_000,
    regex_timeout=10.0,
)
```

**Best practices:**
- Use **conservative limits** when processing untrusted user input
- Use **permissive limits** for internal data transformations
- **Monitor** `max_operations` counter to detect suspicious activity
- **Test** your transformations with realistic data sizes
- **Tune** limits based on your specific use case

---

## Logging and Debugging

J-Perm uses Python's standard `logging` module under the logger name **`j_perm`**.

### Error Logging (Language Call Stack)

When an unhandled exception escapes `Engine.apply()`, j-perm automatically logs the **language-level call stack** at `ERROR` level — showing exactly which operations were executing when the error occurred, without Python internals.

```python
import logging
logging.basicConfig(level=logging.ERROR)

engine = build_default_engine()

engine.apply(
    spec=[
        {"op": "foreach", "in": "/users", "as": "user", "do": [
            {"op": "if", "cond": True, "then": [
                {"op": "set", "path": "/result/-", "value": {"$ref": "/missing/path"}}
            ]}
        ]}
    ],
    source={"users": ["Alice"]},
    dest={},
)
```

Output:
```
ERROR j_perm: j-perm execution failed: KeyError: 'missing'
Language call stack (innermost last):
  #1   {'op': 'foreach', 'in': '/users', 'as': 'user', 'do': [1 items]}
  #2   {'op': 'if', 'cond': True, 'then': [1 items]}
  #3   {'op': 'set', 'path': '/result/-', 'value': {'$ref': '/missing/path'}}
```

**Important:** Errors caught by `{"op": "try", ...}` inside the spec are **not logged** — only errors that propagate all the way out of `apply()` appear in the log. Control flow signals (`$break`, `$continue`, `$return`, `$exit`) are never treated as errors.

The call stack is also attached to the exception itself for programmatic access:

```python
try:
    engine.apply(spec, source=src, dest={})
except Exception as e:
    stack = getattr(e, "_j_perm_lang_stack", None)
    if stack:
        for i, frame in enumerate(stack, 1):
            print(f"  #{i} {frame}")
```

### Trace Logging (Full Execution Log)

To log every step as it executes — even without errors — enable `trace_logging`:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

engine = build_default_engine(trace_logging=True)

engine.apply(
    spec=[
        {"op": "set", "path": "/name", "value": "Alice"},
        {"op": "foreach", "in": "/tags", "as": "tag", "do": [
            {"op": "set", "path": "/out/-", "value": {"$ref": "&:/tag"}}
        ]},
    ],
    source={"tags": ["x", "y"]},
    dest={},
)
```

Output (each step indented by nesting depth):
```
DEBUG j_perm: → {'op': 'set', 'path': '/name', 'value': 'Alice'}
DEBUG j_perm: → {'op': 'foreach', 'in': '/tags', 'as': 'tag', 'do': [1 items]}
DEBUG j_perm:   → {'op': 'set', 'path': '/out/-', 'value': {'$ref': '&:/tag'}}
DEBUG j_perm:   → {'op': 'set', 'path': '/out/-', 'value': {'$ref': '&:/tag'}}
```

### Controlling Step Representation Length

By default, each step is truncated to 200 characters in the call stack and trace output. Use `trace_repr_max` to change this:

```python
# Increase the limit
engine = build_default_engine(trace_logging=True, trace_repr_max=500)

# Disable truncation — show every step in full
engine = build_default_engine(trace_logging=True, trace_repr_max=None)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trace_logging` | `False` | Emit `DEBUG` log for every executed step |
| `trace_repr_max` | `200` | Max characters per step representation. `None` = no limit |

### Value Resolution Tracing

To see how each value is resolved through the value pipeline (template substitution, `$ref`, `$cast`, etc.), enable the `j_perm.values` sub-logger at `DEBUG` level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

engine = build_default_engine(trace_logging=True)  # also enable step trace for full picture

engine.apply(
    spec=[{"op": "set", "path": "/greeting", "value": "Hello, ${/name}!"}],
    source={"name": "Alice"},
    dest={},
)
```

Output — `j_perm` shows the step, `j_perm.values` shows each transformation:
```
DEBUG j_perm:        → {'op': 'set', 'path': '/greeting', 'value': 'Hello, ${/name}!'}
DEBUG j_perm.values:   'Hello, ${/name}!' → 'Hello, Alice!'
```

Value tracing is independent of `trace_logging` — you can enable it alone:

```python
import logging

# Enable only value resolution tracing, suppress step-level trace
logging.getLogger("j_perm.values").setLevel(logging.DEBUG)
logging.getLogger("j_perm").setLevel(logging.ERROR)  # suppress step trace
```

Each line shows one stabilization pass: `input → output`. Multi-step resolution (e.g., `$ref` returning a template that itself gets substituted) appears as multiple lines, indented to the current call depth.

### Invoking a Named Pipeline

`engine.run_pipeline(name, spec, ctx)` runs a registered pipeline **over the given context, as-is** — no sub-context is created and nothing is deep-copied. The result lands in `ctx.dest`, and the same (now-mutated) context is returned for ergonomic chaining and cyclic/fix-point drivers:

```python
result = engine.run_pipeline("normalize", spec, ctx).dest
```

The caller owns all context preparation. To protect the caller's document from a named pipeline, prepare an isolated context first:

```python
sub = ctx.copy(deepcopy_dest=True)        # isolated copy of dest
engine.run_pipeline("normalize", spec, sub)
result = sub.dest
```

> **Migration note (behavior change):** previous versions auto-isolated the call — they ran the pipeline on a deep copy of `dest` and returned that copy's `dest`. `run_pipeline`/`run_pipeline_async` now pass the given context straight through and return the context (not the `dest` value). For the old behavior, pass `ctx.copy(deepcopy_dest=True)` and read `.dest` from the returned context.

### Named Pipeline Tracing

Each named pipeline gets its own logger: **`j_perm.pipeline.<name>`**. This lets you turn on tracing for all pipelines at once or zoom in on a specific one.

```python
import logging

# All named pipelines
logging.getLogger("j_perm.pipeline").setLevel(logging.DEBUG)

# Only the "normalize" pipeline
logging.getLogger("j_perm.pipeline.normalize").setLevel(logging.DEBUG)

# Silence a specific pipeline while keeping others
logging.getLogger("j_perm.pipeline.verbose_one").setLevel(logging.WARNING)
```

To produce step-level output inside a named pipeline, create it with `track_execution=True`:

```python
from j_perm import Pipeline, ActionTypeRegistry, ActionNode

my_reg = ActionTypeRegistry()
# ... register handlers ...
my_pipeline = Pipeline(registry=my_reg, track_execution=True)
engine.register_pipeline("normalize", my_pipeline)
```

When `run_pipeline("normalize", ...)` is called, the pipeline's logger emits a `→ [pipeline:normalize]` entry, and if `track_execution=True`, each step follows indented relative to the caller's depth:

```
DEBUG j_perm: → {'op': 'foreach', 'in': '/items', 'as': 'item', 'do': [1 items]}
DEBUG j_perm.pipeline.normalize:   → [pipeline:normalize]
DEBUG j_perm.pipeline.normalize:   → {'op': 'set', 'path': '/value', 'value': ...}
```

On error, the call stack (including both the outer context and the pipeline's own steps) is logged at `ERROR` level to `j_perm.pipeline.<name>`.

### Logger Hierarchy

J-Perm uses four loggers, all configurable independently via Python's standard `logging` module:

| Logger | Level | When active |
|--------|-------|-------------|
| `j_perm` | `ERROR` | Unhandled error — logs language call stack |
| `j_perm` | `DEBUG` | Step trace (requires `trace_logging=True` on engine) |
| `j_perm.values` | `DEBUG` | Value resolution steps in `process_value` |
| `j_perm.pipeline.<name>` | `ERROR` | Named pipeline error — logs call stack |
| `j_perm.pipeline.<name>` | `DEBUG` | Named pipeline step trace (requires `track_execution=True` on pipeline) |

All `j_perm.pipeline.*` loggers are children of `j_perm.pipeline`, which is itself a child of `j_perm` — so the standard Python logger hierarchy applies:

```python
import logging

# Everything from j-perm (step trace + value trace + all pipeline traces)
logging.getLogger("j_perm").setLevel(logging.DEBUG)

# Only errors, no trace noise
logging.getLogger("j_perm").setLevel(logging.ERROR)

# Suppress all j-perm logging
logging.getLogger("j_perm").setLevel(logging.CRITICAL)

# Step trace only, no value noise
logging.getLogger("j_perm").setLevel(logging.DEBUG)
logging.getLogger("j_perm.values").setLevel(logging.WARNING)

# All named pipeline traces, but not main-pipeline step trace
logging.getLogger("j_perm").setLevel(logging.WARNING)
logging.getLogger("j_perm.pipeline").setLevel(logging.DEBUG)

# Only one specific pipeline
logging.getLogger("j_perm.pipeline").setLevel(logging.WARNING)
logging.getLogger("j_perm.pipeline.normalize").setLevel(logging.DEBUG)
```

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

J-Perm supports **prefixes** to specify which context to read from or write to:

| Prefix | Source | Description |
|--------|--------|-------------|
| `/path` or `_:/path` | **source** | Read from the immutable source document |
| `@:/path` | **dest** | Read/write from the destination being built |
| `&:/path` | **args** | Read from `temp_read_only` — function arguments, loop variables, error info |
| `!:/path` | **temp** | Read/write `temp` — mutable scratch space, not included in the final output |

The `!:` prefix supports both reading and writing. Values written to `temp` never appear in the final result, making it suitable for intermediate state. Use `${str:!:/path}` to force string coercion when needed.

The `&:` prefix is the standard way to access:
- **Function parameters** inside `$def` bodies
- **Loop variables** inside `foreach` `do` blocks
- **Error info** (`_error_message`, `_error_type`) inside `try` `except` blocks

**Example: Accessing dest in templates**

```python
# Build incrementally, referencing previous values
spec = [
    {"/name": "Alice"},
    {"/greeting": "Hello, ${@:/name}!"}  # Reference dest value
]

result = engine.apply(spec, source={}, dest={})
# → {"name": "Alice", "greeting": "Hello, Alice!"}
```

**Example: Function parameters via &:**

```python
spec = [
    {
        "$def": "greet",
        "params": ["name"],
        "body": [{"/msg": "Hello, ${&:/name}!"}],
        "return": "/msg",
    },
    {"/result": {"$func": "greet", "args": ["World"]}},
]

result = engine.apply(spec, source={}, dest={})
# → {"result": "Hello, World!"}
```

**Example: Loop variable via &:**

```python
spec = {
    "op": "foreach",
    "in": "/items",
    "as": "item",
    "do": {"/out[]": "&:/item"},
}

result = engine.apply(spec, source={"items": [1, 2, 3]}, dest={})
# → {"out": [1, 2, 3]}
```

---

### 2. Template Interpolation (`${...}`)

Templates are resolved by `TemplSubstHandler` in the value pipeline.

#### JSON Pointer lookup

```python
"${/user/name}"     # → Resolve pointer from source
"${@:/total}"       # → Read from dest
"${&:/param_name}"  # → Read function argument / loop variable
"${!:/scratch}"     # → Read from temp scratch space
"${_:/user/name}"   # → Same as ${/user/name} (source alias)
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
"${?args.item.age >= `18`}"              # → Query function arg / loop variable
"${?temp.scratch}"                       # → Query temp scratch space
```

**Built-in JMESPath functions:** `add(a, b)`, `subtract(a, b)`

**JMESPath data namespaces:**

| Namespace | Context field | Description |
|-----------|---------------|-------------|
| `source.*` | `ctx.source` | Source document |
| `dest.*` | `ctx.dest` | Destination being built |
| `args.*` | `ctx.temp_read_only` | Function args, loop vars, error info |
| `temp.*` | `ctx.temp` | Mutable scratch space |

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

- Resolves pointer from **source** context (supports all prefixes: `@:`, `&:`, `!:`, `_:`)
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

#### `$raw` — Return a literal without processing

`$raw` has two forms:

**Wrapper construct** — returns the value as-is, preventing all value-pipeline evaluation:

```json
{"$raw": {"$ref": "/not/evaluated"}}
{"$raw": "hello ${not_substituted}"}
{"$raw": [{"$ref": "/a"}, {"$ref": "/b"}]}
```

The wrapped value is never passed through template substitution, `$ref` resolution, or any other pipeline stage. Use this to store construct-shaped data as a literal.

**Flag on any construct** — add `"$raw": true` to stop the stabilisation loop after the construct resolves:

```json
{"$ref": "/path", "$raw": true}
{"$func": "myFunc", "$raw": true}
{"$add": [1, 2], "$raw": true}
```

Without the flag, `process_value` keeps iterating until the result stabilises — so if `$ref` returns a value that itself contains a `$ref`, that too will be resolved. With `"$raw": true`, the loop stops after the first resolution and returns the result as-is.

**Example — preventing chain resolution:**

```python
# source["/a"] contains another construct
source = {"a": {"$ref": "/b"}, "b": "final"}

# Without $raw: True — both hops resolved
spec = {"/result": {"$ref": "/a"}}
# → {"result": "final"}

# With $raw: True — only first hop resolved
spec = {"/result": {"$ref": "/a", "$raw": True}}
# → {"result": {"$ref": "/b"}}
```

**Example — storing a construct as a literal:**

```python
spec = [
    # Store a construct literally (not evaluated)
    {"/template": {"$raw": {"$ref": "/data"}}},
    # Later retrieve it — still unevaluated
    {"/copy": {"$ref": "@:/template", "$raw": True}},
]
result = engine.apply(spec, source={"data": "value"}, dest={})
# → {"template": {"$ref": "/data"}, "copy": {"$ref": "/data"}}
```

#### `$and` — Logical AND with short-circuit

```json
{
    "$and": [
        {"$ref": "/x"},
        {"$gt": [{"$ref": "/y"}, 10]},
        {"$eq": [{"$ref": "/status"}, "active"]}
    ]
}
```

- Processes values in order through value pipeline
- Returns last result if all are truthy
- Short-circuits and returns first falsy result

**Example:**

```python
# Check multiple conditions
spec = {
    "/is_valid": {
        "$and": [
            {"$ref": "/user/name"},           # truthy if name exists
            {"$gte": [{"$ref": "/user/age"}, 18]},  # age >= 18
            {"$in": ["admin", {"$ref": "/user/roles"}]}  # has admin role
        ]
    }
}
```

#### `$or` — Logical OR with short-circuit

```json
{
    "$or": [
        {"$ref": "/x"},
        {"$ref": "/y"},
        {"$ref": "/z"}
    ]
}
```

- Processes values in order through value pipeline
- Returns first truthy result
- Returns last result if all are falsy

**Example:**

```python
# Provide fallback values
spec = {
    "/display_name": {
        "$or": [
            {"$ref": "/user/preferred_name"},
            {"$ref": "/user/full_name"},
            {"$ref": "/user/email"},
            "Unknown User"
        ]
    }
}
```

#### `$not` — Logical negation

```json
{
    "$not": {"$ref": "/disabled"}
}
```

- Processes value through value pipeline
- Returns logical negation of the result

**Example:**

```python
# Negate condition
spec = {
    "/is_enabled": {
        "$not": {"$ref": "/settings/disabled"}
    }
}
```

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

#### Membership and Existence Operators

**`$in` — Python-style membership test**

Works with strings (substring), lists (element), and dicts (key):

```json
{"$in": ["world", "hello world"]}  → true (substring)
{"$in": [2, [1, 2, 3]]}             → true (element in list)
{"$in": ["key", {"key": "val"}]}    → true (key in dict)
```

**`$exists` — Check if a path resolves**

Returns `true` if the pointer can be resolved without error, `false` otherwise.
Supports all context prefixes (`@:`, `&:`, `!:`, `_:`, or plain `/`).

```json
{"$exists": "/user/name"}    → true if source has user.name
{"$exists": "@:/result"}     → true if dest has /result
{"$exists": "&:/param"}      → true if arg named 'param' was passed to the function
```

**Example — conditional processing:**

```python
spec = {
    "op": "if",
    "cond": {"$exists": "/optional_field"},
    "then": [{"/result": "${/optional_field}"}],
    "else": [{"/result": "default"}],
}
```

**Example — template path:**

```python
{"/ok": {"$exists": "/user/${/field_name}"}}
```

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

**`$round` — Rounding**

Rounds a numeric value to a given precision. Supports negative `ndigits` for rounding to tens, hundreds, etc., and a `mode` parameter to control the direction.

```json
{"$round": 3.7}                                                    → 4
{"$round": {"value": 3.14159, "ndigits": 2}}                       → 3.14
{"$round": {"value": 3.141, "ndigits": 2, "mode": "ceil"}}         → 3.15
{"$round": {"value": 3.149, "ndigits": 2, "mode": "floor"}}        → 3.14
{"$round": {"value": 1234, "ndigits": -2}}                         → 1200
{"$round": {"value": 1201, "ndigits": -2, "mode": "ceil"}}         → 1300
{"$round": {"value": "${/price}", "ndigits": 2, "mode": "ceil"}}   → price rounded up
```

- Simple form `{"$round": <value>}` — rounds to the nearest integer (no `ndigits`)
- Dict form `{"$round": {"value": <value>, "ndigits": <int>, "mode": <str>}}`
- `ndigits` — decimal places (default: `None`, i.e. nearest integer). Negative values: −1 → tens, −2 → hundreds, etc.
- `mode` (default `"round"`) — rounding direction:
  - `"round"` — standard rounding (Python `round()`, banker's rounding — round half to even)
  - `"ceil"` — always round up (`math.ceil`)
  - `"floor"` — always round down (`math.floor`)

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

J-Perm supports powerful regex operations backed by the `regex` module (with an execution timeout and a configurable allow-list of flags):

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

**Named groups (`named` parameter):**
Set `named: true` to return a `{name: value}` dict of named `(?P<name>...)` groups
instead of a positional list (empty dict when nothing matches):
```python
{"$regex_groups": {
    "pattern": "(?P<user>\\w+)@(?P<domain>\\w+)",
    "string": "user@domain",
    "named": true
}}  → {"user": "user", "domain": "domain"}
```

**Optional `flags` parameter:**
All regex constructs accept optional `flags` parameter (e.g., `re.IGNORECASE = 2`):
```python
{"$regex_match": {"pattern": "^hello$", "string": "HELLO", "flags": 2}}  → true
```

---

### 5b. Collections and Numbers

Pure, deterministic value-level constructs for working with lists, dicts, and
numbers. Every parameter is resolved through the value pipeline, so any of them
may itself be a `${...}` template, a `$ref`, or a nested construct.

#### Size, keys, and values

```python
{"$len": [1, 2, 3]}             → 3      # length of a list, dict, or string
{"$len": {"a": 1, "b": 2}}      → 2
{"$keys": {"a": 1, "b": 2}}     → ["a", "b"]
{"$values": {"a": 1, "b": 2}}   → [1, 2]
{"$items": {"a": 1, "b": 2}}    → [["a", 1], ["b", 2]]
{"$type": 42}                   → "number"   # string/number/bool/list/dict/null
```

#### Reordering and reshaping

```python
{"$reverse": [1, 2, 3]}                             → [3, 2, 1]   # list or string
{"$slice": {"array": [1, 2, 3, 4, 5], "start": 1, "end": 4}}  → [2, 3, 4]
{"$slice": {"array": [1, 2, 3, 4, 5], "step": 2}}             → [1, 3, 5]
{"$flatten": [[1, 2], [3, 4]]}                      → [1, 2, 3, 4]
{"$flatten": {"array": [1, [2, [3]]], "depth": -1}}  → [1, 2, 3]  # -1 = fully flatten
```

#### Sorting and de-duplication

```python
{"$sort": [3, 1, 2]}                                       → [1, 2, 3]
{"$sort": {"array": [3, 1, 2], "reverse": true}}          → [3, 2, 1]
{"$sort": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}   → [{"n": 1}, {"n": 3}]
{"$unique": [1, 2, 2, 3, 1]}                              → [1, 2, 3]
{"$unique": {"array": [{"id": 1}, {"id": 1}], "key": "/id"}}  → [{"id": 1}]
```

#### Numeric aggregates and scalar math

```python
{"$sum": [1, 2, 3]}     → 6
{"$avg": [2, 4, 6]}     → 4.0
{"$min": [3, 1, 2]}     → 1
{"$max": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}  → {"n": 3}
{"$abs": -5}            → 5
{"$floor": 3.7}        → 3
{"$ceil": 3.2}         → 4
```

#### `$map` / `$filter` — transform each element

Each element is bound to a variable (default `item`) and referenced with the
`&:` prefix. `$map` returns the evaluated expression per element; `$filter`
keeps elements whose condition is truthy.

```python
{"$map": {"in": [1, 2, 3], "as": "n", "expr": {"$mul": ["${&:/n}", 2]}}}
  → [2, 4, 6]

{"$filter": {"in": [1, 2, 3, 4], "as": "n", "cond": {"$gt": ["${&:/n}", 2]}}}
  → [3, 4]
```

A dict passed to `in` is iterated as `[key, value]` pairs.

---

### 6. Functions and Error Handling

J-Perm supports defining reusable functions and controlled error handling.

#### `$def` — Define a function

```json
{
    "$def": "myFunction",
    "params": ["arg1", "arg2"],
    "body": [
        {"/result": "${&:/arg1}"},
        {"/total": "${int:${&:/arg2}}"}
    ],
    "return": "/total",
    "context": "copy",
    "on_failure": [
        {"/error": "Function failed"}
    ]
}
```

- `params` — list of parameter names (optional, default: `[]`)
- `body` — actions to execute when function is called
- `return` — path in local context to return (optional, default: entire dest); superseded by `$return` if used inside the body
- `context` — how the function's dest is initialized (see below)
- `on_failure` — error handler actions (optional)

**Accessing parameters:**

Inside the function body, parameters are available via the `&:` prefix:

```python
spec = [
    {
        "$def": "greet",
        "params": ["name"],
        "body": [{"/msg": "Hello, ${&:/name}!"}],
        "return": "/msg",
    },
    {"/result": {"$func": "greet", "args": ["World"]}},
]
# → {"result": "Hello, World!"}
```

**Accessing original source:**

The original source document is always accessible via the plain `/` pointer (or `_:` alias):

```python
spec = [
    {
        "$def": "getConfig",
        "body": [{"/cfg": {"$ref": "/config/key"}}],
        "return": "/cfg",
    },
    {"/result": {"$func": "getConfig"}},
]

result = engine.apply(spec, source={"config": {"key": "production"}}, dest={})
# → {"result": "production"}
```

**`context` parameter — dest initialization mode:**

| Value | Behavior |
|-------|----------|
| `"copy"` (default) | Function body operates on a **deep copy** of the caller's `dest`. Mutations stay local. |
| `"new"` | Function body starts with an **empty** `dest = {}`. Cannot see the caller's dest. |
| `"shared"` | Function body operates on the **same** dest object as the caller. Mutations are visible to the caller. |

```python
# context: "copy" (default) — isolated
spec = [
    {"$def": "f", "body": [{"/internal": 99}]},
    {"/result": {"$func": "f"}},
]
# "internal" does NOT appear at the top level of the outer dest

# context: "new" — fresh slate
spec = [
    {"/outer": "hello"},
    {
        "$def": "f",
        "context": "new",
        "body": [{"/saw_outer": {"$exists": "@:/outer"}}],
        "return": "/saw_outer",
    },
    {"/result": {"$func": "f"}},
]
# → {"outer": "hello", "result": false}  (function can't see /outer)

# context: "shared" — direct mutation
spec = [
    {"$def": "f", "context": "shared", "body": [{"/shared_key": True}]},
    {"$func": "f"},
]
# → {"shared_key": true}  (mutation visible in outer dest)
```

#### `$func` — Call a function

```json
{
    "$func": "myFunction",
    "args": [10, 20]
}
```

- `args` — list of arguments to pass (optional, default: `[]`)

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

```python
spec = [
    {
        "$def": "validateAge",
        "params": ["age"],
        "body": [
            {
                "op": "if",
                "cond": {"$lt": [{"$ref": "&:/age"}, 0]},
                "then": [{"$raise": "Age cannot be negative"}]
            },
            {"/valid": True}
        ],
        "return": "/valid",
        "on_failure": [{"/validation_failed": True}]
    },
    {"/result": {"$func": "validateAge", "args": [25]}}
]
```

---

### 7. Loop Control Flow

`$break`, `$continue`, `$return`, and `$exit` are **control flow commands** for interrupting loops, functions, and the whole script.  They are top-level actions (registered in the main pipeline) and work from anywhere inside a loop or function body — including inside nested `if`, `try`, or even other loops.

#### `$break` — Exit a loop

```json
{"$break": null}
```

Stops the innermost `foreach` or `while` loop immediately.  Any changes made to `dest` **before** `$break` in the current iteration are preserved.

```python
spec = {
    "op": "foreach",
    "in": "/items",
    "as": "item",
    "do": [
        {
            "op": "if",
            "cond": {"$eq": [{"$ref": "&:/item"}, "stop"]},
            "then": [{"$break": None}],
        },
        {"/result[]": "&:/item"},
    ],
}

result = engine.apply(spec, source={"items": ["a", "b", "stop", "c"]}, dest={"result": []})
# → {"result": ["a", "b"]}
```

#### `$continue` — Skip to the next iteration

```json
{"$continue": null}
```

Skips the remaining actions in the current iteration and moves to the next element (`foreach`) or re-evaluates the condition (`while`).  Changes made **before** `$continue` are preserved.

```python
spec = {
    "op": "foreach",
    "in": "/numbers",
    "as": "n",
    "do": [
        {
            "op": "if",
            "cond": {"$eq": [{"$mod": [{"$ref": "&:/n"}, 2]}, 0]},
            "then": [{"$continue": None}],   # skip even numbers
        },
        {"/odds[]": "&:/n"},
    ],
}

result = engine.apply(spec, source={"numbers": [1, 2, 3, 4, 5]}, dest={"odds": []})
# → {"odds": [1, 3, 5]}
```

#### `$return` — Return a value from a function

```json
{"$return": <value>}
```

Exits the current function immediately, returning `<value>` as the function result.  The value is evaluated through the value pipeline (supports `$ref`, templates, constructs).  Use `null` to return `None`.

This **supersedes** the `"return": "/path"` parameter in `$def` when you need to return from multiple points in the body (early return, return from inside a loop, etc.).

```python
spec = [
    {
        "$def": "sign",
        "params": ["x"],
        "body": [
            {
                "op": "if",
                "cond": {"$gt": [{"$ref": "&:/x"}, 0]},
                "then": [{"$return": "positive"}],
            },
            {
                "op": "if",
                "cond": {"$lt": [{"$ref": "&:/x"}, 0]},
                "then": [{"$return": "negative"}],
            },
            {"$return": "zero"},
        ],
    },
    {"/result": {"$func": "sign", "args": [-3]}},
]

result = engine.apply(spec, source={}, dest={})
# → {"result": "negative"}
```

**Early return from inside a loop:**

```python
spec = [
    {
        "$def": "find_first",
        "params": ["items", "target"],
        "body": [
            {
                "op": "foreach",
                "in": "&:/items",
                "as": "item",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, {"$ref": "&:/target"}]},
                        "then": [{"$return": {"$ref": "&:/item"}}],
                    },
                ],
            },
            {"$return": None},   # not found
        ],
    },
    {"/found": {"$func": "find_first", "args": [["a", "b", "c"], "b"]}},
]

result = engine.apply(spec, source={}, dest={})
# → {"found": "b"}
```

#### `$exit` — Terminate the whole script early

```json
{"$exit": null}
```

Stops the **entire** script immediately, without raising an error.  Whatever has been written to the destination so far is preserved and returned as the final result.  The value is ignored — only the presence of the `$exit` key matters.

Unlike `$break` / `$continue` (which affect only the innermost loop) and `$return` (which exits only the current function), `$exit` unwinds *everything* — loops, `if` / `try` blocks, functions and nested `exec` / named pipelines — up to the top-level `apply()` call.  `try` `finally` blocks still run on the way out, and a function's `on_failure` is **not** triggered (an `$exit` is not an error).

```python
spec = [
    {
        "op": "foreach",
        "in": "/items",
        "as": "item",
        "do": [
            {
                "op": "if",
                "cond": {"$eq": [{"$ref": "&:/item/status"}, "fatal"]},
                "then": [
                    {"/aborted": True},
                    {"$exit": None},
                ],
            },
            {"/processed[]": {"$ref": "&:/item/id"}},
        ],
    },
    {"/done": True},   # never reached once $exit fires
]

result = engine.apply(
    spec,
    source={"items": [{"id": 1, "status": "ok"},
                      {"id": 2, "status": "fatal"},
                      {"id": 3, "status": "ok"}]},
    dest={"processed": []},
)
# → {"processed": [1], "aborted": True}
```

**Embedding note.** `$exit` is swallowed only at the top-level entry points —
`apply` / `apply_async` / `apply_compiled` / `apply_compiled_async`. The
lower-level `apply_to_context` / `apply_compiled_to_context` (and their `_async`
twins) **propagate** the `ExitSignal` instead of catching it, because they double
as the internal runner for nested bodies (`foreach`, `while`, `if`, `exec`,
`try`, `$def`, `$eval`) — if they swallowed it, `$exit` inside a loop or function
could not terminate the whole script. So if you drive a script through a context
you built yourself and want `$exit` to mean "stop cleanly", use the entry-point
twins `run_script_in_context` / `run_compiled_in_context` (and `_async`), which
catch `ExitSignal` and return the document built so far. Real errors still
propagate.

```python
from j_perm import ExecutionContext

ctx = ExecutionContext(source=src, dest={}, engine=engine)
result = engine.run_script_in_context(spec, ctx)   # $exit → clean finish

# Equivalently, catch it yourself around apply_to_context:
from j_perm import ExitSignal
try:
    engine.apply_to_context(spec, ctx)
except ExitSignal:
    pass                       # not an error; ctx.dest holds the result
result = ctx.dest
```

#### Interaction with `try`

Control flow signals propagate **through** `try` blocks — they are never caught by `except`.  The `finally` block still runs before the signal continues propagating.

```python
spec = [
    {
        "$def": "func",
        "body": [
            {
                "op": "try",
                "do": [{"$return": "early"}],
                "except": [{"/caught": True}],   # NOT reached
                "finally": [{"/cleanup": True}],  # always runs
            },
        ],
    },
    {"/answer": {"$func": "func"}},
]

result = engine.apply(spec, source={}, dest={})
# → {"answer": "early"}   ("caught" is never set, "cleanup" is set inside the function)
```

---

### 8. Shorthand Syntax

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

When the value starts with `/`, `@:`, `&:`, `!:`, or `_:`, it is treated as a **copy-from pointer** and expands to an `op: copy` step:

```json
{"/name": "/user/fullName"}
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

This also works with context prefixes:

```json
{"/copy_of": "@:/existing_dest_key"}
{"/arg_val": "&:/param_name"}
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

### 9. Text Syntax

Specs can be written in a compact **text syntax** instead of raw op-dicts. A
text step is just a `str` element in the spec list; a `ParseTextStage` (priority
`1000`, registered by `build_default_engine(text_syntax=True)` — the default)
parses it into op-dicts before execution. Text and JSON **mix freely** in one
list: string elements are parsed, everything else is passed through untouched.

```python
engine = build_default_engine()              # text_syntax=True by default

engine.apply([
    "/min_age = 18",                          # text  → op: set
    {"op": "set", "path": "/tag", "value": "x"},  # raw JSON — untouched
    "/adults <- /users ?? []",                # text  → op: copy with default
], source={"users": [...]}, dest={})
```

A single string may hold many statements (separated by newlines or `;`) and may
expand into several steps. Blocks use **either** indentation (`:` + indent)
**or** braces `{ … }`. Set `build_default_engine(text_syntax=False)` (or the
async builder) to skip stage registration entirely — string steps then raise the
usual "unhandled step" error.

To parse text to op-dicts directly (e.g. for inspection):

```python
from j_perm.text import parse_text
parse_text("/x = 1 + 2")   # → [{"op": "set", "path": "/x", "value": {"$add": [1, 2]}}]
```

#### Statements

| Text | Op-dict |
|---|---|
| `/path = EXPR` | `{op: set, path, value: EXPR}` |
| `/path[] = EXPR` | `set` with `path` `.../-` (append) |
| `/dst <- /src` | `{op: copy, from, path, ignore_missing: true}` |
| `/dst <-! /src` | `copy` with `ignore_missing: false` (strict) |
| `/dst <- /src ?? EXPR` | `copy` with `default: EXPR` |
| `del /a, /b` | one `{op: delete, ignore_missing: true}` per pointer |
| `del! /a` | `delete` with `ignore_missing: false` |
| `assert /x` | `{op: assert, path}` (presence only) |
| `assert /x == EXPR` | `assert` with `equals: EXPR` |
| `if EXPR: … [elif EXPR: …] [else: …]` | `{op: if, cond, then, else?}` (elif → nested if) |
| `foreach v in /src: …` | `{op: foreach, as, in, do}` (bare pointer → `in`) |
| `foreach v in EXPR: …` | `foreach` with `in_value` (non-pointer source) |
| `foreach v in /src default EXPR: …` | `foreach` with `default` |
| `while EXPR: …` | `{op: while, cond, do}` |
| `do: … while EXPR` | `while` with `do_while: true` |
| `try: … except: … finally: …` | `{op: try, do, except?, finally?}` |
| `def name(a, b) [context=copy]: …` | `{$def, params, context?, body, on_failure?}` |
| `return [EXPR]` | `{$return: EXPR \| null}` |
| `raise EXPR` | `{$raise: EXPR}` |
| `break` / `continue` | `{$break: null}` / `{$continue: null}` |
| `exit` | `{$exit: null}` (terminate the whole script early) |
| `exec /script [merge]` / `exec: …` | `{op: exec, from \| actions, merge?}` |
| `/dst = serialize(SRC, key: v)` | `{op: serialize, path: /dst, from\|value: SRC, …}` |
| `/dst = deserialize(SRC, key: v)` | `{op: deserialize, path: /dst, from\|value: SRC, …}` |
| `/dst = encode(SRC, key: v)` | `{op: encode, path: /dst, from\|value: SRC, …}` |
| `/dst = decode(SRC, key: v)` | `{op: decode, path: /dst, from\|value: SRC, …}` |
| `/dst = hash(SRC, key: v)` | `{op: hash, path: /dst, from\|value: SRC, …}` |
| `op "name"(key: v): …` | `{op: "name", key: v, do?: …}` (generic escape hatch) |
| `name(args)` | `{$func: name, args}` (call as a step) |

For the five `serialize` / `deserialize` / `encode` / `decode` / `hash` forms, the
first positional argument is the source: a **bare pointer** (`/data`, `@:/x`) maps
to `from`, any other value maps to `value`; remaining `key: v` arguments become the
operation's options (`format` / `codec` / `algo` / `output` / `encoding` /
`create` / `extend` / `default`). These five names are **reserved keywords** in the
text syntax (like `raw`), so they cannot double as `$func` names there — use the
`op "…"(…)` escape hatch if you need an operation by a dynamic name.

```text
/payload = serialize(/data, format: "json")
/back    = deserialize(@:/payload, format: "json")
/b64     = encode("hello", codec: "base64")
/plain   = decode(@:/b64, codec: "base64")
/sum     = hash(/data, algo: "sha256")
```

A trailing `on_failure: …` block on a `def` becomes `$def.on_failure`.

Blocks nest to any depth; a nested suite may dedent back and be followed by
sibling statements at the outer level (as `/seen[]` is below):

```text
def greet(name) context=copy:
    /msg = "Hello, ${&:/name}!"
    return /msg

foreach item in /users:
    if $(&:/item/age) >= 18:
        /adults[] = $(&:/item)
    /seen[] = $(&:/item/name)
    if $(&:/item/name) == "stop":
        exit
```

#### Expressions

Literals: `42`, `3.14`, `"text ${...}"`, `true`, `false`, `null`,
`[EXPR, …]` (list), `{ k: EXPR, "k2": EXPR }` (dict). A **bare** pointer like
`/price` in value position is a **string literal** `"/price"`; to *read* a
pointer use `$(...)`.

| Text | Value |
|---|---|
| `$(/u/name)`, `$(@:/t)`, `$(&:/item)` | `{$ref: path}` |
| `$(/p ?? EXPR)` | `{$ref, $default}` |
| `$(/p) raw` | `{$ref, $raw: true}` |
| `not X` | `{$not: X}` |
| `-X` | negated number, or `{$sub: [0, X]}` |
| `exists /p` | `{$exists: "/p"}` |
| `a ** b` | `{$pow}` (right-assoc.) |
| `* / %` | `{$mul / $div / $mod}` |
| `+ -` | `{$add / $sub}` |
| `== != < <= > >=` | `{$eq / $ne / $lt / $lte / $gt / $gte}` |
| `X in Y` | `{$in: [X, Y]}` |
| `and` / `or` / `??` | `{$and}` / `{$or}` / `{$or}` (coalesce) |

Calls map known names to constructs; anything else becomes a `$func`:

| Call | Construct |
|---|---|
| `int/float/bool/str(x)` | `{$cast}` |
| `upper/lower/strip/lstrip/rstrip(...)` | `$str_*` |
| `split/join/replace/contains/startswith/endswith/slice(...)` | `$str_*` |
| `round(x, n, mode: "ceil")` | `{$round}` |
| `regex_match/search/findall/replace(..., flags: N)` | `$regex_*` |
| `regex_groups(p, s, named: true)` | `{$regex_groups}` |
| `len/keys/values/items/reverse/type/sum/avg/abs/floor/ceil(x)` | `$len / $keys / …` |
| `sort/unique/min/max(arr, key: "/p", reverse: true)` | `$sort / $unique / $min / $max` |
| `flatten(arr, depth: N)` | `{$flatten}` |
| `lslice(arr, start, end, step)` | `{$slice}` (list slice; `slice(...)` is the string one) |
| `map(arr, EXPR, as: "n")` / `filter(arr, COND, as: "n")` | `{$map}` / `{$filter}` |
| `ref(/p, default: …)` | `{$ref, $default}` |
| `raw(EXPR)` | `{$raw: EXPR}` |
| `eval { BLOCK } select /x` | `{$eval, $select?}` |
| `factorial(5)` | `{$func: "factorial", args: [5]}` |

> Strings use native `${...}` interpolation, resolved at runtime by the value
> pipeline (see [Template Interpolation](#2-template-interpolation-)). The text
> parser only builds structure; `$ref`, casts, limits, and `${...}` all run
> exactly as in JSON specs.

#### Nested sublanguages (`tag{ … }`)

A plugin can register its own high-priority stage that recognises `tag{ … }`
blocks and parses their raw contents with an **independent** parser, emitting
op-dicts before the core text stage runs. The core grammar knows nothing about
these tags. The j-perm-sql plugin ships `sql{ … }` and `sql_write{ … }` this way
— see its README.

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

The `from` pointer supports all context prefixes (`@:`, `&:`, `!:`, `_:`).

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
    // OR: "in_value": <array> — pass array directly (mutually exclusive with "in")
    "as": "item",
    // Variable name (default: "item")
    "do": [
        ...
    ],
    // Nested actions
    "skip_empty": true,
    // Skip if empty (default: true)
    "default": []
    // Fallback if "in" pointer fails (not applicable to "in_value")
}
```

Exactly one of `in` or `in_value` must be specified:

| Parameter | Description |
|-----------|-------------|
| `in` | JSON Pointer to an array in source. Supports slices. Falls back to `default` if the pointer fails. |
| `in_value` | Array value passed directly (supports templates, `$ref`, etc.). No pointer resolution. `default` is ignored. |

**Note:** If source is a dict, iterates over `(key, value)` tuples.

**Loop control:** Use `$break` to exit the loop early or `$continue` to skip the rest of the current iteration (see [Loop Control Flow](#loop-control-flow)).

**Accessing the loop variable:**

The loop variable is stored in `temp_read_only` and is accessible inside `do` blocks via:
- `&:/item` — pointer syntax
- `${&:/item}` — template syntax
- `{"$ref": "&:/item"}` — $ref syntax
- `${?args.item}` — JMESPath syntax

**The original source** is accessible via the plain `/` pointer as usual.

```python
spec = {
    "op": "foreach",
    "in": "/products",
    "as": "product",
    "do": {
        "/results[]": {
            "$eval": [
                {"/name": "${&:/product/name}"},
                {"/price": "${&:/product/price}"},
                {"/tax": {"$ref": "/config/tax"}},  # from original source
            ]
        }
    }
}

result = engine.apply(
    spec,
    source={
        "products": [{"name": "A", "price": 100}],
        "config": {"tax": 1.2}
    },
    dest={}
)
# → {"results": [{"name": "A", "price": 100, "tax": 1.2}]}
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

**Loop control:** Use `$break` to exit the loop early or `$continue` to skip the rest of the current iteration and re-evaluate the condition (see [Loop Control Flow](#loop-control-flow)).

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

### `try`

Execute actions with error handling (try-except-finally pattern).

**Basic try-except:**

```json
{
    "op": "try",
    "do": [
        {"op": "copy", "from": "/might_not_exist", "path": "/result"}
    ],
    "except": [
        {"/error": "Failed to copy value"}
    ]
}
```

**Access error information:**

Inside the `except` block, error info is available via the `&:` prefix:

```json
{
    "op": "try",
    "do": [
        {"$raise": "Something went wrong"}
    ],
    "except": [
        {"/error_message": "${&:/_error_message}"},
        {"/error_type": "${&:/_error_type}"}
    ]
}
```

**With finally cleanup:**

```json
{
    "op": "try",
    "do": [
        {"/status": "processing"},
        {"op": "exec", "from": "/dangerous_operation"}
    ],
    "except": [
        {"/status": "error"},
        {"/error_msg": "${&:/_error_message}"}
    ],
    "finally": [
        {"/processed_at": "2024-01-01"},
        {"/cleanup": true}
    ]
}
```

**Behavior:**
- Executes actions in `do` block
- If error occurs:
  - Error info stored in `temp_read_only` (`_error_type`, `_error_message`)
  - If `except` block provided, executes it with error info accessible via `&:/` prefix
  - If no `except`, re-raises error after executing `finally` (if present)
- `finally` block always executes (even on error)
- Control flow signals (`$break`, `$continue`, `$return`) are **not** caught by `except` — they propagate through the `try` block; `finally` still runs before they propagate

**Error info in except:**
- `&:/_error_type` — error class name (e.g., `"JPermError"`)
- `&:/_error_message` — error message string

**Example: Validation with fallback**

```python
spec = {
    "op": "try",
    "do": [
        {"/age": {"$cast": {"value": "${/user_input}", "type": "int"}}},
        {
            "op": "if",
            "cond": {"$lt": [{"$ref": "@:/age"}, 0]},
            "then": [{"$raise": "Age cannot be negative"}]
        },
        {"/valid": True}
    ],
    "except": [
        {"/valid": False},
        {"/error": "${&:/_error_message}"}
    ]
}

result = engine.apply(spec, source={"user_input": "-5"}, dest={})
# → {"age": -5, "valid": False, "error": "Age cannot be negative"}
```

---

### `deserialize`

Parse a serialized string into a structured value.

```json
{
    "op": "deserialize",
    "from": "/raw_string",
    // OR: "value": "..." — inline serialized string (mutually exclusive with "from")
    "format": "json",
    // Format: "json", "pretty_json" (alias for json), or "yaml"
    "path": "/result",
    // Destination pointer
    "create": true,
    // Auto-create intermediate nodes (default: true)
    "extend": true,
    // Extend list on append (default: true)
    "default": {}
    // Fallback value if source pointer fails or parsing fails
}
```

Exactly one of `from` or `value` must be specified.

| Format | Description |
|--------|-------------|
| `json` | RFC 8259 JSON — compact and pretty-printed both accepted |
| `pretty_json` | Alias for `json` |
| `yaml` | YAML document parsed with `yaml.safe_load` |

The `from` pointer supports all context prefixes (`@:`, `&:`, `!:`, `_:`).

**Example — parse JSON from source:**

```python
spec = {
    "op": "deserialize",
    "from": "/payload",
    "format": "json",
    "path": "/data",
}

result = engine.apply(
    spec,
    source={"payload": '{"name": "Alice", "age": 30}'},
    dest={},
)
# → {"data": {"name": "Alice", "age": 30}}
```

**Example — parse YAML with fallback:**

```python
spec = {
    "op": "deserialize",
    "from": "/raw_config",
    "format": "yaml",
    "path": "/config",
    "default": {},
}

result = engine.apply(spec, source={}, dest={})
# → {"config": {}}  (source pointer missing, default used)
```

**Example — inline value:**

```python
spec = {
    "op": "deserialize",
    "value": "[1, 2, 3]",
    "format": "json",
    "path": "/items",
}
result = engine.apply(spec, source={}, dest={})
# → {"items": [1, 2, 3]}
```

---

### `serialize`

Render a value into a serialized string — the inverse of `deserialize`.

```json
{
    "op": "serialize",
    "from": "/data",
    // OR: "value": <val> — inline value (mutually exclusive with "from")
    "format": "json",
    // Format: "json" (compact), "pretty_json" (indented), or "yaml"
    "path": "/result",
    // Destination pointer
    "create": true,
    // Auto-create intermediate nodes (default: true)
    "extend": true,
    // Extend list on append (default: true)
    "default": "{}"
    // Fallback value if source pointer fails or serialization fails
}
```

Exactly one of `from` or `value` must be specified.

| Format | Description |
|--------|-------------|
| `json` | Compact JSON (`ensure_ascii=False`, no extra whitespace) |
| `pretty_json` | Indented JSON (`indent=2`) |
| `yaml` | YAML document via `yaml.safe_dump` (`sort_keys=False`, `allow_unicode=True`) |

The `from` pointer supports all context prefixes (`@:`, `&:`, `!:`, `_:`).

**Example — value to compact JSON:**

```python
spec = {
    "op": "serialize",
    "value": {"name": "Alice", "age": 30},
    "format": "json",
    "path": "/payload",
}

result = engine.apply(spec, source={}, dest={})
# → {"payload": '{"name":"Alice","age":30}'}
```

**Example — round-trip with `deserialize`:**

```python
spec = [
    {"op": "serialize", "from": "/obj", "format": "yaml", "path": "/text"},
    {"op": "deserialize", "from": "@:/text", "format": "yaml", "path": "/back"},
]

result = engine.apply(spec, source={"obj": {"a": 1, "b": [2, 3]}}, dest={})
# → {"text": "a: 1\nb:\n- 2\n- 3\n", "back": {"a": 1, "b": [2, 3]}}
```

---

### `encode` / `decode`

Encode a text string through a base/URL codec, and decode it back. `encode` maps
text → bytes (via `encoding`) → codec string; `decode` is the exact inverse.

```json
{
    "op": "encode",
    // OR "op": "decode"
    "from": "/text",
    // OR: "value": "..." — inline text (mutually exclusive with "from")
    "codec": "base64",
    // Codec (see table below)
    "encoding": "utf-8",
    // Text encoding applied before/after the codec (default: "utf-8")
    "path": "/result",
    // Destination pointer
    "create": true,
    "extend": true,
    "default": ""
    // Fallback value if source pointer fails or the codec fails
}
```

Exactly one of `from` or `value` must be specified.

| Codec | Description |
|-------|-------------|
| `base64` | Standard RFC 4648 Base64 |
| `base64url` | URL-safe Base64 (`-_` instead of `+/`) |
| `base32` | RFC 4648 Base32 |
| `base16` | RFC 4648 Base16 (uppercase hex) |
| `hex` | Lowercase hexadecimal |
| `base85` | Base85 (RFC 1924 / git alphabet) |
| `ascii85` | Ascii85 (Adobe alphabet) |
| `url` | Percent-encoding (`urllib.parse.quote` / `unquote`) |

The `from` pointer supports all context prefixes (`@:`, `&:`, `!:`, `_:`).

**Example — encode to Base64:**

```python
spec = {
    "op": "encode",
    "value": "hello",
    "codec": "base64",
    "path": "/b64",
}

result = engine.apply(spec, source={}, dest={})
# → {"b64": "aGVsbG8="}
```

**Example — decode a JWT-style Base64URL segment, then parse it:**

```python
spec = [
    {"op": "decode", "from": "/segment", "codec": "base64url", "path": "/json"},
    {"op": "deserialize", "from": "@:/json", "format": "json", "path": "/claims"},
]

result = engine.apply(
    spec,
    source={"segment": "eyJzdWIiOiAiNDIifQ=="},
    dest={},
)
# → {"json": '{"sub": "42"}', "claims": {"sub": "42"}}
```

---

### `hash`

Compute a deterministic digest of a value — for checksums or content-derived ids.

```json
{
    "op": "hash",
    "from": "/obj",
    // OR: "value": <any> — inline value (mutually exclusive with "from")
    "algo": "sha256",
    // Digest algorithm (see table below)
    "output": "hex",
    // Digest encoding: "hex", "base64", or "base64url"
    "encoding": "utf-8",
    // Text encoding for string / canonical inputs (default: "utf-8")
    "path": "/checksum",
    // Destination pointer
    "create": true,
    "extend": true,
    "default": null
    // Fallback value if source pointer fails
}
```

Exactly one of `from` or `value` must be specified. A **string** input is hashed
as its `encoding` bytes; **any other value** is first canonically serialized
(`json.dumps(value, sort_keys=True, separators=(",", ":"))`) so that equal objects
produce equal digests regardless of key order.

| Algorithm | Notes |
|-----------|-------|
| `sha256` | Default |
| `sha512` | |
| `sha1` | Legacy — avoid for security |
| `md5` | Legacy — avoid for security |
| `sha3_256`, `sha3_512` | SHA-3 family |
| `blake2b`, `blake2s` | BLAKE2 family |

The `from` pointer supports all context prefixes (`@:`, `&:`, `!:`, `_:`).

**Example — hash of an object (canonical, key-order independent):**

```python
spec = {
    "op": "hash",
    "from": "/user",
    "algo": "sha256",
    "path": "/user_id",
}

result = engine.apply(spec, source={"user": {"name": "Alice", "age": 30}}, dest={})
# → {"user_id": "<64-hex-char sha256 of the canonical JSON>"}
```

**Example — verify a checksum with `assert`:**

```python
spec = [
    {"op": "hash", "from": "/payload", "algo": "sha256", "path": "/actual"},
    {"op": "assert", "path": "@:/actual", "equals": "${/expected_sha256}"},
]

# Raises AssertionError if the payload's digest does not match the expected one.
# (`@:/actual` reads the just-computed digest from dest; `${/expected_sha256}`
#  pulls the expected value from source.)
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
    CORE_HANDLERS,                # $ref, $eval
    LOGICAL_HANDLERS,             # $and, $or, $not
    COMPARISON_HANDLERS,          # $gt, $gte, $lt, $lte, $eq, $ne, $in, $exists
    MATH_HANDLERS,                # $add, $sub, $mul, $div, $pow, $mod (with default limits)
    STRING_HANDLERS,              # All string operations (11 constructs, with default limits)
    REGEX_HANDLERS,               # All regex operations (5 constructs, with default limits)
    get_all_handlers,             # Function to get all handlers with casters
    get_all_handlers_with_limits, # Function to get all handlers with custom limits
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
    exists_handler,
    # ... etc
)
```

**Available groups:**
- `CORE_HANDLERS` — Core constructs (`$ref`, `$eval`)
- `LOGICAL_HANDLERS` — Logical operators (`$and`, `$or`, `$not`)
- `COMPARISON_HANDLERS` — Comparison operators (`$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`, `$in`, `$exists`)
- `MATH_HANDLERS` — Mathematical operators with default limits (`$add`, `$sub`, `$mul`, `$div`, `$pow`, `$mod`, `$round`)
- `STRING_HANDLERS` — String operations with default limits (11 constructs)
- `REGEX_HANDLERS` — Regular expression operations with default limits (5 constructs)
- `ALL_HANDLERS_NO_CAST` — All handlers except `$cast`
- `get_all_handlers(casters)` — Function returning all handlers including `$cast` (with default limits)
- `get_all_handlers_with_limits(casters, **limits)` — Function returning all handlers with custom limits

**Example with custom limits:**

```python
from j_perm import get_all_handlers_with_limits
from j_perm.casters import BUILTIN_CASTERS

# Build handlers with conservative limits
secure_handlers = get_all_handlers_with_limits(
    casters=BUILTIN_CASTERS,
    regex_timeout=1.0,
    pow_max_exponent=100,
    str_max_join_result=100_000,
    mul_max_string_result=100_000,
    add_max_string_result=1_000_000,
    sub_max_number_result=1e10,
)

engine = build_default_engine(specials=secure_handlers)
```

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

## Compilation

When the same DSL script runs many times with different data, stage processing and matcher resolution repeat identically on every call.  **Compilation** eliminates that cost: run stages and resolve handlers once, capture the result as a `CompiledSpec`, and execute it as many times as needed.

```python
engine = build_default_engine()

# Compile once
spec = [
    {"op": "foreach", "in": "/orders", "as": "order",
     "do": [{"op": "set", "path": "/results/-", "value": "${&:/order/total}"}]}
]
compiled = engine.compile(spec)   # → CompiledSpec

# Run many times with different data
result_a = compiled.apply(source={"orders": [...]}, dest={"results": []})
result_b = compiled.apply(source={"orders": [...]}, dest={"results": []})
```

### What gets compiled

- **Stage processors** (`AssertShorthandProcessor`, `DeleteShorthandProcessor`, `AssignShorthandProcessor`) — run once; their output (normalized steps) is stored.
- **Handler resolution** — `ActionTypeRegistry.resolve()` is called once per step; the handler list is cached in `CompiledStep.handlers`.
- **Nested specs** — bodies of compound operations (`foreach.do`, `while.do`, `if.then/else`, `try.do/except/finally`, `$def.body`) are recursively compiled. When a compound handler executes, it calls `compiled_body.run(ctx)` instead of re-running stage processing.
- **Isolated/named pipelines** — by default a nested spec is compiled against the same pipeline as its parent. A `Compound` handler may override `nested_spec_pipeline(step, key)` to return a registered pipeline name; the compiler then compiles that nested spec against `engine.get_pipeline(name)` instead. The resulting `CompiledSpec` remembers its owning pipeline, so `compiled.run(ctx)` dispatches through it — not the main pipeline. This lets a plugin with its own isolated pipeline (e.g. the `j-perm-sql` SQL pipeline) be compiled end-to-end without the core engine knowing anything about the plugin's constructs.

### CompiledSpec API

```python
compiled = engine.compile(spec)       # → CompiledSpec | None

# Execute
result = compiled.apply(source=data, dest={})
result = await compiled.apply_async(source=data, dest={})

# Use inside an existing execution context (e.g. from a custom handler)
result = compiled.run(ctx)

# Re-attach engine after unpickling
compiled = pickle.loads(pickle.dumps(compiled))
compiled.attach_engine(engine)
result = compiled.apply(source=data, dest={})

# Or pass engine directly
result = compiled.apply(source=data, dest={}, engine=engine)
```

`engine.compile()` returns `None` when compilation is not possible (see below).

### Pickle support

`CompiledSpec` and its nested `CompiledStep` objects are picklable.  The engine reference is excluded from pickle because engines contain closures and stateful objects.  After unpickling, re-attach an engine before calling `apply()`:

```python
import pickle

compiled = engine.compile(spec)
data     = pickle.dumps(compiled)      # save to disk / send over network

# --- later, in a different process ---
compiled = pickle.loads(data)
compiled.attach_engine(engine)
result = compiled.apply(source=..., dest={})
```

`compiled.run(ctx)` works without `attach_engine` because it reads the engine from the provided context.

### Context-aware stages

Stage processors and matchers that read from `ctx` at runtime must declare `context_aware = True`.  `engine.compile()` returns `None` for pipelines that contain any such stage:

```python
from j_perm import StageProcessor

class MyContextAwareStage(StageProcessor):
    context_aware = True   # opt out of compilation

    def apply(self, steps, ctx):
        # reads ctx.source — cannot be run at compile time
        return [s for s in steps if ctx.source.get("enabled")]
```

Built-in stages (`AssertShorthandProcessor`, `DeleteShorthandProcessor`, `AssignShorthandProcessor`) all have `context_aware = False` (the default), so `build_default_engine()` pipelines are always compilable.

### `exec` and dynamic specs

`ExecHandler` (`op: exec` with `"from"`) loads its action list from the source document at runtime — the nested spec is unknown at compile time.  The outer `exec` step is still compiled (handler resolved), but its body falls back to dynamic dispatch on every run.  This is transparent: `compiled.apply(...)` produces the same result as `engine.apply(...)`.

### The `Compound` interface

Handlers that contain nested DSL specs implement `Compound`:

```python
from j_perm import Compound, ActionHandler, CompiledSpec
from typing import Any

class MyCompoundHandler(ActionHandler, Compound):
    def nested_spec_keys(self, step: Any) -> list[str]:
        """Tell the compiler which keys hold nested specs."""
        return ["body"]

    def execute(self, step: Any, ctx) -> Any:
        return ctx.engine.apply_to_context(step["body"], ctx)

    def execute_compiled(self, step: Any, ctx, nested: dict[str, CompiledSpec]) -> Any:
        """Execute using pre-compiled nested specs (called by run_compiled)."""
        compiled_body = nested.get("body")
        if compiled_body is not None:
            return compiled_body.run(ctx)
        return ctx.engine.apply_to_context(step["body"], ctx)
```

`nested` is a `dict` rather than a single `CompiledSpec` because one step can contain **multiple** named sub-specs.  The key matches the field name in the step dict:

| Operation | Keys in `nested` |
|-----------|-----------------|
| `foreach` | `"do"` |
| `while` | `"do"` |
| `if` | `"then"`, `"do"`, `"else"` (whichever are present) |
| `try` | `"do"`, `"except"`, `"finally"` (whichever are present) |
| `$def` | `"body"`, `"on_failure"` (if defined) |

In `execute_compiled`, look up the key you need with `nested.get(key)`.  If it returns `None` (e.g. the `if` step has no `"else"` branch), fall back to `ctx.engine.apply_to_context`.

Built-in compound handlers: `ForeachHandler`, `WhileHandler`, `IfHandler`, `TryHandler`, `DefHandler`.

### Engine methods

| Method | Description |
|--------|-------------|
| `engine.compile(spec)` | Compile a spec. Returns `CompiledSpec` or `None`. |
| `engine.apply_compiled(compiled, *, source, dest)` | Execute a compiled spec. |
| `engine.apply_compiled_async(compiled, *, source, dest)` | Async version. |
| `engine.apply_compiled_to_context(compiled, ctx)` | Run inside an existing context (propagates `$exit`). |
| `engine.apply_compiled_to_context_async(compiled, ctx)` | Async version. |
| `engine.run_compiled_in_context(compiled, ctx)` | Run inside an existing context, swallowing `$exit` (clean finish). |
| `engine.run_compiled_in_context_async(compiled, ctx)` | Async version. |

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
| `engine.apply_to_context_async()` | Async version of `apply_to_context()` (propagates `$exit`) |
| `engine.run_script_in_context_async()` | Async version of `run_script_in_context()` (swallows `$exit`) |
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

To stop the loop early and return the current result as-is, use the `$raw: true` flag or the `$raw` wrapper construct (see [`$raw`](#raw--return-a-literal-without-processing)).

### PipelineSignal — extensible in-pipeline control flow

`PipelineSignal` is an abstract base class (defined in `core.py`) for signals that `Pipeline.run` intercepts. When a handler raises a `PipelineSignal`, `Pipeline.run` calls `signal.handle(ctx)`. If `handle` re-raises, the signal propagates up to the caller (e.g. `Engine.process_value`).

This lets you add new pipeline-level behaviours without touching `core.py`:

```python
from j_perm import PipelineSignal

class MySignal(PipelineSignal):
    def __init__(self, value):
        self.value = value

    def handle(self, ctx):
        ctx.dest = self.value
        raise self  # propagate to stop the stabilisation loop
```

Built-in signals that inherit from `PipelineSignal`:

| Signal | Raised by | Caught by |
|--------|-----------|-----------|
| `RawValueSignal` | `raw_handler`, `SpecialResolveHandler` (flag) | `Engine.process_value` |

Control flow signals inherit from `ControlFlowSignal` (not `PipelineSignal`):

| Signal | Raised by | Caught by |
|--------|-----------|-----------|
| `BreakSignal` | `$break` | `foreach` / `while` handler |
| `ContinueSignal` | `$continue` | `foreach` / `while` handler |
| `ReturnSignal` | `$return` | `$func` call handler |

`ControlFlowSignal` is the common base for all three. `Pipeline.run` treats them as non-errors and never attaches a language call stack to them. You can catch the base class if you need to intercept any control flow signal propagating out of a custom handler:

```python
from j_perm import ControlFlowSignal

try:
    engine.apply(spec, source=src, dest={})
except ControlFlowSignal:
    # $break / $continue / $return used outside their valid scope
    ...
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
    ValueProcessor,
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

    # Pipeline control flow (base classes for extensible signals)
    ControlFlowSignal,  # Base for $break / $continue / $return signals
    PipelineSignal,     # Base for value-pipeline signals (e.g. RawValueSignal)

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
    raw_handler,        # $raw wrapper / literal escape
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
    exists_handler,

    # Mathematical operators
    add_handler,
    make_add_handler,     # Factory with configurable limits
    sub_handler,
    make_sub_handler,     # Factory with configurable limits
    mul_handler,
    make_mul_handler,     # Factory with configurable limits
    div_handler,
    pow_handler,
    make_pow_handler,     # Factory with configurable limits
    mod_handler,
    round_handler,

    # String operations
    str_split_handler,
    make_str_split_handler,    # Factory with configurable limits
    str_join_handler,
    make_str_join_handler,     # Factory with configurable limits
    str_slice_handler,
    str_upper_handler,
    str_lower_handler,
    str_strip_handler,
    str_lstrip_handler,
    str_rstrip_handler,
    str_replace_handler,
    make_str_replace_handler,  # Factory with configurable limits
    str_contains_handler,
    str_startswith_handler,
    str_endswith_handler,

    # Regex operations
    regex_match_handler,
    make_regex_match_handler,   # Factory with configurable limits
    regex_search_handler,
    make_regex_search_handler,  # Factory with configurable limits
    regex_findall_handler,
    make_regex_findall_handler, # Factory with configurable limits
    regex_replace_handler,
    make_regex_replace_handler, # Factory with configurable limits
    regex_groups_handler,
    make_regex_groups_handler,  # Factory with configurable limits

    # Collection / value operations
    len_handler,
    keys_handler,
    values_handler,
    items_handler,
    reverse_handler,
    slice_handler,
    flatten_handler,
    type_handler,
    sum_handler,
    avg_handler,
    min_handler,
    max_handler,
    sort_handler,
    unique_handler,
    abs_handler,
    floor_handler,
    ceil_handler,
    map_handler,
    make_map_handler,     # Factory with configurable size limit
    filter_handler,
    make_filter_handler,  # Factory with configurable size limit

    # Function handlers
    DefMatcher,
    CallMatcher,
    DefHandler,
    CallHandler,
    RaiseMatcher,
    RaiseHandler,
    JPermError,
    ReturnMatcher,
    ReturnHandler,

    # Loop control flow handlers
    BreakMatcher,
    BreakHandler,
    ContinueMatcher,
    ContinueHandler,

    # Control flow signals (exceptions)
    BreakSignal,       # raised by $break
    ContinueSignal,    # raised by $continue
    ReturnSignal,      # raised by $return (.value holds the return value)
    RawValueSignal,    # raised by $raw / $raw:True flag (.value = raw result)

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
    TryHandler,
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

    # Processor
    PointerProcessor,

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

### Example 1: Data Filtering with foreach

```python
spec = {
    "op": "foreach",
    "in": "/products",
    "as": "item",
    "do": {
        "op": "if",
        "cond": "${?args.item.price < `100`}",
        "then": {"/affordable[]": "&:/item"}
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

### Example 5: Functions with Parameters and Error Handling

```python
spec = [
    {
        "$def": "validateAge",
        "params": ["age"],
        "body": [
            {
                "op": "if",
                "cond": {"$lt": [{"$ref": "&:/age"}, 0]},
                "then": [{"$raise": "Age cannot be negative: ${&:/age}"}]
            },
            {"/valid": True}
        ],
        "return": "/valid",
        "on_failure": [
            {"/validation_failed": True},
            {"/last_error": "Validation error occurred"}
        ]
    },
    {"/user_age_valid": {"$func": "validateAge", "args": [25]}},
]
```

### Example 6: try-except with Error Info

```python
spec = {
    "op": "try",
    "do": [
        {"/age": {"$cast": {"value": "${/user_input}", "type": "int"}}},
        {
            "op": "if",
            "cond": {"$lt": [{"$ref": "@:/age"}, 0]},
            "then": [{"$raise": "Age cannot be negative"}]
        },
        {"/valid": True}
    ],
    "except": [
        {"/valid": False},
        {"/error": "${&:/_error_message}"}
    ]
}

result = engine.apply(spec, source={"user_input": "-5"}, dest={})
# → {"age": -5, "valid": False, "error": "Age cannot be negative"}
```

### Example 7: Loop Control with $break and $continue

```python
# Collect items until sentinel, skipping nulls
spec = {
    "op": "foreach",
    "in": "/stream",
    "as": "item",
    "do": [
        {
            "op": "if",
            "cond": {"$eq": [{"$ref": "&:/item"}, None]},
            "then": [{"$continue": None}],  # skip null
        },
        {
            "op": "if",
            "cond": {"$eq": [{"$ref": "&:/item"}, "END"]},
            "then": [{"$break": None}],     # stop at sentinel
        },
        {"/result[]": "&:/item"},
    ],
}

result = engine.apply(
    spec,
    source={"stream": ["a", None, "b", "END", "c"]},
    dest={"result": []},
)
# → {"result": ["a", "b"]}
```

### Example 8: Early Return from a Function

```python
spec = [
    {
        "$def": "first_positive",
        "params": ["nums"],
        "body": [
            {
                "op": "foreach",
                "in": "&:/nums",
                "as": "n",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$gt": [{"$ref": "&:/n"}, 0]},
                        "then": [{"$return": {"$ref": "&:/n"}}],
                    },
                ],
            },
            {"$return": None},
        ],
    },
    {"/result": {"$func": "first_positive", "args": [[-3, -1, 0, 5, 8]]}},
]

result = engine.apply(spec, source={}, dest={})
# → {"result": 5}
```

### Example 9: $exists for Optional Fields

```python
spec = [
    {
        "op": "if",
        "cond": {"$exists": "/user/middle_name"},
        "then": [{"/display": "${/user/first_name} ${/user/middle_name} ${/user/last_name}"}],
        "else": [{"/display": "${/user/first_name} ${/user/last_name}"}],
    }
]
```

---

## License

MIT (or adapt to your project as needed)

---

## Contributing

Issues and pull requests welcome!