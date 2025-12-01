
# JSON patch applier

A small, composable JSON transformation DSL implemented in Python.

The library lets you describe transformations as data (a list of steps) and then
apply them to an input document. It supports JSON Pointer paths, custom
JMESPath expressions, interpolation with `${...}` syntax, and a set of
built-in operations.

## Features

- JSON Pointer read/write with support for:
  - root pointers (`''`, `'/'`, `'.'`)
  - relative `..` segments
  - list slices like `/items[1:3]`
- String interpolation:
  - `${/path/to/node}` — JSON Pointer lookup
  - `${int:/path}` / `${float:/path}` / `${bool:/path}` — simple casters
  - `${? some.jmespath(expression) }` — JMESPath with custom functions
- Special values:
  - `$ref` — reference into the source document
  - `$eval` — nested DSL evaluation with optional `$select`
- Rich set of operations:
  - `set`, `copy`, `copyD`, `delete`, `assert`
  - `foreach`, `if`, `distinct`
  - `replace_root`, `exec`, `update`
- Schema helper: approximate JSON Schema generation for a given DSL script.

## Basic usage

```python
from j_perm import apply_actions

source = {
    "users": [
        {"name": "Alice", "age": 17},
        {"name": "Bob", "age": 22}
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
# result -> {"fullName": "Alice", "isAdult": True}
```

## Schema generation

```python
from j_perm import build_schema

schema = build_schema(script)
```

`schema` is a JSON-Schema-like structure that you can use for documentation,
validation or introspection.

## Extending with custom operations

```python
from j_perm import register_op

@register_op("my_op")
def my_op(step, dest, src):
    # implement your logic here
    return dest
```

Any registered operation can then be used in DSL scripts via `{"op": "my_op", ...}`.

## License

This package is provided as-is; feel free to adapt it to your project structure.
