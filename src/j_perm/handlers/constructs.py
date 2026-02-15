"""Special construct handlers: ``$ref``, ``$eval``, and ``$cast``.

These are ``SpecialFn`` callables meant to be registered with
``SpecialResolveHandler`` in the value pipeline.

Exports
-------
ref_handler
    Handles ``{"$ref": pointer, "$default": fallback}`` construct.
    Resolves the pointer from source, returns deep copy.

eval_handler
    Handles ``{"$eval": actions, "$select": pointer}`` construct.
    Executes nested actions with empty dest, optionally selects sub-path.

make_cast_handler
    Factory function that creates a cast handler with registered casters.
    Returns a handler for ``{"$cast": {"value": <value>, "type": <type_name>}}`` construct.
"""

from __future__ import annotations

import copy
from typing import Any, Mapping, Tuple

from ..core import ExecutionContext

_MISSING = object()


def ref_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$ref`` construct: resolve pointer from source or dest.

    Schema::

        {"$ref": "/path/to/value", "$default": <fallback>}

    Behavior:
    * Template substitution is applied to the ``$ref`` value itself
    * Pointer is resolved from ``ctx.source`` by default, or from ``ctx.dest`` if ``$from: "dest"``
    * Supports prefix syntax: ``@:/path`` for dest, ``_:/path`` for metadata
    * Supports slices (``/arr[1:]``) via ``ctx.engine.processor.get``
    * Returns deep copy to prevent aliasing
    * If pointer fails and ``$default`` exists → return ``$default``
    * Otherwise raises the original exception

    Examples::

        {"$ref": "/user/name"}                      # from source
        {"$ref": "@:/user/name"}                    # from dest (prefix syntax)
        {"$ref": "${/path}", "$default": "unknown"}
        {"$ref": "/items[2:]"}
    """
    # Expand templates in the pointer itself
    ptr = ctx.engine.process_value(node["$ref"], ctx, _unescape=False)

    dflt = node.get("$default", _MISSING)
    try:
        return copy.deepcopy(ctx.engine.processor.get(ptr, ctx))
    except Exception:
        if dflt is not _MISSING:
            return ctx.engine.process_value(copy.deepcopy(dflt), ctx)
        raise


def eval_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$eval`` construct: execute nested actions inline.

    Schema::

        {"$eval": <actions>, "$select": "/path"}

    Behavior:
    * ``$eval`` value is treated as a full action spec (can be shorthand dict,
      list of steps, etc.)
    * Executed with ``dest={}`` and ``source=ctx.source``
    * If ``$select`` is present, that pointer is extracted from the result
    * Returns the final value (or selected sub-path)

    Examples::

        {"$eval": {"/name": "/user/name"}}
        {"$eval": [{"/a": 1}, {"/b": 2}], "$select": "/a"}
        {"$eval": {"op": "copy", "from": "/x", "path": "/y"}, "$select": "/y"}
    """
    # Execute the nested actions with fresh dest
    result = ctx.engine.apply(node["$eval"], source=ctx.source, dest={})

    if "$select" in node:
        sel_ptr = ctx.engine.process_value(node["$select"], ctx, _unescape=False)
        # Create temporary context to resolve from eval result
        temp_ctx = ctx.copy(new_source=result)
        return ctx.engine.processor.get(sel_ptr, temp_ctx)

    return result


def and_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$and`` construct: execute multiple actions and merge results.

    Schema::

        {"$and": [<actions>]}

    Behavior:
    * Each action is executed with the same source and an initially empty dest
    * Results of all actions are merged (shallow dict merge)
    * If there are conflicting keys, later actions override earlier ones

    Examples::

        {"$and": [[{"op": "copy", "from": "/a", "path": "/"}], [{"op": "copy", "from": "/b", "path": "/"}]]}
        {"$and": [[{"op": "assert", "path": "/a", "return": "/"}], [{"op": "copy", "from": "/b", "path": "/"}]]}
    """

    last_result = None
    for action in node["$and"]:
        last_result = ctx.engine.apply(action, source=ctx.source, dest={})
        if not last_result:
            return last_result
    return last_result


def or_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$or`` construct: execute multiple actions and return first truthy result.

    Schema::

        {"$or": [<actions>]}

    Behavior:
    * Each action is executed with the same source and an initially empty dest
    * Returns the first truthy result among the actions
    * If all results are falsy, returns the last result

    Examples::

        {"$or": [[{"op": "assert", "path": "/a", "return": True}], [{"op": "copy", "from": "/c", "path": "/"}]]}
        {"$or": [{"op": "assert", "path": "/a", "return": True}, {"op": "assert", "path": "/b", "return": True}]}
    """

    last_result = None
    for action in node["$or"]:
        last_result = ctx.engine.apply(action, source=ctx.source, dest={})
        if last_result:
            return last_result
    return last_result


def not_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$not`` construct: execute a single action and negate its result.

    Schema::

        {"$not": <action>}

    Behavior:
    * The action is executed with the same source and an initially empty dest
    * Returns the logical negation of the action's result

    Examples::

        {"$not": [{"op": "assert", "path": "/a", "return": True}]}
        {"$not": [{"op": "copy", "from": "/a", "path": "/"}]}
    """

    result = ctx.engine.apply(node["$not"], source=ctx.source, dest={})
    return not result


def gt_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$gt`` construct: greater than comparison.

    Schema::

        {"$gt": [<left>, <right>]}

    Behavior:
    * Both values are processed through ``process_value``
    * Returns ``left > right``

    Examples::

        {"$gt": [10, 5]}                              → True
        {"$gt": ["${/age}", 18]}                      → True if age > 18
        {"$gt": [{"$ref": "/count"}, 0]}              → True if count > 0
    """
    if not isinstance(node["$gt"], list) or len(node["$gt"]) != 2:
        raise ValueError("$gt requires a list of exactly 2 values")

    left = ctx.engine.process_value(node["$gt"][0], ctx)
    right = ctx.engine.process_value(node["$gt"][1], ctx)
    return left > right


def gte_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$gte`` construct: greater than or equal comparison.

    Schema::

        {"$gte": [<left>, <right>]}

    Behavior:
    * Both values are processed through ``process_value``
    * Returns ``left >= right``

    Examples::

        {"$gte": [10, 10]}                            → True
        {"$gte": ["${/age}", 18]}                     → True if age >= 18
    """
    if not isinstance(node["$gte"], list) or len(node["$gte"]) != 2:
        raise ValueError("$gte requires a list of exactly 2 values")

    left = ctx.engine.process_value(node["$gte"][0], ctx)
    right = ctx.engine.process_value(node["$gte"][1], ctx)
    return left >= right


def lt_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$lt`` construct: less than comparison.

    Schema::

        {"$lt": [<left>, <right>]}

    Behavior:
    * Both values are processed through ``process_value``
    * Returns ``left < right``

    Examples::

        {"$lt": [5, 10]}                              → True
        {"$lt": ["${/age}", 18]}                      → True if age < 18
    """
    if not isinstance(node["$lt"], list) or len(node["$lt"]) != 2:
        raise ValueError("$lt requires a list of exactly 2 values")

    left = ctx.engine.process_value(node["$lt"][0], ctx)
    right = ctx.engine.process_value(node["$lt"][1], ctx)
    return left < right


def lte_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$lte`` construct: less than or equal comparison.

    Schema::

        {"$lte": [<left>, <right>]}

    Behavior:
    * Both values are processed through ``process_value``
    * Returns ``left <= right``

    Examples::

        {"$lte": [10, 10]}                            → True
        {"$lte": ["${/age}", 65]}                     → True if age <= 65
    """
    if not isinstance(node["$lte"], list) or len(node["$lte"]) != 2:
        raise ValueError("$lte requires a list of exactly 2 values")

    left = ctx.engine.process_value(node["$lte"][0], ctx)
    right = ctx.engine.process_value(node["$lte"][1], ctx)
    return left <= right


def eq_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$eq`` construct: equality comparison.

    Schema::

        {"$eq": [<left>, <right>]}

    Behavior:
    * Both values are processed through ``process_value``
    * Returns ``left == right``

    Examples::

        {"$eq": [10, 10]}                             → True
        {"$eq": ["${/status}", "active"]}             → True if status == "active"
        {"$eq": [{"$ref": "/name"}, "Alice"]}         → True if name == "Alice"
    """
    if not isinstance(node["$eq"], list) or len(node["$eq"]) != 2:
        raise ValueError("$eq requires a list of exactly 2 values")

    left = ctx.engine.process_value(node["$eq"][0], ctx)
    right = ctx.engine.process_value(node["$eq"][1], ctx)
    return left == right


def ne_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$ne`` construct: not equal comparison.

    Schema::

        {"$ne": [<left>, <right>]}

    Behavior:
    * Both values are processed through ``process_value``
    * Returns ``left != right``

    Examples::

        {"$ne": [10, 5]}                              → True
        {"$ne": ["${/status}", "deleted"]}            → True if status != "deleted"
    """
    if not isinstance(node["$ne"], list) or len(node["$ne"]) != 2:
        raise ValueError("$ne requires a list of exactly 2 values")

    left = ctx.engine.process_value(node["$ne"][0], ctx)
    right = ctx.engine.process_value(node["$ne"][1], ctx)
    return left != right


def add_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$add`` construct: addition.

    Schema::

        {"$add": [<value1>, <value2>, ...]}

    Behavior:
    * All values are processed through ``process_value``
    * With 1 operand: returns the value itself
    * With 2+ operands: returns value1 + value2 + ... (left-to-right)

    Examples::

        {"$add": [10]}                                → 10
        {"$add": [10, 5]}                             → 15
        {"$add": [1, 2, 3, 4]}                        → 10
        {"$add": ["${/a}", {"$ref": "/b"}, 5]}        → a + b + 5
    """
    if not isinstance(node["$add"], list) or len(node["$add"]) < 1:
        raise ValueError("$add requires a list of at least 1 value")

    values = [ctx.engine.process_value(v, ctx) for v in node["$add"]]

    result = values[0]
    for val in values[1:]:
        result = result + val
    return result


def sub_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$sub`` construct: subtraction.

    Schema::

        {"$sub": [<value1>, <value2>, ...]}

    Behavior:
    * All values are processed through ``process_value``
    * With 1 operand: returns the value itself
    * With 2+ operands: returns value1 - value2 - ... (left-to-right)

    Examples::

        {"$sub": [10]}                                → 10
        {"$sub": [10, 5]}                             → 5
        {"$sub": [100, 20, 10]}                       → 70
        {"$sub": ["${/total}", {"$ref": "/discount"}]} → total - discount
    """
    if not isinstance(node["$sub"], list) or len(node["$sub"]) < 1:
        raise ValueError("$sub requires a list of at least 1 value")

    values = [ctx.engine.process_value(v, ctx) for v in node["$sub"]]

    result = values[0]
    for val in values[1:]:
        result = result - val
    return result


def mul_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$mul`` construct: multiplication.

    Schema::

        {"$mul": [<value1>, <value2>, ...]}

    Behavior:
    * All values are processed through ``process_value``
    * With 1 operand: returns the value itself
    * With 2+ operands: returns value1 * value2 * ... (left-to-right)

    Examples::

        {"$mul": [5]}                                 → 5
        {"$mul": [10, 5]}                             → 50
        {"$mul": [2, 3, 4]}                           → 24
        {"$mul": ["${/price}", {"$ref": "/quantity"}]} → price * quantity
    """
    if not isinstance(node["$mul"], list) or len(node["$mul"]) < 1:
        raise ValueError("$mul requires a list of at least 1 value")

    values = [ctx.engine.process_value(v, ctx) for v in node["$mul"]]

    result = values[0]
    for val in values[1:]:
        result = result * val
    return result


def div_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$div`` construct: division.

    Schema::

        {"$div": [<value1>, <value2>, ...]}

    Behavior:
    * All values are processed through ``process_value``
    * With 1 operand: returns the value itself
    * With 2+ operands: returns value1 / value2 / ... (left-to-right)

    Examples::

        {"$div": [10]}                                → 10
        {"$div": [10, 5]}                             → 2.0
        {"$div": [100, 2, 5]}                         → 10.0
        {"$div": ["${/total}", {"$ref": "/count"}]}   → total / count
    """
    if not isinstance(node["$div"], list) or len(node["$div"]) < 1:
        raise ValueError("$div requires a list of at least 1 value")

    values = [ctx.engine.process_value(v, ctx) for v in node["$div"]]

    result = values[0]
    for val in values[1:]:
        result = result / val
    return result


def pow_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$pow`` construct: exponentiation.

    Schema::

        {"$pow": [<value1>, <value2>, ...]}

    Behavior:
    * All values are processed through ``process_value``
    * With 1 operand: returns the value itself
    * With 2+ operands: returns value1 ** value2 ** ... (left-to-right)

    Examples::

        {"$pow": [2]}                                 → 2
        {"$pow": [2, 3]}                              → 8
        {"$pow": [2, 3, 2]}                           → 64  (i.e., (2 ** 3) ** 2)
        {"$pow": ["${/base}", {"$ref": "/exponent"}]} → base ** exponent
    """
    if not isinstance(node["$pow"], list) or len(node["$pow"]) < 1:
        raise ValueError("$pow requires a list of at least 1 value")

    values = [ctx.engine.process_value(v, ctx) for v in node["$pow"]]

    result = values[0]
    for val in values[1:]:
        result = result ** val
    return result


def mod_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$mod`` construct: modulo.

    Schema::

        {"$mod": [<value1>, <value2>, ...]}

    Behavior:
    * All values are processed through ``process_value``
    * With 1 operand: returns the value itself
    * With 2+ operands: returns value1 % value2 % ... (left-to-right)

    Examples::

        {"$mod": [10]}                                → 10
        {"$mod": [10, 3]}                             → 1
        {"$mod": [100, 7, 3]}                         → 2  (i.e., (100 % 7) % 3)
        {"$mod": ["${/value}", {"$ref": "/divisor"}]} → value % divisor
    """
    if not isinstance(node["$mod"], list) or len(node["$mod"]) < 1:
        raise ValueError("$mod requires a list of at least 1 value")

    values = [ctx.engine.process_value(v, ctx) for v in node["$mod"]]

    result = values[0]
    for val in values[1:]:
        result = result % val
    return result


def make_cast_handler(casters: Mapping[str, Any]) -> Any:
    """Factory function that creates a cast handler with registered casters.

    Args:
        casters: Dictionary mapping type names to caster functions.
                 Example: {"int": int, "float": float, "mytype": my_caster_fn}

    Returns:
        A handler function for the ``$cast`` construct.

    The returned handler processes constructs of the form::

        {"$cast": {"value": <some_value>, "type": "<type_name>"}}

    Behavior:
    * ``value`` is processed through ``process_value`` (template substitution, etc.)
    * ``type`` is the name of a registered caster
    * The caster is applied to the processed value
    * Returns the casted result
    * Raises ``KeyError`` if the type is not registered
    * Raises ``ValueError`` if the construct is malformed

    Examples::

        {"$cast": {"value": "42", "type": "int"}}          → 42
        {"$cast": {"value": "${/count}", "type": "float"}} → 3.14
        {"$cast": {"value": "1", "type": "bool"}}          → True
    """
    _casters = dict(casters)

    def cast_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$cast`` construct: apply a registered type caster to a value."""
        cast_spec = node.get("$cast")

        if not isinstance(cast_spec, Mapping):
            raise ValueError(
                f"$cast construct requires a dict with 'value' and 'type' keys, "
                f"got {type(cast_spec).__name__}: {cast_spec!r}"
            )

        if "value" not in cast_spec or "type" not in cast_spec:
            raise ValueError(
                f"$cast construct requires both 'value' and 'type' keys, "
                f"got keys: {list(cast_spec.keys())}"
            )

        # Process the value (allows templates, references, etc.)
        value = ctx.engine.process_value(cast_spec["value"], ctx)

        # Get the type name (also process it to allow dynamic type selection)
        type_name = ctx.engine.process_value(cast_spec["type"], ctx)

        if not isinstance(type_name, str):
            raise ValueError(
                f"$cast type must be a string, got {type(type_name).__name__}: {type_name!r}"
            )

        # Look up and apply the caster
        if type_name not in _casters:
            raise KeyError(
                f"Unknown cast type '{type_name}'. "
                f"Available types: {sorted(_casters.keys())}"
            )

        caster = _casters[type_name]
        return caster(value)

    return cast_handler
