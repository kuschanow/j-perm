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
import re
import regex
from typing import Any, Mapping, Tuple, Callable

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
    # Execute the nested actions with fresh dest, preserving metadata
    eval_ctx = ctx.copy(new_dest={}, deepcopy_dest=False)

    # Temporarily remove _real_dest to prevent @: references from accessing parent dest
    # This ensures eval is properly isolated
    old_real_dest = eval_ctx.metadata.pop('_real_dest', None)
    try:
        result = ctx.engine.apply_to_context(node["$eval"], eval_ctx)
    finally:
        # Restore _real_dest for parent context
        if old_real_dest is not None:
            eval_ctx.metadata['_real_dest'] = old_real_dest

    if "$select" in node:
        sel_ptr = ctx.engine.process_value(node["$select"], ctx, _unescape=False)
        # Create temporary context to resolve from eval result
        temp_ctx = ctx.copy(new_source=result)
        return ctx.engine.processor.get(sel_ptr, temp_ctx)

    return result


def and_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$and`` construct: process multiple values and return last result if all truthy.

    Schema::

        {"$and": [<values>]}

    Behavior:
    * Each value is processed in sequence
    * Returns the last result if all values are truthy
    * Returns the first falsy value if any value is falsy

    Examples::

        {"$and": [{"$ref": "/a"}, {"$ref": "/b"}]}
        {"$and": [{"$gt": [{"$ref": "/x"}, 0]}, {"$lt": [{"$ref": "/x"}, 100]}]}
    """

    last_result = None
    for action in node["$and"]:
        last_result = ctx.engine.process_value(action, ctx)
        if not last_result:
            return last_result
    return last_result


def or_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$or`` construct: process multiple values and return first truthy result.

    Schema::

        {"$or": [<values>]}

    Behavior:
    * Each value is processed in sequence
    * Returns the first truthy result
    * If all results are falsy, returns the last result

    Examples::

        {"$or": [{"$ref": "/a"}, {"$ref": "/b"}]}
        {"$or": [{"$eq": [{"$ref": "/status"}, "active"]}, {"$eq": [{"$ref": "/status"}, "pending"]}]}
    """

    last_result = None
    for action in node["$or"]:
        last_result = ctx.engine.process_value(action, ctx)
        if last_result:
            return last_result
    return last_result


def not_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$not`` construct: process a value and negate its result.

    Schema::

        {"$not": <value>}

    Behavior:
    * The value is processed
    * Returns the logical negation of the result

    Examples::

        {"$not": {"$ref": "/enabled"}}
        {"$not": {"$gt": [{"$ref": "/age"}, 18]}}
    """

    result = ctx.engine.process_value(node["$not"], ctx)
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


def make_add_handler(
    max_number_result: float = 1e15,
    max_string_result: int = 100_000_000,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$add`` handler with security limits.

    Args:
        max_number_result: Maximum absolute value for numeric results.
        max_string_result: Maximum length for string results.

    Returns:
        Handler function for ``$add`` construct.
    """
    def add_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$add`` construct: addition with security limits.

        Schema::

            {"$add": [<value1>, <value2>, ...]}

        Behavior:
        * All values are processed through ``process_value``
        * With 1 operand: returns the value itself
        * With 2+ operands: returns value1 + value2 + ... (left-to-right)
        * Numeric results limited by max_number_result
        * String results limited by max_string_result

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

            # Check limits after each addition
            if isinstance(result, (int, float)):
                if abs(result) > max_number_result:
                    raise ValueError(
                        f"Addition result {result} exceeds numeric limit of {max_number_result}"
                    )
            elif isinstance(result, str):
                if len(result) > max_string_result:
                    raise ValueError(
                        f"Addition result string length {len(result)} exceeds limit of {max_string_result}"
                    )

        return result

    return add_handler


# Default add_handler for backward compatibility
add_handler = make_add_handler()


def make_sub_handler(
    max_number_result: float = 1e15,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$sub`` handler with security limits.

    Args:
        max_number_result: Maximum absolute value for numeric results.

    Returns:
        Handler function for ``$sub`` construct.
    """
    def sub_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$sub`` construct: subtraction with security limits.

        Schema::

            {"$sub": [<value1>, <value2>, ...]}

        Behavior:
        * All values are processed through ``process_value``
        * With 1 operand: returns the value itself
        * With 2+ operands: returns value1 - value2 - ... (left-to-right)
        * Numeric results limited by max_number_result

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

            # Check limits after each subtraction
            if isinstance(result, (int, float)):
                if abs(result) > max_number_result:
                    raise ValueError(
                        f"Subtraction result {result} exceeds numeric limit of {max_number_result}"
                    )

        return result

    return sub_handler


# Default sub_handler for backward compatibility
sub_handler = make_sub_handler()


def make_mul_handler(
    max_string_result: int = 1_000_000,
    max_operand: float = 1e9,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$mul`` handler with security limits.

    Args:
        max_string_result: Maximum length of resulting string when multiplying string by number.
        max_operand: Maximum absolute value for numeric operands.

    Returns:
        Handler function for ``$mul`` construct.
    """
    def mul_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$mul`` construct: multiplication with security limits.

        Schema::

            {"$mul": [<value1>, <value2>, ...]}

        Behavior:
        * All values are processed through ``process_value``
        * With 1 operand: returns the value itself
        * With 2+ operands: returns value1 * value2 * ... (left-to-right)
        * String multiplication is limited by max_string_result
        * Numeric operands are limited by max_operand

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
            # Check for string multiplication DoS
            if isinstance(result, str) and isinstance(val, (int, float)):
                potential_length = len(result) * abs(val)
                if potential_length > max_string_result:
                    raise ValueError(
                        f"String multiplication would create string of length {potential_length}, "
                        f"exceeding limit of {max_string_result}"
                    )
            elif isinstance(val, str) and isinstance(result, (int, float)):
                potential_length = len(val) * abs(result)
                if potential_length > max_string_result:
                    raise ValueError(
                        f"String multiplication would create string of length {potential_length}, "
                        f"exceeding limit of {max_string_result}"
                    )
            # Check numeric operand limits
            elif isinstance(result, (int, float)) and isinstance(val, (int, float)):
                if abs(val) > max_operand:
                    raise ValueError(
                        f"Numeric operand {val} exceeds limit of {max_operand}"
                    )

            result = result * val

        return result

    return mul_handler


# Default mul_handler for backward compatibility
mul_handler = make_mul_handler()


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


def make_pow_handler(
    max_base: float = 1e6,
    max_exponent: float = 1000,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$pow`` handler with security limits.

    Args:
        max_base: Maximum absolute value for base.
        max_exponent: Maximum absolute value for exponent.

    Returns:
        Handler function for ``$pow`` construct.
    """
    def pow_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$pow`` construct: exponentiation with security limits.

        Schema::

            {"$pow": [<value1>, <value2>, ...]}

        Behavior:
        * All values are processed through ``process_value``
        * With 1 operand: returns the value itself
        * With 2+ operands: returns value1 ** value2 ** ... (left-to-right)
        * Base values are limited by max_base
        * Exponent values are limited by max_exponent

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

        # Check first value (base)
        if isinstance(result, (int, float)) and abs(result) > max_base:
            raise ValueError(
                f"Base value {result} exceeds limit of {max_base}"
            )

        for val in values[1:]:
            # Check exponent
            if isinstance(val, (int, float)) and abs(val) > max_exponent:
                raise ValueError(
                    f"Exponent value {val} exceeds limit of {max_exponent}"
                )

            result = result ** val

            # Check intermediate result if it becomes a new base
            if isinstance(result, (int, float)) and abs(result) > max_base:
                raise ValueError(
                    f"Intermediate result {result} exceeds base limit of {max_base}"
                )

        return result

    return pow_handler


# Default pow_handler for backward compatibility
pow_handler = make_pow_handler()


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


def in_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$in`` construct: membership test (like Python's ``in`` operator).

    Schema::

        {"$in": [<value>, <container>]}

    Behavior:
    * Both values are processed through ``process_value``
    * For strings: checks if value is a substring of container
    * For lists/tuples: checks if value is in the list
    * For dicts: checks if value is a key in the dict
    * Returns boolean

    Examples::

        {"$in": ["world", "hello world"]}             → True (substring)
        {"$in": [2, [1, 2, 3]]}                       → True (element in list)
        {"$in": ["key", {"key": "value"}]}            → True (key in dict)
        {"$in": ["x", "hello"]}                       → False
        {"$in": [{"$ref": "/search"}, "${/text}"]}    → True/False
    """
    if not isinstance(node["$in"], list) or len(node["$in"]) != 2:
        raise ValueError("$in requires a list of exactly 2 values: [value, container]")

    value = ctx.engine.process_value(node["$in"][0], ctx)
    container = ctx.engine.process_value(node["$in"][1], ctx)

    return value in container


# ─────────────────────────────────────────────────────────────────────────────
# String operations
# ─────────────────────────────────────────────────────────────────────────────


def make_str_split_handler(
    max_results: int = 100_000,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$str_split`` handler with security limits.

    Args:
        max_results: Maximum number of split results allowed.

    Returns:
        Handler function for ``$str_split`` construct.
    """
    def str_split_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$str_split`` construct: split string by delimiter.

        Schema::

            {"$str_split": {"string": <str>, "delimiter": <str>}}
            {"$str_split": {"string": <str>, "delimiter": <str>, "maxsplit": <int>}}

        Behavior:
        * Splits string by delimiter
        * Optional maxsplit parameter (default: -1, split all)
        * Returns list of strings
        * Limited to max_results items

        Examples::

            {"$str_split": {"string": "a,b,c", "delimiter": ","}}  → ["a", "b", "c"]
            {"$str_split": {"string": "a b c", "delimiter": " "}}  → ["a", "b", "c"]
            {"$str_split": {"string": "a:b:c", "delimiter": ":", "maxsplit": 1}}  → ["a", "b:c"]
        """
        spec = node["$str_split"]
        if isinstance(spec, str):
            raise ValueError("$str_split requires a dict with 'string' and 'delimiter'")

        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        delimiter = ctx.engine.process_value(spec.get("delimiter", " "), ctx)
        maxsplit = ctx.engine.process_value(spec.get("maxsplit", -1), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$str_split 'string' must be a string, got {type(string).__name__}")

        # Limit maxsplit to prevent DoS
        if maxsplit < 0 or maxsplit > max_results:
            maxsplit = max_results

        result = string.split(delimiter, maxsplit)

        # Check result size
        if len(result) > max_results:
            raise ValueError(
                f"Split operation would create {len(result)} items, "
                f"exceeding limit of {max_results}"
            )

        return result

    return str_split_handler


# Default str_split_handler for backward compatibility
str_split_handler = make_str_split_handler()


def make_str_join_handler(
    max_result_length: int = 10_000_000,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$str_join`` handler with security limits.

    Args:
        max_result_length: Maximum length of joined string.

    Returns:
        Handler function for ``$str_join`` construct.
    """
    def str_join_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$str_join`` construct: join list of strings with separator.

        Schema::

            {"$str_join": {"array": <list>, "separator": <str>}}

        Behavior:
        * Joins array elements with separator
        * All elements are converted to strings
        * Returns string
        * Limited to max_result_length

        Examples::

            {"$str_join": {"array": ["a", "b", "c"], "separator": "-"}}  → "a-b-c"
            {"$str_join": {"array": [1, 2, 3], "separator": ","}}  → "1,2,3"
        """
        spec = node["$str_join"]
        if isinstance(spec, str):
            raise ValueError("$str_join requires a dict with 'array' and 'separator'")

        array = ctx.engine.process_value(spec.get("array", []), ctx)
        separator = ctx.engine.process_value(spec.get("separator", ""), ctx)

        if not isinstance(array, (list, tuple)):
            raise ValueError(f"$str_join 'array' must be a list, got {type(array).__name__}")

        # Estimate result length before joining
        array_len = len(array)
        if array_len > 0:
            # Convert items to strings and calculate total length
            str_items = [str(item) for item in array]
            total_length = sum(len(s) for s in str_items) + len(separator) * (array_len - 1)

            if total_length > max_result_length:
                raise ValueError(
                    f"Join operation would create string of length {total_length}, "
                    f"exceeding limit of {max_result_length}"
                )

            return separator.join(str_items)

        return ""

    return str_join_handler


# Default str_join_handler for backward compatibility
str_join_handler = make_str_join_handler()


def str_slice_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_slice`` construct: extract substring using slice notation.

    Schema::

        {"$str_slice": {"string": <str>, "start": <int>, "end": <int>}}
        {"$str_slice": {"string": <str>, "start": <int>}}
        {"$str_slice": {"string": <str>, "end": <int>}}

    Behavior:
    * Extracts substring using Python slice notation
    * start: starting index (default: 0)
    * end: ending index (default: None, end of string)
    * Supports negative indices

    Examples::

        {"$str_slice": {"string": "hello", "start": 1, "end": 4}}  → "ell"
        {"$str_slice": {"string": "hello", "start": 2}}  → "llo"
        {"$str_slice": {"string": "hello", "end": 3}}  → "hel"
        {"$str_slice": {"string": "hello", "start": -3}}  → "llo"
    """
    spec = node["$str_slice"]
    if isinstance(spec, str):
        raise ValueError("$str_slice requires a dict with 'string' and slice parameters")

    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    start = ctx.engine.process_value(spec.get("start"), ctx)
    end = ctx.engine.process_value(spec.get("end"), ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_slice 'string' must be a string, got {type(string).__name__}")

    return string[start:end]


def str_upper_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_upper`` construct: convert string to uppercase.

    Schema::

        {"$str_upper": <string>}

    Examples::

        {"$str_upper": "hello"}  → "HELLO"
        {"$str_upper": "${/text}"}  → uppercased text
    """
    string = ctx.engine.process_value(node["$str_upper"], ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_upper requires a string, got {type(string).__name__}")

    return string.upper()


def str_lower_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_lower`` construct: convert string to lowercase.

    Schema::

        {"$str_lower": <string>}

    Examples::

        {"$str_lower": "HELLO"}  → "hello"
        {"$str_lower": "${/text}"}  → lowercased text
    """
    string = ctx.engine.process_value(node["$str_lower"], ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_lower requires a string, got {type(string).__name__}")

    return string.lower()


def str_strip_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_strip`` construct: remove leading and trailing characters.

    Schema::

        {"$str_strip": <string>}  // strips whitespace
        {"$str_strip": {"string": <str>, "chars": <str>}}  // strips specified chars

    Behavior:
    * Removes leading and trailing characters
    * chars: characters to remove (default: whitespace)

    Examples::

        {"$str_strip": "  hello  "}  → "hello"
        {"$str_strip": {"string": "***hello***", "chars": "*"}}  → "hello"
        {"$str_strip": {"string": "xyzabcxyz", "chars": "xyz"}}  → "abc"
    """
    spec = node["$str_strip"]

    if isinstance(spec, str):
        # Simple form: just strip whitespace
        string = ctx.engine.process_value(spec, ctx)
        if not isinstance(string, str):
            raise ValueError(f"$str_strip requires a string, got {type(string).__name__}")
        return string.strip()

    # Dict form with chars parameter
    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    chars = ctx.engine.process_value(spec.get("chars"), ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_strip 'string' must be a string, got {type(string).__name__}")

    return string.strip(chars)


def str_lstrip_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_lstrip`` construct: remove leading characters.

    Schema::

        {"$str_lstrip": <string>}  // strips whitespace
        {"$str_lstrip": {"string": <str>, "chars": <str>}}

    Examples::

        {"$str_lstrip": "  hello  "}  → "hello  "
        {"$str_lstrip": {"string": "___hello", "chars": "_"}}  → "hello"
    """
    spec = node["$str_lstrip"]

    if isinstance(spec, str):
        string = ctx.engine.process_value(spec, ctx)
        if not isinstance(string, str):
            raise ValueError(f"$str_lstrip requires a string, got {type(string).__name__}")
        return string.lstrip()

    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    chars = ctx.engine.process_value(spec.get("chars"), ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_lstrip 'string' must be a string, got {type(string).__name__}")

    return string.lstrip(chars)


def str_rstrip_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_rstrip`` construct: remove trailing characters.

    Schema::

        {"$str_rstrip": <string>}  // strips whitespace
        {"$str_rstrip": {"string": <str>, "chars": <str>}}

    Examples::

        {"$str_rstrip": "  hello  "}  → "  hello"
        {"$str_rstrip": {"string": "hello___", "chars": "_"}}  → "hello"
    """
    spec = node["$str_rstrip"]

    if isinstance(spec, str):
        string = ctx.engine.process_value(spec, ctx)
        if not isinstance(string, str):
            raise ValueError(f"$str_rstrip requires a string, got {type(string).__name__}")
        return string.rstrip()

    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    chars = ctx.engine.process_value(spec.get("chars"), ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_rstrip 'string' must be a string, got {type(string).__name__}")

    return string.rstrip(chars)


def make_str_replace_handler(
    max_result_length: int = 10_000_000,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$str_replace`` handler with security limits.

    Args:
        max_result_length: Maximum length of result string.

    Returns:
        Handler function for ``$str_replace`` construct.
    """
    def str_replace_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$str_replace`` construct: replace substring.

        Schema::

            {"$str_replace": {"string": <str>, "old": <str>, "new": <str>}}
            {"$str_replace": {"string": <str>, "old": <str>, "new": <str>, "count": <int>}}

        Behavior:
        * Replaces occurrences of old substring with new substring
        * count: max number of replacements (default: all)
        * Limited to max_result_length

        Examples::

            {"$str_replace": {"string": "hello", "old": "ll", "new": "rr"}}  → "herro"
            {"$str_replace": {"string": "aaa", "old": "a", "new": "b", "count": 2}}  → "bba"
        """
        spec = node["$str_replace"]

        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        old = ctx.engine.process_value(spec["old"], ctx)
        new = ctx.engine.process_value(spec["new"], ctx)
        count = ctx.engine.process_value(spec.get("count", -1), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$str_replace 'string' must be a string, got {type(string).__name__}")

        # Estimate result length
        if old:
            occurrences = string.count(old)
            if count >= 0:
                occurrences = min(occurrences, count)

            # Calculate potential result length
            # result_len = original_len - (occurrences * len(old)) + (occurrences * len(new))
            estimated_length = len(string) - (occurrences * len(old)) + (occurrences * len(new))

            if estimated_length > max_result_length:
                raise ValueError(
                    f"Replace operation would create string of length {estimated_length}, "
                    f"exceeding limit of {max_result_length}"
                )

        return string.replace(old, new, count)

    return str_replace_handler


# Default str_replace_handler for backward compatibility
str_replace_handler = make_str_replace_handler()


def str_contains_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_contains`` construct: check if string contains substring.

    Schema::

        {"$str_contains": {"string": <str>, "substring": <str>}}

    Examples::

        {"$str_contains": {"string": "hello world", "substring": "world"}}  → True
        {"$str_contains": {"string": "hello", "substring": "x"}}  → False
    """
    spec = node["$str_contains"]

    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    substring = ctx.engine.process_value(spec["substring"], ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_contains 'string' must be a string, got {type(string).__name__}")

    return substring in string


def str_startswith_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_startswith`` construct: check if string starts with prefix.

    Schema::

        {"$str_startswith": {"string": <str>, "prefix": <str>}}

    Examples::

        {"$str_startswith": {"string": "hello", "prefix": "he"}}  → True
        {"$str_startswith": {"string": "hello", "prefix": "x"}}  → False
    """
    spec = node["$str_startswith"]

    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    prefix = ctx.engine.process_value(spec["prefix"], ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_startswith 'string' must be a string, got {type(string).__name__}")

    return string.startswith(prefix)


def str_endswith_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$str_endswith`` construct: check if string ends with suffix.

    Schema::

        {"$str_endswith": {"string": <str>, "suffix": <str>}}

    Examples::

        {"$str_endswith": {"string": "hello", "suffix": "lo"}}  → True
        {"$str_endswith": {"string": "hello", "suffix": "x"}}  → False
    """
    spec = node["$str_endswith"]

    string = ctx.engine.process_value(spec.get("string", ""), ctx)
    suffix = ctx.engine.process_value(spec["suffix"], ctx)

    if not isinstance(string, str):
        raise ValueError(f"$str_endswith 'string' must be a string, got {type(string).__name__}")

    return string.endswith(suffix)


# ─────────────────────────────────────────────────────────────────────────────
# Regular expressions
# ─────────────────────────────────────────────────────────────────────────────


def make_regex_match_handler(
    timeout: float = 2.0,
    allowed_flags: int | None = None,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$regex_match`` handler with security limits.

    Args:
        timeout: Timeout in seconds for regex operations.
        allowed_flags: Bitmask of allowed flags. None means default safe flags
                      (IGNORECASE, MULTILINE, DOTALL, VERBOSE, ASCII).
                      Use -1 to allow all flags (not recommended for untrusted input).

    Returns:
        Handler function for ``$regex_match`` construct.
    """
    # Default allowed flags: IGNORECASE, MULTILINE, DOTALL, VERBOSE, ASCII
    if allowed_flags is None:
        allowed_flags = re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE | re.ASCII
    elif allowed_flags == -1:
        # Special value to allow all flags (not recommended for untrusted input)
        allowed_flags = 0xFFFFFFFF  # Allow all possible flags

    def regex_match_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$regex_match`` construct: check if string matches regex pattern.

        Schema::

            {"$regex_match": {"pattern": <str>, "string": <str>}}
            {"$regex_match": {"pattern": <str>, "string": <str>, "flags": <int>}}

        Behavior:
        * Checks if entire string matches the pattern (using regex.fullmatch with timeout)
        * Returns True/False
        * Optional flags parameter (limited to allowed_flags)

        Examples::

            {"$regex_match": {"pattern": "^\\\\d+$", "string": "123"}}  → True
            {"$regex_match": {"pattern": "^\\\\d+$", "string": "abc"}}  → False
            {"$regex_match": {"pattern": "^hello$", "string": "HELLO", "flags": 2}}  → True (IGNORECASE)
        """
        spec = node["$regex_match"]

        pattern = ctx.engine.process_value(spec["pattern"], ctx)
        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        flags = ctx.engine.process_value(spec.get("flags", 0), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$regex_match 'string' must be a string, got {type(string).__name__}")

        # Validate flags
        if flags & ~allowed_flags:
            raise ValueError(
                f"Regex flags {flags} contain disallowed flags. "
                f"Allowed flags bitmask: {allowed_flags}"
            )

        try:
            return bool(regex.fullmatch(pattern, string, flags, timeout=timeout))
        except TimeoutError:
            raise TimeoutError(f"Regex operation exceeded timeout of {timeout}s")

    return regex_match_handler


# Default regex_match_handler for backward compatibility
regex_match_handler = make_regex_match_handler()


def make_regex_search_handler(
    timeout: float = 2.0,
    allowed_flags: int | None = None,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$regex_search`` handler with security limits.

    Args:
        timeout: Timeout in seconds for regex operations.
        allowed_flags: Bitmask of allowed flags. None means default safe flags.
                      Use -1 to allow all flags (not recommended for untrusted input).
    """
    if allowed_flags is None:
        allowed_flags = re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE | re.ASCII
    elif allowed_flags == -1:
        allowed_flags = 0xFFFFFFFF

    def regex_search_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$regex_search`` construct: search for first occurrence of pattern.

        Schema::

            {"$regex_search": {"pattern": <str>, "string": <str>}}
            {"$regex_search": {"pattern": <str>, "string": <str>, "flags": <int>}}

        Behavior:
        * Searches for first occurrence of pattern in string
        * Returns the matched string, or None if not found

        Examples::

            {"$regex_search": {"pattern": "\\\\d+", "string": "abc123def"}}  → "123"
            {"$regex_search": {"pattern": "\\\\d+", "string": "abc"}}  → None
        """
        spec = node["$regex_search"]

        pattern = ctx.engine.process_value(spec["pattern"], ctx)
        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        flags = ctx.engine.process_value(spec.get("flags", 0), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$regex_search 'string' must be a string, got {type(string).__name__}")

        if flags & ~allowed_flags:
            raise ValueError(
                f"Regex flags {flags} contain disallowed flags. "
                f"Allowed flags bitmask: {allowed_flags}"
            )

        try:
            match = regex.search(pattern, string, flags, timeout=timeout)
            return match.group(0) if match else None
        except TimeoutError:
            raise TimeoutError(f"Regex operation exceeded timeout of {timeout}s")

    return regex_search_handler


regex_search_handler = make_regex_search_handler()


def make_regex_findall_handler(
    timeout: float = 2.0,
    allowed_flags: int | None = None,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$regex_findall`` handler with security limits.

    Args:
        timeout: Timeout in seconds for regex operations.
        allowed_flags: Bitmask of allowed flags. None means default safe flags.
                      Use -1 to allow all flags (not recommended for untrusted input).
    """
    if allowed_flags is None:
        allowed_flags = re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE | re.ASCII
    elif allowed_flags == -1:
        allowed_flags = 0xFFFFFFFF

    def regex_findall_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$regex_findall`` construct: find all occurrences of pattern.

        Schema::

            {"$regex_findall": {"pattern": <str>, "string": <str>}}
            {"$regex_findall": {"pattern": <str>, "string": <str>, "flags": <int>}}

        Behavior:
        * Finds all non-overlapping occurrences of pattern
        * Returns list of matched strings

        Examples::

            {"$regex_findall": {"pattern": "\\\\d+", "string": "a1b2c3"}}  → ["1", "2", "3"]
            {"$regex_findall": {"pattern": "\\\\d+", "string": "abc"}}  → []
        """
        spec = node["$regex_findall"]

        pattern = ctx.engine.process_value(spec["pattern"], ctx)
        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        flags = ctx.engine.process_value(spec.get("flags", 0), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$regex_findall 'string' must be a string, got {type(string).__name__}")

        if flags & ~allowed_flags:
            raise ValueError(
                f"Regex flags {flags} contain disallowed flags. "
                f"Allowed flags bitmask: {allowed_flags}"
            )

        try:
            return regex.findall(pattern, string, flags, timeout=timeout)
        except TimeoutError:
            raise TimeoutError(f"Regex operation exceeded timeout of {timeout}s")

    return regex_findall_handler


regex_findall_handler = make_regex_findall_handler()


def make_regex_replace_handler(
    timeout: float = 2.0,
    allowed_flags: int | None = None,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$regex_replace`` handler with security limits.

    Args:
        timeout: Timeout in seconds for regex operations.
        allowed_flags: Bitmask of allowed flags. None means default safe flags.
                      Use -1 to allow all flags (not recommended for untrusted input).
    """
    if allowed_flags is None:
        allowed_flags = re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE | re.ASCII
    elif allowed_flags == -1:
        allowed_flags = 0xFFFFFFFF

    def regex_replace_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$regex_replace`` construct: replace pattern matches in string.

        Schema::

            {"$regex_replace": {"pattern": <str>, "replacement": <str>, "string": <str>}}
            {"$regex_replace": {"pattern": <str>, "replacement": <str>, "string": <str>, "count": <int>}}
            {"$regex_replace": {"pattern": <str>, "replacement": <str>, "string": <str>, "flags": <int>}}

        Behavior:
        * Replaces occurrences of pattern with replacement
        * count: max number of replacements (default: all)
        * Supports backreferences in replacement (\\1, \\2, etc.)

        Examples::

            {"$regex_replace": {"pattern": "\\\\d+", "replacement": "X", "string": "a1b2c3"}}  → "aXbXcX"
            {"$regex_replace": {"pattern": "(\\\\w+)@(\\\\w+)", "replacement": "\\\\1 AT \\\\2", "string": "user@domain"}}  → "user AT domain"
            {"$regex_replace": {"pattern": "\\\\d+", "replacement": "X", "string": "a1b2c3", "count": 2}}  → "aXbXc3"
        """
        spec = node["$regex_replace"]

        pattern = ctx.engine.process_value(spec["pattern"], ctx)
        replacement = ctx.engine.process_value(spec["replacement"], ctx)
        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        count = ctx.engine.process_value(spec.get("count", 0), ctx)
        flags = ctx.engine.process_value(spec.get("flags", 0), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$regex_replace 'string' must be a string, got {type(string).__name__}")

        if flags & ~allowed_flags:
            raise ValueError(
                f"Regex flags {flags} contain disallowed flags. "
                f"Allowed flags bitmask: {allowed_flags}"
            )

        try:
            return regex.sub(pattern, replacement, string, count, flags, timeout=timeout)
        except TimeoutError:
            raise TimeoutError(f"Regex operation exceeded timeout of {timeout}s")

    return regex_replace_handler


regex_replace_handler = make_regex_replace_handler()


def make_regex_groups_handler(
    timeout: float = 2.0,
    allowed_flags: int | None = None,
) -> Callable[[Mapping[str, Any], ExecutionContext], Any]:
    """Factory for ``$regex_groups`` handler with security limits.

    Args:
        timeout: Timeout in seconds for regex operations.
        allowed_flags: Bitmask of allowed flags. None means default safe flags.
                      Use -1 to allow all flags (not recommended for untrusted input).
    """
    if allowed_flags is None:
        allowed_flags = re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE | re.ASCII
    elif allowed_flags == -1:
        allowed_flags = 0xFFFFFFFF

    def regex_groups_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
        """``$regex_groups`` construct: extract capture groups from pattern match.

        Schema::

            {"$regex_groups": {"pattern": <str>, "string": <str>}}
            {"$regex_groups": {"pattern": <str>, "string": <str>, "flags": <int>}}

        Behavior:
        * Searches for pattern and returns list of captured groups
        * Returns empty list if no match found
        * Does not include group 0 (full match)

        Examples::

            {"$regex_groups": {"pattern": "(\\\\w+)@(\\\\w+)", "string": "user@domain"}}  → ["user", "domain"]
            {"$regex_groups": {"pattern": "(\\\\d+)-(\\\\d+)", "string": "123-456"}}  → ["123", "456"]
            {"$regex_groups": {"pattern": "\\\\d+", "string": "abc"}}  → []
        """
        spec = node["$regex_groups"]

        pattern = ctx.engine.process_value(spec["pattern"], ctx)
        string = ctx.engine.process_value(spec.get("string", ""), ctx)
        flags = ctx.engine.process_value(spec.get("flags", 0), ctx)

        if not isinstance(string, str):
            raise ValueError(f"$regex_groups 'string' must be a string, got {type(string).__name__}")

        if flags & ~allowed_flags:
            raise ValueError(
                f"Regex flags {flags} contain disallowed flags. "
                f"Allowed flags bitmask: {allowed_flags}"
            )

        try:
            match = regex.search(pattern, string, flags, timeout=timeout)
            return list(match.groups()) if match else []
        except TimeoutError:
            raise TimeoutError(f"Regex operation exceeded timeout of {timeout}s")

    return regex_groups_handler


regex_groups_handler = make_regex_groups_handler()


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
