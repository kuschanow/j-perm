"""Special construct handlers: ``$ref`` and ``$eval``.

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
"""

from __future__ import annotations

import copy
from typing import Any, Mapping

from ..core import ExecutionContext

_MISSING = object()


def ref_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    """``$ref`` construct: resolve pointer from source.

    Schema::

        {"$ref": "/path/to/value", "$default": <fallback>}

    Behavior:
    * Template substitution is applied to the ``$ref`` value itself
    * Pointer is resolved from ``ctx.source``
    * Supports slices (``/arr[1:]``) via ``ctx.resolver.get``
    * Returns deep copy to prevent aliasing
    * If pointer fails and ``$default`` exists → return ``$default``
    * Otherwise raises the original exception

    Examples::

        {"$ref": "/user/name"}
        {"$ref": "${/path}", "$default": "unknown"}
        {"$ref": "/items[2:]"}
    """
    # Expand templates in the pointer itself
    ptr = ctx.engine.process_value(node["$ref"], ctx, _unescape=False)

    dflt = node.get("$default", _MISSING)
    try:
        return copy.deepcopy(ctx.resolver.get(ptr, ctx.source))
    except Exception:
        if dflt is not _MISSING:
            return copy.deepcopy(dflt)
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
        return ctx.resolver.get(sel_ptr, result)

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
