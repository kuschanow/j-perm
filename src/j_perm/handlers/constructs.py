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
