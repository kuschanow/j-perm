from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from .set import op_set
from ..op_handler import OpRegistry
from ..utils.pointers import maybe_slice


@OpRegistry.register("copy")
def op_copy(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Copy value from source pointer into dest path."""
    path = engine.substitutor.substitute(step["path"], src)
    create = bool(step.get("create", True))
    extend_list = bool(step.get("extend", True))

    ptr = engine.substitutor.substitute(step["from"], src)
    ignore = bool(step.get("ignore_missing", False))

    try:
        value = copy.deepcopy(maybe_slice(ptr, src))
    except Exception:
        if "default" in step:
            value = copy.deepcopy(step["default"])
        elif ignore:
            return dest
        else:
            raise

    return op_set(
        {"op": "set", "path": path, "value": value, "create": create, "extend": extend_list},
        dest,
        src,
    )
