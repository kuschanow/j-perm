from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from .set import op_set
from src import register_op
from utils.pointers import maybe_slice
from utils.subst import substitute


@register_op("copyD")
def op_copy_d(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Copy value from dest (self) into another dest path."""
    path = substitute(step["path"], src)
    create = bool(step.get("create", True))

    ptr = substitute(step["from"], dest)
    ignore = bool(step.get("ignore_missing", False))

    try:
        value = copy.deepcopy(maybe_slice(ptr, dest))
    except Exception:
        if "default" in step:
            value = copy.deepcopy(step["default"])
        elif ignore:
            return dest
        else:
            raise

    return op_set({"op": "set", "path": path, "value": value, "create": create}, dest, src)
