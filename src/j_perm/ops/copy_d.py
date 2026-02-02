from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from .set import op_set
from ..op_handler import OpRegistry


@OpRegistry.register("copyD")
def op_copy_d(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Copy value from dest (self) into another dest path."""
    path = engine.substitutor.substitute(step["path"], src)
    create = bool(step.get("create", True))

    ptr = engine.substitutor.substitute(step["from"], dest)
    ignore = bool(step.get("ignore_missing", False))

    try:
        value = copy.deepcopy(engine.pointer_manager.maybe_slice(ptr, dest))
    except Exception:
        if "default" in step:
            value = copy.deepcopy(step["default"])
        elif ignore:
            return dest
        else:
            raise

    return op_set({"op": "set", "path": path, "value": value, "create": create}, dest, src)
