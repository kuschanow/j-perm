from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from ..op_handler import OpRegistry


@OpRegistry.register("foreach")
def op_foreach(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any] | list,
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Iterate over array in source and execute nested actions for each element."""
    arr_ptr = engine.substitutor.substitute(step["in"], src, engine)

    default = copy.deepcopy(step.get("default", []))
    skip_empty = bool(step.get("skip_empty", True))

    try:
        arr = engine.pointer_manager.maybe_slice(arr_ptr, src)
    except Exception:
        arr = default

    if not arr and skip_empty:
        return dest

    if isinstance(arr, dict):
        arr = list(arr.items())

    var = step.get("as", "item")
    body = engine.normalize_actions(step["do"])
    snapshot = copy.deepcopy(dest)

    try:
        for elem in arr:
            if isinstance(src, Mapping):
                extended = dict(src)
            else:
                extended = {"_": src}

            extended[var] = elem
            dest = engine.apply_actions(body, dest=dest, source=extended)
    except Exception:
        dest.clear()
        if isinstance(dest, list):
            dest = snapshot
        else:
            dest.update(snapshot)
        raise

    return dest
