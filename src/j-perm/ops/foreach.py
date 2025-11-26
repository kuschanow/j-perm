from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from src import register_op, normalize_actions, apply_actions
from utils.pointers import maybe_slice
from utils.subst import substitute


@register_op("foreach")
def op_foreach(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any] | list,
) -> MutableMapping[str, Any]:
    """Iterate over array in source and execute nested actions for each element."""
    arr_ptr = substitute(step["in"], src)

    default = copy.deepcopy(step.get("default", []))
    skip_empty = bool(step.get("skip_empty", True))

    try:
        arr = maybe_slice(arr_ptr, src)
    except Exception:
        arr = default

    if not arr and skip_empty:
        return dest

    if isinstance(arr, dict):
        arr = list(arr.items())

    var = step.get("as", "item")
    body = normalize_actions(step["do"])
    snapshot = copy.deepcopy(dest)

    try:
        for elem in arr:
            if isinstance(src, Mapping):
                extended = dict(src)
            else:
                extended = {"_": src}

            extended[var] = elem
            dest = apply_actions(body, dest=dest, source=extended)
    except Exception:
        dest.clear()
        if isinstance(dest, list):
            dest = snapshot
        else:
            dest.update(snapshot)
        raise

    return dest
