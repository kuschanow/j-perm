from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from src import register_op
from utils.pointers import maybe_slice, jptr_ensure_parent
from utils.special import resolve_special
from utils.subst import substitute


@register_op("update")
def op_update(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Update a mapping at the given path using a mapping from source or inline value."""
    path = substitute(step["path"], src)
    create = bool(step.get("create", True))
    deep = bool(step.get("deep", False))

    if "from" in step:
        ptr = substitute(step["from"], src)
        try:
            update_value = copy.deepcopy(maybe_slice(ptr, src))
        except Exception:
            if "default" in step:
                update_value = copy.deepcopy(step["default"])
            else:
                raise
    elif "value" in step:
        update_value = resolve_special(step["value"], src)
        if isinstance(update_value, (str, list, Mapping)):
            update_value = substitute(update_value, src)
    else:
        raise ValueError("update operation requires either 'from' or 'value' parameter")

    if not isinstance(update_value, Mapping):
        raise TypeError(f"update value must be a dict, got {type(update_value).__name__}")

    parent, leaf = jptr_ensure_parent(dest, path, create=create)

    if leaf:
        if isinstance(parent, list):
            idx = int(leaf)
            target = parent[idx]
        else:
            if leaf not in parent:
                if create:
                    parent[leaf] = {}
                else:
                    raise KeyError(f"{path} does not exist")
            target = parent[leaf]
    else:
        target = dest

    if not isinstance(target, MutableMapping):
        raise TypeError(f"{path} is not a dict, cannot update")

    if deep:
        def deep_update(dst: MutableMapping, src_val: Mapping) -> None:
            for key, value in src_val.items():
                if key in dst and isinstance(dst[key], MutableMapping) and isinstance(value, Mapping):
                    deep_update(dst[key], value)  # type: ignore[index]
                else:
                    dst[key] = copy.deepcopy(value)

        deep_update(target, update_value)
    else:
        target.update(update_value)

    return dest
