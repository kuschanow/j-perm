from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..op_handler import OpRegistry
from ..utils.pointers import jptr_get


@OpRegistry.register("distinct")
def op_distinct(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Remove duplicates from a list at the given path, preserving order."""
    path = engine.substitutor.substitute(step["path"], src)
    lst = jptr_get(dest, path)

    if not isinstance(lst, list):
        raise TypeError(f"{path} is not a list (distinct)")

    key = step.get("key", None)
    key_path = engine.substitutor.substitute(key, src)

    seen = set()
    unique = []
    for item in lst:
        if key is not None:
            filter_item = jptr_get(item, key_path)
        else:
            filter_item = item

        if filter_item not in seen:
            seen.add(filter_item)
            unique.append(item)

    lst[:] = unique
    return dest
