from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..registry import register_op
from ..utils.pointers import jptr_get
from ..utils.subst import substitute


@register_op("distinct")
def op_distinct(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Remove duplicates from a list at the given path, preserving order."""
    path = substitute(step["path"], src)
    lst = jptr_get(dest, path)

    if not isinstance(lst, list):
        raise TypeError(f"{path} is not a list (distinct)")

    seen = set()
    unique = []
    for item in lst:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    lst[:] = unique
    return dest
