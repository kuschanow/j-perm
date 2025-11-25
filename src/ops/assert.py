from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from src import register_op
from utils.pointers import jptr_get
from utils.subst import substitute


@register_op("assert")
def op_assert(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Assert node existence and/or value at JSON Pointer path in dest."""
    path = substitute(step["path"], src)

    try:
        current = jptr_get(dest, path)
    except Exception:
        raise AssertionError(f"{path} does not exist")

    if "equals" in step and current != step["equals"]:
        raise AssertionError(f"{path} != {step['equals']!r}")

    return dest
