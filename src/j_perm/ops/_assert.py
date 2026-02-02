from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..op_handler import OpRegistry


@OpRegistry.register("assert")
def op_assert(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Assert node existence and/or value at JSON Pointer path in dest."""
    path = engine.substitutor.substitute(step["path"], src)

    try:
        current = engine.pointer_manager.get_pointer(src, path)
    except Exception:
        raise AssertionError(f"'{path}' does not exist in source")

    if "equals" in step and current != step["equals"]:
        raise AssertionError(f"'{path}' != '{step['equals']!r}'")

    return dest
