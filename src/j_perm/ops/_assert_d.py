from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..op_handler import OpRegistry


@OpRegistry.register("assertD")
def op_assert_d(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Assert node existence and/or value at JSON Pointer path in dest."""
    path = engine.substitutor.substitute(step["path"], dest)

    try:
        current = engine.pointer_manager.get_pointer(dest, path)
    except Exception:
        raise AssertionError(f"'{path}' does not exist in destination")

    if "equals" in step and current != step["equals"]:
        raise AssertionError(f"'{path}' != '{step['equals']!r}'")

    return dest
