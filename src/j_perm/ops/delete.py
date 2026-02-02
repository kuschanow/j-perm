from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..op_handler import OpRegistry


@OpRegistry.register("delete")
def op_delete(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Delete node at the given JSON Pointer path in dest."""
    path = engine.substitutor.substitute(step["path"], src)
    ignore = bool(step.get("ignore_missing", True))

    try:
        parent, leaf = engine.pointer_manager.ensure_parent(dest, path, create=False)

        if leaf == "-":
            raise ValueError("'-' not allowed in delete")

        if isinstance(parent, list):
            del parent[int(leaf)]
        else:
            del parent[leaf]

    except (KeyError, IndexError):
        if not ignore:
            raise

    return dest
