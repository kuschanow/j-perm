from __future__ import annotations

from typing import Any, Mapping, MutableMapping

from ..op_handler import OpRegistry


@OpRegistry.register("set")
def op_set(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Set or append a value at JSON Pointer path in dest."""
    path = engine.substitutor.substitute(step["path"], src)
    create = bool(step.get("create", True))
    extend_list = bool(step.get("extend", True))

    value = engine.special.resolve(step["value"], src, engine)
    if isinstance(value, (str, list, Mapping)):
        value = engine.substitutor.substitute(value, src)

    parent, leaf = engine.pointer_manager.ensure_parent(dest, path, create=create)

    if leaf == "-":
        if not isinstance(parent, list):
            if create:
                grand, last = engine.pointer_manager.ensure_parent(dest, path.rsplit("/", 1)[0], create=True)
                if not isinstance(grand[last], list):
                    if grand[last] == {}:
                        grand[last] = []
                    else:
                        grand[last] = [grand[last]]
                parent = grand[last]
            else:
                raise TypeError(f"{path} is not a list (append)")

        if isinstance(value, list) and extend_list:
            parent.extend(value)
        else:
            parent.append(value)
    else:
        if isinstance(parent, list):
            idx = int(leaf)
            if idx >= len(parent):
                if create:
                    while idx >= len(parent):
                        parent.append(None)
                else:
                    raise IndexError(f"{path}: index {idx} out of range")
            parent[idx] = value
        else:
            parent[leaf] = value

    return dest
