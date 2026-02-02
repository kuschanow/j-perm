from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..op_handler import OpRegistry


@OpRegistry.register("exec")
def op_exec(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
        engine: "ActionEngine",
) -> MutableMapping[str, Any]:
    """Execute nested actions stored either in source or inline in the op."""
    has_from = "from" in step
    has_actions = "actions" in step

    if has_from and has_actions:
        raise ValueError("exec operation cannot have both 'from' and 'actions' parameters")

    if not has_from and not has_actions:
        raise ValueError("exec operation requires either 'from' or 'actions' parameter")

    if has_from:
        actions_ptr = engine.substitutor.substitute(step["from"], src)
        try:
            actions = engine.pointer_manager.maybe_slice(actions_ptr, src)
        except Exception:
            if "default" in step:
                actions = engine.special.resolve(step["default"], src, engine)
                if isinstance(actions, (str, list, dict)):
                    actions = engine.substitutor.substitute(actions, src)
            else:
                raise ValueError(f"Cannot find actions at {actions_ptr}")
    else:
        actions = engine.special.resolve(step["actions"], src, engine)
        if isinstance(actions, (str, list, dict)):
            actions = engine.substitutor.substitute(actions, src)

    merge = bool(step.get("merge", False))

    if merge:
        result = engine.apply_actions(actions, dest=dest, source=src)
        return result
    else:
        result = engine.apply_actions(actions, dest={}, source=src)
        dest.clear()
        if isinstance(dest, list):
            dest.extend(result)  # type: ignore[arg-type]
        else:
            dest.update(result)  # type: ignore[arg-type]
        return dest
