from __future__ import annotations

from typing import MutableMapping, Any, Mapping

from ..registry import register_op
from ..engine import apply_actions
from ..utils.pointers import maybe_slice
from ..utils.special import resolve_special
from ..utils.subst import substitute


@register_op("exec")
def op_exec(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Execute nested actions stored either in source or inline in the op."""
    has_from = "from" in step
    has_actions = "actions" in step

    if has_from and has_actions:
        raise ValueError("exec operation cannot have both 'from' and 'actions' parameters")

    if not has_from and not has_actions:
        raise ValueError("exec operation requires either 'from' or 'actions' parameter")

    if has_from:
        actions_ptr = substitute(step["from"], src)
        try:
            actions = maybe_slice(actions_ptr, src)
        except Exception:
            if "default" in step:
                actions = resolve_special(step["default"], src)
                if isinstance(actions, (str, list, dict)):
                    actions = substitute(actions, src)
            else:
                raise ValueError(f"Cannot find actions at {actions_ptr}")
    else:
        actions = resolve_special(step["actions"], src)
        if isinstance(actions, (str, list, dict)):
            actions = substitute(actions, src)

    merge = bool(step.get("merge", False))

    if merge:
        result = apply_actions(actions, dest=dest, source=src)
        return result
    else:
        result = apply_actions(actions, dest={}, source=src)
        dest.clear()
        if isinstance(dest, list):
            dest.extend(result)  # type: ignore[arg-type]
        else:
            dest.update(result)  # type: ignore[arg-type]
        return dest
