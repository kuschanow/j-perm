from __future__ import annotations

import copy
from typing import MutableMapping, Any, Mapping

from src import register_op, normalize_actions, apply_actions
from utils.pointers import maybe_slice
from utils.subst import substitute


@register_op("if")
def op_if(
        step: dict,
        dest: MutableMapping[str, Any],
        src: Mapping[str, Any] | list,
) -> MutableMapping[str, Any]:
    """Conditionally execute nested actions based on a path or expression."""
    if "path" in step:
        try:
            ptr = substitute(step["path"], src)
            current = maybe_slice(ptr, dest)
            missing = False
        except Exception:
            current = None
            missing = True

        if "equals" in step:
            expected = substitute(step["equals"], src)
            cond_val = current == expected and not missing
        elif step.get("exists"):
            cond_val = not missing
        else:
            cond_val = bool(current) and not missing
    else:
        raw_cond = substitute(step.get("cond"), src)
        cond_val = bool(raw_cond)

    branch_key = "then" if cond_val else "else"
    branch_key = branch_key if branch_key in step else "do" if cond_val else None
    actions = step.get(branch_key)

    if not actions:
        return dest

    actions_norm = normalize_actions(actions)
    snapshot = copy.deepcopy(dest)

    try:
        return apply_actions(actions_norm, dest=dest, source=src)
    except Exception:
        dest.clear()
        dest.update(snapshot)
        raise
