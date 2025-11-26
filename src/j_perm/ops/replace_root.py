from __future__ import annotations

import copy

from ..registry import register_op
from ..utils.special import resolve_special
from ..utils.subst import substitute


@register_op("replace_root")
def op_replace_root(step, dest, src):
    """Replace the whole dest root value with the resolved special value."""
    value = resolve_special(step["value"], src)
    if isinstance(value, (str, list, dict)):
        value = substitute(value, src)
    return copy.deepcopy(value)
