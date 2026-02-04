from __future__ import annotations

import copy

from ..op_handler import OpRegistry


@OpRegistry.register("replace_root")
def op_replace_root(step, dest, src, engine):
    """Replace the whole dest root value with the resolved special value."""
    value = engine.special.resolve(step["value"], src, engine)
    if isinstance(value, (str, list, dict)):
        value = engine.substitutor.substitute(value, src, engine)
    return copy.deepcopy(value)
