from __future__ import annotations

import copy
from typing import Mapping, Any

from ..special_resolver import _MISSING, SpecialRegistry
from ..utils.pointers import maybe_slice


@SpecialRegistry.register("$ref")
def of_ref(node: Mapping[str, Any], src: Mapping[str, Any], engine: "ActionEngine") -> Any:
    # Expand templates inside "$ref" using configured substitutor
    ptr = engine.substitutor.substitute(node["$ref"], src)

    dflt = node.get("$default", _MISSING)
    try:
        return copy.deepcopy(maybe_slice(ptr, src))
    except Exception:
        if dflt is not _MISSING:
            return copy.deepcopy(dflt)
        raise
