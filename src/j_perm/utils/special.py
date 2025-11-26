from __future__ import annotations

import copy
from typing import Any, Mapping

from .pointers import maybe_slice
from .subst import substitute
from ..engine import apply_actions

_MISSING = object()


def resolve_special(val: Any, src: Mapping[str, Any]):
    """Resolve $ref / $eval constructs inside an arbitrary value tree."""
    if isinstance(val, dict):
        if "$ref" in val:
            ptr = substitute(val["$ref"], src)
            dflt = val.get("$default", _MISSING)
            try:
                return copy.deepcopy(maybe_slice(ptr, src))
            except Exception:
                if dflt is not _MISSING:
                    return copy.deepcopy(dflt)
                raise

        if "$eval" in val:
            out = apply_actions(val["$eval"], dest={}, source=src)
            if "$select" in val:
                sel = maybe_slice(val["$select"], out)  # type: ignore[arg-type]
                return sel
            return out

        return {k: resolve_special(v, src) for k, v in val.items()}

    if isinstance(val, list):
        return [resolve_special(x, src) for x in val]

    if isinstance(val, tuple):
        return tuple(resolve_special(x, src) for x in val)

    return val
