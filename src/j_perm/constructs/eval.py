from __future__ import annotations

from typing import Mapping, Any

from j_perm.special_resolver import SpecialRegistry


@SpecialRegistry.register("$eval")
def sp_eval(node: Mapping[str, Any], src: Mapping[str, Any], engine: "ActionEngine") -> Any:
    out = engine.apply_actions(node["$eval"], dest={}, source=src)

    if "$select" in node:
        sel = resolver.slice(node["$select"], out)  # type: ignore[arg-type]
        return sel

    return out
