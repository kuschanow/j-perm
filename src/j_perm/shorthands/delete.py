from typing import Any

from ..normalizer import ShorthandRegistry, ExpandResult


@ShorthandRegistry.register("delete")
def rule_delete(key: str, val: Any) -> ExpandResult:
    if key != "~delete":
        return None
    return [{"op": "delete", "path": p} for p in (val if isinstance(val, list) else [val])]
