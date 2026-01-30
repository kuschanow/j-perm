from collections.abc import Mapping
from typing import Any

from ..normalizer import ShorthandRegistry, ExpandResult


@ShorthandRegistry.register("assert")
def rule_assert(key: str, val: Any) -> ExpandResult:
    if key != "~assert" and key != "~assertD":
        return None
    if key == "~assertD":
        op = "assertD"
    else:
        op = "assert"
    if isinstance(val, Mapping):
        return [{"op": op, "path": p, "equals": eq} for p, eq in val.items()]
    return [{"op": op, "path": p} for p in (val if isinstance(val, list) else [val])]
