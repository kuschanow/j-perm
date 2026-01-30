from typing import Any

from ..normalizer import ExpandResult, ShorthandRegistry


@ShorthandRegistry.register("assign_or_append", priority=-1)
def rule_assign_or_append(key: str, val: Any) -> ExpandResult:
    # Fallback rule: handles:
    # - append shorthand: field[] -> "/field/-"
    # - pointer assignment -> copy
    # - literal assignment -> set
    append = key.endswith("[]")
    dst = f"{key[:-2]}/-" if append else key

    if isinstance(val, str) and val.startswith("/"):
        return [{"op": "copy", "from": val, "path": dst, "ignore_missing": True}]
    return [{"op": "set", "path": dst, "value": val}]
