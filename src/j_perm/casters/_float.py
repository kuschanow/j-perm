from typing import Any

from j_perm.subst import CasterRegistry


@CasterRegistry.register("float")
def cast_float(x: Any) -> float:
    return float(x)
