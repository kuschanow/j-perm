from typing import Any

from j_perm.subst import CasterRegistry


@CasterRegistry.register("int")
def cast_int(x: Any) -> int:
    return int(x)
