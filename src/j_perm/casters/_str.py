from typing import Any

from j_perm.subst import CasterRegistry


@CasterRegistry.register("str")
def cast_str(x: Any) -> str:
    return str(x)
