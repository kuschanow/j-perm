from typing import Any

from j_perm.subst import CasterRegistry


@CasterRegistry.register("bool")
def cast_bool(x: Any) -> bool:
    # Preserve original semantics
    return bool(int(x)) if isinstance(x, (int, str)) else bool(x)
