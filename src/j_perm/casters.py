"""Built-in type casters for template substitution and $cast construct.

This module defines the standard type conversion functions used by both
template expressions (``${int:...}``) and the ``$cast`` construct
(``{"$cast": {"value": ..., "type": "int"}}``).

Exports
-------
BUILTIN_CASTERS
    Dictionary mapping type names to caster functions.
    Default types: int, float, bool, str.

Custom casters can be registered by passing a custom casters dict to
``build_default_engine(casters=...)``.
"""

from __future__ import annotations

from typing import Any, Callable

# ─────────────────────────────────────────────────────────────────────────────
# Built-in casters
# ─────────────────────────────────────────────────────────────────────────────

BUILTIN_CASTERS: dict[str, Callable[[Any], Any]] = {
    "int": lambda x: int(x),
    "float": lambda x: float(x),
    "bool": lambda x: bool(int(x)) if isinstance(x, (int, str)) else bool(x),
    "str": lambda x: str(x),
}