from __future__ import annotations

from jmespath import functions as _jp_funcs

from ..subst import JpFuncRegistry


@JpFuncRegistry.register("subtract")
@_jp_funcs.signature({"types": ["number"]}, {"types": ["number"]})
def subtract(self, a: float, b: float) -> float:
    return a - b
