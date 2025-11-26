from __future__ import annotations

import jmespath
from jmespath import functions as _jp_funcs


class _UserFunctions(_jp_funcs.Functions):
    """Container for custom JMESPath functions used by the DSL."""

    @_jp_funcs.signature({'types': ['number']}, {'types': ['number']})
    def _func_subtract(self, a: float, b: float) -> float:
        """JMESPath function that subtracts two numbers (a - b)."""
        return a - b


USER_FUNCTIONS = _UserFunctions()
JP_OPTIONS = jmespath.Options(custom_functions=USER_FUNCTIONS)
