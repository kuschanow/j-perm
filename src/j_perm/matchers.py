"""Shared ActionMatcher implementations.

Only matchers that are genuinely reusable across multiple sub-systems live
here.  Matchers that are tightly coupled to a single handler (e.g.
``TemplMatcher``) are co-located with that handler in the ``handlers``
sub-package.

Exports
-------
OpMatcher
    Match by the value of ``step["op"]``.  Every op registered in
    *main_pipeline* will use one.

AlwaysMatcher
    Unconditional match — catch-all / fallback sentinel.
"""

from __future__ import annotations

from typing import Any, Mapping

from .core import ActionMatcher


class OpMatcher(ActionMatcher):
    """Match a step by its ``"op"`` field value.

    ::

        OpMatcher("set").matches({"op": "set", …})   # True
        OpMatcher("set").matches({"op": "copy", …})  # False
    """

    def __init__(self, op: str) -> None:
        self._op = op

    def matches(self, step: Any) -> bool:
        return isinstance(step, Mapping) and step.get("op") == self._op


class AlwaysMatcher(ActionMatcher):
    """Unconditional match — use as a catch-all / fallback node.

    ::

        AlwaysMatcher().matches(anything)   # True
    """

    def matches(self, step: Any) -> bool:
        return True
