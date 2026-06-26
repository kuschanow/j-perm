"""``ParseTextStage`` — expand string steps into op-dicts at stage time.

The text syntax plugs into the existing :class:`~j_perm.core.StageRegistry`.  A
spec is a list; every **string** element is parsed by the generated standalone
parser and replaced (spliced) by the op-dicts it produces, while non-string
elements pass through untouched.  This is what lets text and JSON mix freely in
one list.

The stage is ``context_aware = False`` so compilation (:meth:`Engine.compile`)
still works — parsing does not depend on the execution context.
"""
from __future__ import annotations

from typing import Any, List

from ..core import ExecutionContext, StageMatcher, StageProcessor
from . import parse_text


class TextStageMatcher(StageMatcher):
    """Fire when any step is a raw string to be parsed."""

    context_aware = False

    def matches(self, steps: List[Any], ctx: ExecutionContext) -> bool:
        return any(isinstance(s, str) for s in steps)


class ParseTextStage(StageProcessor):
    """Parse string steps into op-dicts; leave everything else as-is."""

    context_aware = False

    def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        out: List[Any] = []
        for s in steps:
            if isinstance(s, str):
                out.extend(parse_text(s))
            else:
                out.append(s)
        return out
