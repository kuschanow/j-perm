"""Recursive container descent — lists, tuples, and plain dicts.

Any value that is a container but is *not* a special construct needs its
children processed individually through ``process_value``.  This module owns
both the matcher that identifies such containers and the handler that
recurses into them.

Exports
-------
ContainerMatcher
    Fires on lists, tuples, and dicts that do **not** carry a special key.

RecursiveDescentHandler
    Recurses into the container, calling ``ctx.engine.process_value`` on each
    element / value.
"""

from __future__ import annotations

from typing import Any, Mapping

from ..core import ActionHandler, ActionMatcher, ExecutionContext


class ContainerMatcher(ActionMatcher):
    """Match containers that are *not* special constructs.

    * Lists and tuples always match.
    * Dicts match only if none of their keys are in *special_keys*.

    *special_keys* must be the same set that ``SpecialMatcher`` was built
    with — otherwise the two matchers can overlap and a special dict may be
    silently descended into instead of dispatched.
    """

    def __init__(self, special_keys: set[str]) -> None:
        self._special_keys = special_keys

    def matches(self, step: Any) -> bool:
        if isinstance(step, (list, tuple)):
            return True
        if isinstance(step, Mapping):
            return not self._special_keys.intersection(step.keys())
        return False


class RecursiveDescentHandler(ActionHandler):
    """Walk into a container and ``process_value`` each element.

    * Lists / tuples  → each item is processed; tuples become lists
                        (JMESPath compatibility).
    * Dicts           → keys *and* values are processed.  If two keys
                        collide after substitution a ``KeyError`` is raised.

    All inner calls use ``_unescape=False`` so that the ``$${`` → ``${``
    unescape fires only once at the outermost ``process_value``.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        if isinstance(step, (list, tuple)):
            return [
                ctx.engine.process_value(item, ctx, _unescape=False)
                for item in step
            ]

        if isinstance(step, Mapping):
            out: dict[Any, Any] = {}
            for k, v in step.items():
                new_key = (
                    ctx.engine.process_value(k, ctx, _unescape=False)
                    if isinstance(k, str) else k
                )
                if new_key in out:
                    raise KeyError(f"duplicate key after substitution: {new_key!r}")
                out[new_key] = ctx.engine.process_value(v, ctx, _unescape=False)
            return out

        return step
