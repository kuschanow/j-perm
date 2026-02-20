"""Special-construct dispatch — ``$ref``, ``$eval``, and extensible peers.

A *special construct* is a dict that contains a recognised marker key
(e.g. ``$ref``, ``$eval``).  The entire dict is replaced by the value
produced by the corresponding handler function.

Exports
-------
SpecialFn
    Type alias for the handler signature: ``(node, ctx) → Any``.

SpecialMatcher
    Fires on dicts that contain at least one registered special key.

SpecialResolveHandler
    Dispatches to the correct ``SpecialFn`` based on which key is present.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from ..core import ActionHandler, ActionMatcher, ExecutionContext

# -- type alias ---------------------------------------------------------

#: Signature for a single special-construct handler.
#: *node* is the full dict (e.g. ``{"$ref": "/foo", "$default": 0}``).
SpecialFn = Callable[[Mapping[str, Any], ExecutionContext], Any]


# -- matcher ------------------------------------------------------------


class SpecialMatcher(ActionMatcher):
    """Match dicts that carry at least one key from *keys*.

    ``keys`` is the set of all registered special keys (e.g. ``{"$ref", "$eval"}``).
    """

    def __init__(self, keys: set[str]) -> None:
        self._keys = keys

    def matches(self, step: Any) -> bool:
        return isinstance(step, Mapping) and bool(self._keys.intersection(step.keys()))


# -- handler ------------------------------------------------------------


class SpecialResolveHandler(ActionHandler):
    """Dispatch a special-construct dict to its registered handler.

    The first key in the dict that appears in *specials* wins.  The handler
    receives the entire original dict so it can read auxiliary keys
    (e.g. ``$default``).

    Should never return *step* unchanged in normal operation — if it does,
    it means ``SpecialMatcher`` let through a dict that has no handler,
    which is a wiring bug.
    """

    def __init__(self, specials: Mapping[str, SpecialFn] | None = None) -> None:
        self._specials: dict[str, SpecialFn] = dict(specials) if specials else {}

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        for key, fn in self._specials.items():
            if key in step:
                result = fn(step, ctx)
                if step.get("$raw") is True:
                    from .signals import RawValueSignal
                    raise RawValueSignal(result)
                return result
        return step
