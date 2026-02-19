"""Loop control flow handlers: ``$break`` and ``$continue``.

These handlers implement early exit / iteration skip for ``foreach`` and
``while`` loops.  They raise :class:`~.signals.BreakSignal` /
:class:`~.signals.ContinueSignal` which are caught by the enclosing loop
handler in ``ops.py``.
"""

from typing import Any

from j_perm import ActionHandler, ExecutionContext, ActionMatcher
from .signals import BreakSignal, ContinueSignal


# ─────────────────────────────────────────────────────────────────────────────
# $break — exit the innermost loop
# ─────────────────────────────────────────────────────────────────────────────

class BreakMatcher(ActionMatcher):
    """Match a step by checking the ``$break`` field."""

    def matches(self, step: Any) -> bool:
        return isinstance(step, dict) and "$break" in step


class BreakHandler(ActionHandler):
    """``$break`` — exit the innermost ``foreach`` or ``while`` loop.

    Schema::

        {"$break": null}

    The value is ignored; only the presence of the ``$break`` key matters.
    Any changes to ``dest`` made before ``$break`` are preserved.

    Example — stop iterating once a condition is met::

        {
            "op": "foreach",
            "in": "/items",
            "as": "item",
            "do": [
                {
                    "op": "if",
                    "cond": {"$eq": [{"$ref": "&:/item"}, "stop"]},
                    "then": [{"$break": null}]
                },
                {"op": "set", "path": "/result/-", "value": "&:/item"}
            ]
        }
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        raise BreakSignal()


# ─────────────────────────────────────────────────────────────────────────────
# $continue — skip to the next loop iteration
# ─────────────────────────────────────────────────────────────────────────────

class ContinueMatcher(ActionMatcher):
    """Match a step by checking the ``$continue`` field."""

    def matches(self, step: Any) -> bool:
        return isinstance(step, dict) and "$continue" in step


class ContinueHandler(ActionHandler):
    """``$continue`` — skip to the next iteration of the innermost loop.

    Schema::

        {"$continue": null}

    The value is ignored; only the presence of the ``$continue`` key matters.
    Any changes to ``dest`` made before ``$continue`` in the current iteration
    are preserved.

    Example — skip items that don't pass a filter::

        {
            "op": "foreach",
            "in": "/items",
            "as": "item",
            "do": [
                {
                    "op": "if",
                    "cond": {"$lt": [{"$ref": "&:/item/score"}, 0]},
                    "then": [{"$continue": null}]
                },
                {"op": "set", "path": "/result/-", "value": "&:/item"}
            ]
        }
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        raise ContinueSignal()