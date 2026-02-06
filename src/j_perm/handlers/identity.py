"""Identity (catch-all) handler — the bottom of the value-pipeline tree."""

from __future__ import annotations

from typing import Any

from ..core import ActionHandler, ExecutionContext


class IdentityHandler(ActionHandler):
    """Return the value unchanged.

    Mounted at the lowest priority (typically -999) in the value_pipeline
    so that every value that does not match a more specific handler still
    produces a result without raising.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return step
