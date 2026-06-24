"""Async twin of :class:`~j_perm.handlers.special.SpecialResolveHandler`.

Dispatches a special-construct dict to its registered handler and **awaits** the
result when the handler is async (e.g. the async ``$func`` construct, or any
async construct twin).  Synchronous constructs return plain values and are used
as-is, so a single async value pipeline can mix sync and async specials.
"""

from __future__ import annotations

import inspect
from typing import Any

from ..core import AsyncActionHandler, ExecutionContext
from .special import SpecialResolveHandler
from .signals import RawValueSignal


class AsyncSpecialResolveHandler(SpecialResolveHandler, AsyncActionHandler):
    """Async special-construct dispatch."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        for key, fn in self._specials.items():
            if key in step:
                result = fn(step, ctx)
                if inspect.isawaitable(result):
                    result = await result
                if step.get("$raw") is True:
                    raise RawValueSignal(result)
                return result
        return step
