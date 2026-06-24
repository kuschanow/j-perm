"""Async twin of :class:`~j_perm.handlers.container.RecursiveDescentHandler`.

Recurses into containers awaiting ``process_value_async`` on every element, so
an async construct nested anywhere inside a list/dict value is awaited.
"""

from __future__ import annotations

from typing import Any, Mapping

from ..core import AsyncActionHandler, ExecutionContext
from .container import RecursiveDescentHandler


class AsyncRecursiveDescentHandler(RecursiveDescentHandler, AsyncActionHandler):
    """Walk into a container and ``process_value_async`` each element."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        if isinstance(step, (list, tuple)):
            return [
                await ctx.engine.process_value_async(item, ctx, _unescape=False)
                for item in step
            ]

        if isinstance(step, Mapping):
            out: dict[Any, Any] = {}
            for k, v in step.items():
                new_key = (
                    await ctx.engine.process_value_async(k, ctx, _unescape=False)
                    if isinstance(k, str) else k
                )
                if new_key in out:
                    raise KeyError(f"duplicate key after substitution: {new_key!r}")
                out[new_key] = await ctx.engine.process_value_async(v, ctx, _unescape=False)
            return out

        return step
