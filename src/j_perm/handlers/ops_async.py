"""Async twins of the built-in operation handlers (``handlers/ops.py``).

Every handler here mirrors its synchronous counterpart exactly, differing only
in that value-resolution and nested-body execution are awaited
(``process_value_async`` / ``apply_to_context_async`` / ``run_async``).  This is
what lets an :class:`~j_perm.core.AsyncActionHandler` (e.g. an async SQL handler
or an async ``$func``) actually be awaited when it appears as a step, inside a
compound body, or in value position.

Handlers whose *pure* mutation logic is identical reuse the ``_apply`` /
``_normalize_array`` / ``_eval_condition`` seams extracted on the sync classes,
so the non-async logic stays single-sourced.

``foreach`` additionally gains a **parallel** mode: with ``"parallel": true`` the
iterations run concurrently (bounded by an optional ``"concurrency"`` limit),
each on an isolated fresh ``{}`` dest, and their resulting deltas are folded into
``ctx.dest`` in input order via :func:`~j_perm.handlers.merge.deep_merge`.
``$break`` / ``$continue`` are not supported in parallel mode.
"""

from __future__ import annotations

import asyncio
import copy
from typing import Any

from ..core import AsyncActionHandler, CompiledSpec, ExecutionContext
from .merge import deep_merge
from .ops import (
    SetHandler, CopyHandler, DeleteHandler,
    ForeachHandler, WhileHandler, IfHandler, ExecHandler,
    UpdateHandler, DistinctHandler, AssertHandler, TryHandler,
    DeserializeHandler, _parse_format, _DESERIALIZE_FORMATS,
    SerializeHandler, _serialize_format, _SERIALIZE_FORMATS,
    EncodeHandler, DecodeHandler, _CodecHandler, _CODECS,
    HashHandler, _hash_digest, _hash_input_bytes, _HASH_ALGOS, _HASH_OUTPUTS,
)
from .signals import BreakSignal, ContinueSignal, ReturnSignal, ExitSignal


# ─────────────────────────────────────────────────────────────────────────────
# set / copy / delete
# ─────────────────────────────────────────────────────────────────────────────

class AsyncSetHandler(SetHandler, AsyncActionHandler):
    """Async ``op: set`` — resolves ``value`` (and friends) via the async value
    pipeline, then delegates the mutation to the shared :meth:`SetHandler._apply`."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        extend_list = bool(await ctx.engine.process_value_async(step.get("extend", True), ctx))
        value = await ctx.engine.process_value_async(step["value"], ctx)
        return self._apply(ctx, path, create, extend_list, value)


class AsyncCopyHandler(CopyHandler, AsyncActionHandler):
    """Async ``op: copy`` — mirrors the sync handler, delegating to an async set."""

    def __init__(self, set_handler: AsyncSetHandler | None = None) -> None:
        self._set = set_handler or AsyncSetHandler()

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        extend_list = bool(await ctx.engine.process_value_async(step.get("extend", True), ctx))

        ptr = await ctx.engine.process_value_async(step["from"], ctx)
        ignore = bool(await ctx.engine.process_value_async(step.get("ignore_missing", False), ctx))

        try:
            value = copy.deepcopy(ctx.engine.processor.get(ptr, ctx))
        except Exception:
            if "default" in step:
                value = copy.deepcopy(await ctx.engine.process_value_async(step["default"], ctx))
            elif ignore:
                return ctx.dest
            else:
                raise

        return await self._set.execute(
            {"op": "set", "path": path, "value": value,
             "create": create, "extend": extend_list},
            ctx,
        )


class AsyncDeleteHandler(DeleteHandler, AsyncActionHandler):
    """Async ``op: delete``."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = await ctx.engine.process_value_async(step["path"], ctx)
        ignore = bool(await ctx.engine.process_value_async(step.get("ignore_missing", True), ctx))
        return self._apply(ctx, path, ignore)


# ─────────────────────────────────────────────────────────────────────────────
# foreach
# ─────────────────────────────────────────────────────────────────────────────

class AsyncForeachHandler(ForeachHandler, AsyncActionHandler):
    """Async ``op: foreach`` — sequential (awaited body) and parallel modes."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return await self._arun(step, ctx, None)

    async def execute_compiled_async(
            self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return await self._arun(step, ctx, nested.get("do"))

    async def _arun(self, step: Any, ctx: ExecutionContext, compiled_body: Any) -> Any:
        has_in, has_in_value = self._validate_source(step)
        skip_empty = bool(await ctx.engine.process_value_async(step.get("skip_empty", True), ctx))

        if has_in_value:
            arr = await ctx.engine.process_value_async(step["in_value"], ctx)
        else:
            arr_ptr = await ctx.engine.process_value_async(step["in"], ctx)
            default = copy.deepcopy(await ctx.engine.process_value_async(step.get("default", []), ctx))
            try:
                arr = ctx.engine.processor.get(arr_ptr, ctx)
            except Exception:
                arr = default

        arr = self._normalize_array(arr, skip_empty)
        if arr is self._SKIP:
            return ctx.dest

        var = await ctx.engine.process_value_async(step.get("as", "item"), ctx)
        body = step["do"]

        if bool(await ctx.engine.process_value_async(step.get("parallel", False), ctx)):
            return await self._arun_parallel(step, ctx, compiled_body, arr, var, body)

        snapshot = copy.deepcopy(ctx.dest)
        try:
            for elem in arr:
                foreach_ctx = ctx.copy(new_temp_read_only={**ctx.temp_read_only, var: elem})
                try:
                    if compiled_body is not None:
                        ctx.dest = await compiled_body.run_async(foreach_ctx)
                    else:
                        ctx.dest = await ctx.engine.apply_to_context_async(body, foreach_ctx)
                except ContinueSignal:
                    ctx.dest = foreach_ctx.dest
                except BreakSignal:
                    ctx.dest = foreach_ctx.dest
                    break
        except ExitSignal:
            # $exit ends the whole script — keep the dest built so far
            # (including changes made in this iteration) and propagate.
            ctx.dest = foreach_ctx.dest
            raise
        except ReturnSignal:
            raise  # Propagate without rollback
        except Exception:
            ctx.dest = snapshot
            raise

        return ctx.dest

    async def _arun_parallel(
            self, step: Any, ctx: ExecutionContext, compiled_body: Any,
            arr: Any, var: Any, body: Any) -> Any:
        raw_conc = await ctx.engine.process_value_async(step.get("concurrency", None), ctx)
        concurrency = None
        if raw_conc is not None:
            concurrency = int(raw_conc)
            if concurrency <= 0:
                raise ValueError("foreach 'concurrency' must be a positive integer")
        sem = asyncio.Semaphore(concurrency) if concurrency else None

        async def run_one(elem: Any) -> Any:
            iter_ctx = ctx.copy(new_dest={}, new_temp_read_only={**ctx.temp_read_only, var: elem})
            if compiled_body is not None:
                return await compiled_body.run_async(iter_ctx)
            return await ctx.engine.apply_to_context_async(body, iter_ctx)

        async def guarded(elem: Any) -> Any:
            if sem is None:
                return await run_one(elem)
            async with sem:
                return await run_one(elem)

        snapshot = copy.deepcopy(ctx.dest)
        try:
            results = await asyncio.gather(*(guarded(elem) for elem in arr))
        except (BreakSignal, ContinueSignal) as exc:
            ctx.dest = snapshot
            raise ValueError(
                "$break/$continue is not supported in parallel foreach"
            ) from exc
        except (ReturnSignal, ExitSignal):
            raise
        except Exception:
            ctx.dest = snapshot
            raise

        for result in results:
            ctx.dest = deep_merge(ctx.dest, result)
        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# while
# ─────────────────────────────────────────────────────────────────────────────

class AsyncWhileHandler(WhileHandler, AsyncActionHandler):
    """Async ``op: while``."""

    async def _eval_condition_async(self, step: Any, ctx: ExecutionContext) -> bool:
        if "cond" in step:
            return bool(await ctx.engine.process_value_async(step["cond"], ctx))
        if "path" in step:
            try:
                ptr = await ctx.engine.process_value_async(step["path"], ctx)
                current = ctx.engine.processor.get(ptr, ctx)
                missing = False
            except Exception:
                current = None
                missing = True
            if "equals" in step:
                expected = await ctx.engine.process_value_async(step["equals"], ctx)
                return current == expected and not missing
            elif await ctx.engine.process_value_async(step.get("exists", False), ctx):
                return not missing
            else:
                return bool(current) and not missing
        raise ValueError("while operation requires 'cond' or 'path'")

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return await self._arun(step, ctx, None)

    async def execute_compiled_async(
            self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return await self._arun(step, ctx, nested.get("do"))

    async def _arun(self, step: Any, ctx: ExecutionContext, compiled_body: Any) -> Any:
        do_while = bool(await ctx.engine.process_value_async(step.get("do_while", False), ctx))
        body = step["do"]
        snapshot = copy.deepcopy(ctx.dest)

        try:
            iteration = 0
            while True:
                if iteration >= self._max_iterations:
                    raise RuntimeError(
                        f"While loop exceeded maximum iterations ({self._max_iterations})"
                    )
                if not do_while and not await self._eval_condition_async(step, ctx):
                    break

                try:
                    if compiled_body is not None:
                        ctx.dest = await compiled_body.run_async(ctx)
                    else:
                        ctx.dest = await ctx.engine.apply_to_context_async(body, ctx)
                except ContinueSignal:
                    pass
                except BreakSignal:
                    break

                do_while = False
                iteration += 1
        except (ReturnSignal, ExitSignal):
            raise
        except Exception:
            ctx.dest = snapshot
            raise

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# if
# ─────────────────────────────────────────────────────────────────────────────

class AsyncIfHandler(IfHandler, AsyncActionHandler):
    """Async ``op: if``."""

    async def _eval_condition_async(self, step: Any, ctx: ExecutionContext) -> bool:
        if "path" in step:
            try:
                ptr = await ctx.engine.process_value_async(step["path"], ctx)
                current = ctx.engine.processor.get(ptr, ctx)
                missing = False
            except Exception:
                current = None
                missing = True
            if "equals" in step:
                expected = await ctx.engine.process_value_async(step["equals"], ctx)
                return current == expected and not missing
            elif await ctx.engine.process_value_async(step.get("exists", False), ctx):
                return not missing
            else:
                return bool(current) and not missing
        raw_cond = await ctx.engine.process_value_async(step.get("cond"), ctx)
        return bool(raw_cond)

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return await self._arun(step, ctx, None)

    async def execute_compiled_async(
            self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return await self._arun(step, ctx, nested)

    async def _arun(self, step: Any, ctx: ExecutionContext, nested: Any) -> Any:
        cond_val = await self._eval_condition_async(step, ctx)
        branch_key = self._get_branch_key(step, cond_val)
        actions = step.get(branch_key)

        if not actions:
            return ctx.dest

        compiled_branch = nested.get(branch_key) if (nested and branch_key) else None
        snapshot = copy.deepcopy(ctx.dest)
        try:
            if compiled_branch is not None:
                return await compiled_branch.run_async(ctx)
            return await ctx.engine.apply_to_context_async(actions, ctx)
        except (BreakSignal, ContinueSignal, ReturnSignal, ExitSignal):
            raise
        except Exception:
            ctx.dest = snapshot
            raise


# ─────────────────────────────────────────────────────────────────────────────
# exec
# ─────────────────────────────────────────────────────────────────────────────

class AsyncExecHandler(ExecHandler, AsyncActionHandler):
    """Async ``op: exec``."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_actions = "actions" in step

        if has_from and has_actions:
            raise ValueError("exec operation cannot have both 'from' and 'actions' parameters")
        if not has_from and not has_actions:
            raise ValueError("exec operation requires either 'from' or 'actions' parameter")

        if has_from:
            actions_ptr = await ctx.engine.process_value_async(step["from"], ctx)
            try:
                actions = ctx.engine.processor.get(actions_ptr, ctx)
            except Exception:
                if "default" in step:
                    actions = await ctx.engine.process_value_async(step["default"], ctx)
                else:
                    raise ValueError(f"Cannot find actions at {actions_ptr}")
        else:
            actions = await ctx.engine.process_value_async(step["actions"], ctx)

        merge = bool(await ctx.engine.process_value_async(step.get("merge", False), ctx))

        if merge:
            return await ctx.engine.apply_to_context_async(actions, ctx)
        exec_ctx = ctx.copy(new_dest={}, deepcopy_dest=False)
        return await ctx.engine.apply_to_context_async(actions, exec_ctx)


# ─────────────────────────────────────────────────────────────────────────────
# update / distinct
# ─────────────────────────────────────────────────────────────────────────────

class AsyncUpdateHandler(UpdateHandler, AsyncActionHandler):
    """Async ``op: update``."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        deep = bool(await ctx.engine.process_value_async(step.get("deep", False), ctx))

        if "from" in step:
            ptr = await ctx.engine.process_value_async(step["from"], ctx)
            try:
                update_value = copy.deepcopy(ctx.engine.processor.get(ptr, ctx))
            except Exception:
                if "default" in step:
                    update_value = copy.deepcopy(await ctx.engine.process_value_async(step["default"], ctx))
                else:
                    raise
        elif "value" in step:
            update_value = await ctx.engine.process_value_async(step["value"], ctx)
        else:
            raise ValueError("update operation requires either 'from' or 'value' parameter")

        return self._apply(ctx, path, create, deep, update_value)


class AsyncDistinctHandler(DistinctHandler, AsyncActionHandler):
    """Async ``op: distinct``."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = await ctx.engine.process_value_async(step["path"], ctx)
        key = step.get("key", None)
        key_path = await ctx.engine.process_value_async(key, ctx) if key is not None else None
        return self._apply(ctx, path, key, key_path)


# ─────────────────────────────────────────────────────────────────────────────
# assert
# ─────────────────────────────────────────────────────────────────────────────

class AsyncAssertHandler(AssertHandler, AsyncActionHandler):
    """Async ``op: assert``."""

    async def _return_value_async(self, step: Any, ctx: ExecutionContext, value: Any) -> Any:
        if "to_path" in step:
            return ctx.engine.resolver.set(
                await ctx.engine.process_value_async(step["to_path"], ctx), ctx.dest, value)
        return value

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_path = "path" in step
        has_value = "value" in step

        if has_path and has_value:
            raise ValueError("assert operation cannot have both 'path' and 'value' parameters")
        if not has_path and not has_value:
            raise ValueError("assert operation requires either 'path' or 'value' parameter")

        should_return = await ctx.engine.process_value_async(step.get("return", False), ctx)

        if has_value:
            current = await ctx.engine.process_value_async(step["value"], ctx)
        else:
            path = await ctx.engine.process_value_async(step["path"], ctx)
            try:
                current = ctx.engine.processor.get(path, ctx)
            except Exception:
                if should_return:
                    return await self._return_value_async(step, ctx, False)
                raise AssertionError(f"'{path}' does not exist in source")

        if "equals" in step:
            expected = await ctx.engine.process_value_async(step["equals"], ctx)
            if current != expected:
                if should_return:
                    return await self._return_value_async(step, ctx, False)
                raise AssertionError(f"Value != {expected!r}")

        if should_return:
            return await self._return_value_async(step, ctx, current)

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# try
# ─────────────────────────────────────────────────────────────────────────────

class AsyncTryHandler(TryHandler, AsyncActionHandler):
    """Async ``op: try``."""

    async def _run_body_async(self, actions: Any, compiled: Any, ctx: ExecutionContext) -> None:
        if compiled is not None:
            await compiled.run_async(ctx)
        else:
            await ctx.engine.apply_to_context_async(actions, ctx)

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return await self._aexecute_impl(step, ctx, {})

    async def execute_compiled_async(
            self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return await self._aexecute_impl(step, ctx, nested)

    async def _aexecute_impl(self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        do_actions = step.get("do")
        except_actions = step.get("except")
        finally_actions = step.get("finally")

        if do_actions is None:
            raise ValueError("try operation requires 'do' parameter")

        compiled_do = nested.get("do")
        compiled_except = nested.get("except")
        compiled_finally = nested.get("finally")

        try:
            await self._run_body_async(do_actions, compiled_do, ctx)
        except (BreakSignal, ContinueSignal, ReturnSignal, ExitSignal):
            if finally_actions is not None:
                try:
                    await self._run_body_async(finally_actions, compiled_finally, ctx)
                except Exception:
                    pass
            raise
        except Exception as e:
            error_info = {
                "_error_type": type(e).__name__,
                "_error_message": str(e),
            }

            if except_actions is not None:
                ctx.temp_read_only.update(error_info)
                try:
                    await self._run_body_async(except_actions, compiled_except, ctx)
                finally:
                    ctx.temp_read_only.pop('_error_type', None)
                    ctx.temp_read_only.pop('_error_message', None)
            else:
                if finally_actions is not None:
                    try:
                        await self._run_body_async(finally_actions, compiled_finally, ctx)
                    except Exception:
                        pass
                raise

        if finally_actions is not None:
            await self._run_body_async(finally_actions, compiled_finally, ctx)

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# deserialize
# ─────────────────────────────────────────────────────────────────────────────

class AsyncDeserializeHandler(DeserializeHandler, AsyncActionHandler):
    """Async ``op: deserialize``."""

    def __init__(self, set_handler: AsyncSetHandler | None = None) -> None:
        self._set = set_handler or AsyncSetHandler()

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError("deserialize cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError("deserialize requires either 'from' or 'value'")

        fmt = await ctx.engine.process_value_async(step.get("format", "json"), ctx)
        if fmt not in _DESERIALIZE_FORMATS:
            raise ValueError(
                f"deserialize: unknown format '{fmt}'. Supported: {', '.join(sorted(_DESERIALIZE_FORMATS))}"
            )

        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        extend_list = bool(await ctx.engine.process_value_async(step.get("extend", True), ctx))

        if has_from:
            ptr = await ctx.engine.process_value_async(step["from"], ctx)
            try:
                raw = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    fallback = await ctx.engine.process_value_async(step["default"], ctx)
                    return await self._set.execute(
                        {"op": "set", "path": path, "value": fallback,
                         "create": create, "extend": extend_list},
                        ctx,
                    )
                raise
        else:
            raw = await ctx.engine.process_value_async(step["value"], ctx)

        try:
            parsed = _parse_format(fmt, raw)
        except Exception as exc:
            if "default" in step:
                fallback = await ctx.engine.process_value_async(step["default"], ctx)
                return await self._set.execute(
                    {"op": "set", "path": path, "value": fallback,
                     "create": create, "extend": extend_list},
                    ctx,
                )
            raise ValueError(f"deserialize: failed to parse as '{fmt}': {exc}") from exc

        return await self._set.execute(
            {"op": "set", "path": path, "value": parsed, "create": create, "extend": extend_list},
            ctx,
        )


# ─────────────────────────────────────────────────────────────────────────────
# serialize
# ─────────────────────────────────────────────────────────────────────────────

class AsyncSerializeHandler(SerializeHandler, AsyncActionHandler):
    """Async ``op: serialize``."""

    def __init__(self, set_handler: AsyncSetHandler | None = None) -> None:
        self._set = set_handler or AsyncSetHandler()

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError("serialize cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError("serialize requires either 'from' or 'value'")

        fmt = await ctx.engine.process_value_async(step.get("format", "json"), ctx)
        if fmt not in _SERIALIZE_FORMATS:
            raise ValueError(
                f"serialize: unknown format '{fmt}'. Supported: {', '.join(sorted(_SERIALIZE_FORMATS))}"
            )

        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        extend_list = bool(await ctx.engine.process_value_async(step.get("extend", True), ctx))

        if has_from:
            ptr = await ctx.engine.process_value_async(step["from"], ctx)
            try:
                value = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    return await self._write_default_async(step, ctx, path, create, extend_list)
                raise
        else:
            value = await ctx.engine.process_value_async(step["value"], ctx)

        try:
            rendered = _serialize_format(fmt, value)
        except Exception as exc:
            if "default" in step:
                return await self._write_default_async(step, ctx, path, create, extend_list)
            raise ValueError(f"serialize: failed to render as '{fmt}': {exc}") from exc

        return await self._set.execute(
            {"op": "set", "path": path, "value": rendered, "create": create, "extend": extend_list},
            ctx,
        )

    async def _write_default_async(self, step: Any, ctx: ExecutionContext, path: Any,
                                   create: bool, extend_list: bool) -> Any:
        fallback = await ctx.engine.process_value_async(step["default"], ctx)
        return await self._set.execute(
            {"op": "set", "path": path, "value": fallback, "create": create, "extend": extend_list},
            ctx,
        )


# ─────────────────────────────────────────────────────────────────────────────
# encode / decode
# ─────────────────────────────────────────────────────────────────────────────

class _AsyncCodecHandler(_CodecHandler, AsyncActionHandler):
    """Async twin of :class:`~j_perm.handlers.ops._CodecHandler` — awaits value
    resolution, reuses the sync ``_op`` / ``_transform`` seams."""

    def __init__(self, set_handler: AsyncSetHandler | None = None) -> None:
        self._set = set_handler or AsyncSetHandler()

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError(f"{self._op} cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError(f"{self._op} requires either 'from' or 'value'")

        codec = await ctx.engine.process_value_async(step.get("codec", "base64"), ctx)
        if codec not in _CODECS:
            raise ValueError(
                f"{self._op}: unknown codec '{codec}'. Supported: {', '.join(sorted(_CODECS))}"
            )

        encoding = await ctx.engine.process_value_async(step.get("encoding", "utf-8"), ctx)
        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        extend_list = bool(await ctx.engine.process_value_async(step.get("extend", True), ctx))

        if has_from:
            ptr = await ctx.engine.process_value_async(step["from"], ctx)
            try:
                raw = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    return await self._write_default_async(step, ctx, path, create, extend_list)
                raise
        else:
            raw = await ctx.engine.process_value_async(step["value"], ctx)

        try:
            result = self._transform(codec, raw, encoding)
        except Exception as exc:
            if "default" in step:
                return await self._write_default_async(step, ctx, path, create, extend_list)
            verb = "encode" if self._op == "encode" else "decode"
            raise ValueError(f"{self._op}: failed to {verb} as '{codec}': {exc}") from exc

        return await self._set.execute(
            {"op": "set", "path": path, "value": result, "create": create, "extend": extend_list},
            ctx,
        )

    async def _write_default_async(self, step: Any, ctx: ExecutionContext, path: Any,
                                   create: bool, extend_list: bool) -> Any:
        fallback = await ctx.engine.process_value_async(step["default"], ctx)
        return await self._set.execute(
            {"op": "set", "path": path, "value": fallback, "create": create, "extend": extend_list},
            ctx,
        )


class AsyncEncodeHandler(EncodeHandler, _AsyncCodecHandler):
    """Async ``op: encode`` — ``_op`` / ``_transform`` from :class:`EncodeHandler`,
    async ``execute`` from :class:`_AsyncCodecHandler`."""


class AsyncDecodeHandler(DecodeHandler, _AsyncCodecHandler):
    """Async ``op: decode`` — ``_op`` / ``_transform`` from :class:`DecodeHandler`,
    async ``execute`` from :class:`_AsyncCodecHandler`."""


# ─────────────────────────────────────────────────────────────────────────────
# hash
# ─────────────────────────────────────────────────────────────────────────────

class AsyncHashHandler(HashHandler, AsyncActionHandler):
    """Async ``op: hash``."""

    def __init__(self, set_handler: AsyncSetHandler | None = None) -> None:
        self._set = set_handler or AsyncSetHandler()

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError("hash cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError("hash requires either 'from' or 'value'")

        algo = await ctx.engine.process_value_async(step.get("algo", "sha256"), ctx)
        if algo not in _HASH_ALGOS:
            raise ValueError(
                f"hash: unknown algo '{algo}'. Supported: {', '.join(sorted(_HASH_ALGOS))}"
            )

        output = await ctx.engine.process_value_async(step.get("output", "hex"), ctx)
        if output not in _HASH_OUTPUTS:
            raise ValueError(
                f"hash: unknown output '{output}'. Supported: {', '.join(sorted(_HASH_OUTPUTS))}"
            )

        encoding = await ctx.engine.process_value_async(step.get("encoding", "utf-8"), ctx)
        path = await ctx.engine.process_value_async(step["path"], ctx)
        create = bool(await ctx.engine.process_value_async(step.get("create", True), ctx))
        extend_list = bool(await ctx.engine.process_value_async(step.get("extend", True), ctx))

        if has_from:
            ptr = await ctx.engine.process_value_async(step["from"], ctx)
            try:
                value = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    fallback = await ctx.engine.process_value_async(step["default"], ctx)
                    return await self._set.execute(
                        {"op": "set", "path": path, "value": fallback,
                         "create": create, "extend": extend_list},
                        ctx,
                    )
                raise
        else:
            value = await ctx.engine.process_value_async(step["value"], ctx)

        digest = _hash_digest(algo, output, _hash_input_bytes(value, encoding))

        return await self._set.execute(
            {"op": "set", "path": path, "value": digest, "create": create, "extend": extend_list},
            ctx,
        )
