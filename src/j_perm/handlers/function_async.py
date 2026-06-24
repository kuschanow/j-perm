"""Async twins of the function handlers (``handlers/function.py``).

* :class:`AsyncDefHandler` registers an **async** function closure that awaits
  its body, so async operations inside a ``$func`` body are awaited.
* :class:`AsyncCallHandler` awaits the closure result (and tolerates a plain
  sync closure, awaiting only when the result is awaitable).
* :class:`AsyncRaiseHandler` / :class:`AsyncReturnHandler` resolve their value
  through the async value pipeline.
"""

from __future__ import annotations

import inspect
from typing import Any

from ..core import AsyncActionHandler, CompiledSpec, ExecutionContext
from .function import DefHandler, CallHandler, RaiseHandler, ReturnHandler, JPermError
from .signals import ReturnSignal


class AsyncDefHandler(DefHandler, AsyncActionHandler):
    """Async ``$def`` — stores an async function closure."""

    def _make_function(self, step: Any, ctx: ExecutionContext,
                       compiled_body: Any, compiled_on_failure: Any) -> Any:
        function_name = step["$def"]
        params = step.get("params", [])
        body = step["body"]
        return_path = step.get("return")
        on_failure = step.get("on_failure", None)
        context = step.get("context", "copy")

        async def function(*args):
            if len(args) != len(params):
                raise ValueError(
                    f"Expected {len(params)} arguments, got {len(args)} for function '{function_name}'")
            try:
                call_ctx = ctx.metadata.get('_current_call_ctx', ctx)

                copy_args = {}
                if context == "new":
                    copy_args = {"new_dest": {}}
                elif context == "shared":
                    copy_args = {}
                elif context == "copy":
                    copy_args = {"deepcopy_dest": True}
                else:
                    raise ValueError(
                        f"Invalid context option '{context}' in function definition '{function_name}'")

                ctx_copy = call_ctx.copy(
                    new_temp_read_only={param: arg for param, arg in zip(params, args)},
                    **copy_args
                )
                if compiled_body is not None:
                    result = await compiled_body.run_async(ctx_copy)
                else:
                    result = await ctx.engine.apply_to_context_async(body, ctx_copy)
                if return_path:
                    temp_ctx = ctx.copy(new_source=result)
                    return ctx.engine.processor.get(return_path, temp_ctx)
                return result
            except ReturnSignal as e:
                return e.value
            except Exception as e:
                if on_failure is not None:
                    call_ctx = ctx.metadata.get('_current_call_ctx', ctx)
                    ctx_copy = call_ctx.copy(
                        new_temp_read_only={param: arg for param, arg in zip(params, args)},
                    )
                    if compiled_on_failure is not None:
                        return await compiled_on_failure.run_async(ctx_copy)
                    return await ctx.engine.apply_to_context_async(on_failure, ctx_copy)
                else:
                    raise e

        return function

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        function = self._make_function(step, ctx, None, None)
        ctx.metadata["__functions__"] = ctx.metadata.get("__functions__", {})
        ctx.metadata["__functions__"][step["$def"]] = function
        return ctx.dest

    async def execute_compiled_async(
            self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        compiled_body = nested.get("body")
        compiled_on_failure = nested.get("on_failure")
        function = self._make_function(step, ctx, compiled_body, compiled_on_failure)
        ctx.metadata["__functions__"] = ctx.metadata.get("__functions__", {})
        ctx.metadata["__functions__"][step["$def"]] = function
        return ctx.dest


class AsyncCallHandler(CallHandler, AsyncActionHandler):
    """Async ``$func`` — awaits the (possibly async) function closure."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        function_name = step["$func"]
        raw_args = step.get("args", [])
        functions = ctx.metadata.get("__functions__", {})
        if function_name not in functions:
            raise ValueError(f"Function '{function_name}' is not defined.")

        args = [await ctx.engine.process_value_async(arg, ctx) for arg in raw_args]

        call_stack = ctx.metadata.get('_function_call_stack', [])
        call_stack.append(function_name)
        ctx.metadata['_function_call_stack'] = call_stack

        try:
            if len(call_stack) > ctx.engine.max_function_recursion_depth:
                raise RecursionError(
                    f"Function recursion depth ({len(call_stack)}) "
                    f"exceeded maximum ({ctx.engine.max_function_recursion_depth})"
                )

            function = functions[function_name]
            old_call_ctx = ctx.metadata.get('_current_call_ctx')
            ctx.metadata['_current_call_ctx'] = ctx
            try:
                result = function(*args)
                if inspect.isawaitable(result):
                    result = await result
                return result
            finally:
                if old_call_ctx is None:
                    ctx.metadata.pop('_current_call_ctx', None)
                else:
                    ctx.metadata['_current_call_ctx'] = old_call_ctx
        finally:
            call_stack.pop()


class AsyncRaiseHandler(RaiseHandler, AsyncActionHandler):
    """Async ``$raise``."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        message = await ctx.engine.process_value_async(step["$raise"], ctx)
        raise JPermError(str(message))


class AsyncReturnHandler(ReturnHandler, AsyncActionHandler):
    """Async ``$return``."""

    async def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        value = await ctx.engine.process_value_async(step["$return"], ctx)
        raise ReturnSignal(value)
