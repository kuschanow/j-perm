from typing import Any

from j_perm import ActionHandler, ExecutionContext, ActionMatcher


class DefMatcher(ActionMatcher):
    """Match a step by checking the def field.
    """

    def matches(self, step: Any) -> bool:
        return isinstance(step, dict) and "$def" in step


class CallMatcher(ActionMatcher):
    """Match a step by checking the func field.
    """

    def matches(self, step: Any) -> bool:
        return isinstance(step, dict) and "$func" in step


class DefHandler(ActionHandler):
    """``def: <function_name>``: Defines a function with the given name and body.

    Schema::

        {"$def": "<function_name>",
         "params": ["<param_name>", "<second_param_name>"]  # optional, default is empty dict
         "body": <function_body>,
         "return": "<path/in/local/ctx>"  # optional, default is None
         "on_failure": <failure_actions>  # optional, default is None,
         "context": "new|copy|shared"  # optional, default is "copy"}

    * ``<function_name>``: The name of the function to define.
    * ``params``: The list of parameters for the function. This is optional and can be an empty list if the function does not take any parameters.
    * ``body``: The body of the function, which can be a list of any valid J-Perm actions. This is the code that will be executed when the function is called.
    * ``return``: An optional path in the local context where the return value of the function will be stored. If not provided, the function will return the entire dest after executing the body.
    * ``on_failure``: An optional set of actions to execute if the function execution fails. This can be used to handle errors gracefully.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        function_name = step["$def"]
        params = step.get("params", [])
        body = step["body"]
        return_path = step.get("return")
        on_failure = step.get("on_failure", None)
        context = step.get("context", "copy")

        def function(*args):
            if len(args) != len(params):
                raise ValueError(f"Expected {len(params)} arguments, got {len(args)} for function '{function_name}'")
            try:
                # Use current call context if available (for recursive calls)
                call_ctx = ctx.metadata.get('_current_call_ctx', ctx)

                copy_args = {}
                if context == "new":
                    # Create a new context with empty dest and source, but inherit metadata
                    copy_args = {"new_dest": {}}
                elif context == "shared":
                    # Use the same context (dest, source, temp) for the function body
                    copy_args = {}
                else:  # default "copy"
                    copy_args = {"deepcopy_dest": True}

                ctx_copy = call_ctx.copy(
                    new_temp_read_only={param: arg for param, arg in zip(params, args)},
                    **copy_args
                )
                result = ctx.engine.apply_to_context(
                    body,
                    ctx_copy
                )
                if return_path:
                    temp_ctx = ctx.copy(new_source=result)
                    return ctx.engine.processor.get(return_path, temp_ctx)
                return result
            except Exception as e:
                if on_failure is not None:
                    call_ctx = ctx.metadata.get('_current_call_ctx', ctx)
                    ctx_copy = call_ctx.copy(
                        new_temp_read_only={param: arg for param, arg in zip(params, args)},
                    )
                    return ctx.engine.apply_to_context(on_failure, ctx_copy)
                else:
                    raise e

        ctx.metadata["__functions__"] = ctx.metadata.get("__functions__", {})
        ctx.metadata["__functions__"][function_name] = function
        return ctx.dest


class CallHandler(ActionHandler):
    """``$func: <function_name>``: Calls a previously defined function.

    Schema::

        {"$func": "<function_name>",
         "args": [<arg1>, <arg2>, …]}  # optional, default is empty list

    * ``<function_name>``: The name of the function to call. This should match the name of a function defined using the ``def`` action.
    * ``args``: An optional list of arguments to pass to the function. If not provided, the function will be called with no arguments.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        function_name = step["$func"]
        raw_args = step.get("args", [])
        functions = ctx.metadata.get("__functions__", {})
        if function_name not in functions:
            raise ValueError(f"Function '{function_name}' is not defined.")

        # Process arguments through value pipeline
        args = [ctx.engine.process_value(arg, ctx) for arg in raw_args]

        # Track function call depth to prevent stack overflow
        call_stack = ctx.metadata.get('_function_call_stack', [])
        call_stack.append(function_name)
        ctx.metadata['_function_call_stack'] = call_stack

        try:
            # Check recursion depth
            if len(call_stack) > ctx.engine.max_function_recursion_depth:
                raise RecursionError(
                    f"Function recursion depth ({len(call_stack)}) "
                    f"exceeded maximum ({ctx.engine.max_function_recursion_depth})"
                )

            function = functions[function_name]
            # Store current context for function to use
            old_call_ctx = ctx.metadata.get('_current_call_ctx')
            ctx.metadata['_current_call_ctx'] = ctx
            try:
                result = function(*args)
                return result
            finally:
                # Restore previous context
                if old_call_ctx is None:
                    ctx.metadata.pop('_current_call_ctx', None)
                else:
                    ctx.metadata['_current_call_ctx'] = old_call_ctx
        finally:
            # Pop from call stack
            call_stack.pop()


class RaiseMatcher(ActionMatcher):
    """Match a step by checking the $raise field.
    """

    def matches(self, step: Any) -> bool:
        return isinstance(step, dict) and "$raise" in step


class JPermError(Exception):
    """Custom error raised by the $raise operation."""
    pass


class RaiseHandler(ActionHandler):
    """``$raise: <message>``: Raises a custom error with the specified message.

    Schema::

        {"$raise": "<error_message>"}

    * ``<error_message>``: The error message to raise. This is the only configurable parameter.

    This operation raises a ``JPermError`` exception with the provided message.
    The error can be caught by the ``on_failure`` handler in function definitions.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        message = ctx.engine.process_value(step["$raise"], ctx)
        raise JPermError(str(message))
