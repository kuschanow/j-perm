"""All built-in operation handlers.

Each operation is implemented as an ``ActionHandler`` that accepts steps
with ``{"op": "operation_name", ...}``.

Exports all 12 operation handlers:
* SetHandler, CopyHandler, CopyDHandler
* DeleteHandler
* ForeachHandler, IfHandler, ExecHandler
* UpdateHandler, DistinctHandler
* ReplaceRootHandler
* AssertHandler, AssertDHandler
"""

from __future__ import annotations

import copy
from typing import Any, Mapping

from ..core import ActionHandler, ExecutionContext


# ─────────────────────────────────────────────────────────────────────────────
# set — write value at path
# ─────────────────────────────────────────────────────────────────────────────

class SetHandler(ActionHandler):
    """``op: set`` — write value to destination path.

    Schema::

        {"op": "set", "path": "/dest/path", "value": <val>,
         "create": true, "extend": true}

    * ``path`` — destination pointer (template-expanded)
    * ``value`` — value to write (special-resolved + template-expanded)
    * ``create`` (default ``true``) — auto-create missing intermediate nodes
    * ``extend`` (default ``true``) — if appending a list to list, extend instead of wrap

    Special ``"-"`` leaf: append to parent list.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        extend_list = bool(ctx.engine.process_value(step.get("extend", True), ctx))

        value = ctx.engine.process_value(step["value"], ctx)

        # Handle '-' append (may need to convert parent to list or create it)
        if path.endswith("/-"):
            parent_path = path.rsplit("/", 1)[0] or "/"

            # Try to get parent
            try:
                parent = ctx.engine.processor.get("@:" + parent_path, ctx)
            except Exception:
                # Parent doesn't exist
                if create:
                    # Create parent as empty list (set always writes to dest)
                    ctx.engine.processor.set(parent_path, ctx, [])
                    parent = ctx.engine.processor.get("@:" + parent_path, ctx)
                else:
                    raise

            # Ensure parent is a list
            if not isinstance(parent, list):
                if create:
                    # Convert to list (wrap value if not empty)
                    if parent == {}:
                        ctx.engine.processor.set(parent_path, ctx, [])
                    else:
                        ctx.engine.processor.set(parent_path, ctx, [parent])
                    parent = ctx.engine.processor.get("@:" + parent_path, ctx)
                else:
                    raise TypeError(f"{path}: parent is not a list (append)")

            # Append value
            if isinstance(value, list) and extend_list:
                parent.extend(value)
            else:
                parent.append(value)
        else:
            # Normal set (set always writes to dest)
            ctx.engine.processor.set(path, ctx, value)

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# copy — copy from source pointer to dest path
# ─────────────────────────────────────────────────────────────────────────────

class CopyHandler(ActionHandler):
    """``op: copy`` — copy value from source to destination.

    Schema::

        {"op": "copy", "from": "/src/path", "path": "/dest/path",
         "create": true, "extend": true, "ignore_missing": false,
         "default": <fallback>}

    * ``from`` — source pointer (supports slices)
    * ``path`` — destination pointer
    * ``ignore_missing`` — if true, skip on missing source
    * ``default`` — fallback if source missing

    Delegates to SetHandler after resolving the value.
    """

    def __init__(self, set_handler: SetHandler | None = None) -> None:
        self._set = set_handler or SetHandler()

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        extend_list = bool(ctx.engine.process_value(step.get("extend", True), ctx))

        ptr = ctx.engine.process_value(step["from"], ctx)
        ignore = bool(ctx.engine.process_value(step.get("ignore_missing", False), ctx))

        try:
            value = copy.deepcopy(ctx.engine.processor.get(ptr, ctx))
        except Exception:
            if "default" in step:
                value = copy.deepcopy(ctx.engine.process_value(step["default"], ctx))
            elif ignore:
                return ctx.dest
            else:
                raise

        # Delegate to SetHandler
        return self._set.execute(
            {"op": "set", "path": path, "value": value,
             "create": create, "extend": extend_list},
            ctx
        )


# ─────────────────────────────────────────────────────────────────────────────
# delete — remove node at path
# ─────────────────────────────────────────────────────────────────────────────

class DeleteHandler(ActionHandler):
    """``op: delete`` — remove value at path.

    Schema::

        {"op": "delete", "path": "/path/to/remove",
         "ignore_missing": true}

    * ``ignore_missing`` (default ``true``) — don't raise if path doesn't exist
    * ``"-"`` is **not** allowed as leaf (no "delete from end")
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = ctx.engine.process_value(step["path"], ctx)
        ignore = bool(ctx.engine.process_value(step.get("ignore_missing", True), ctx))

        # Validate no '-'
        if path.endswith("/-"):
            raise ValueError("'-' not allowed in delete")

        try:
            ctx.engine.processor.delete(path, ctx, ignore_missing=ignore)
        except (KeyError, IndexError):
            if not ignore:
                raise

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# foreach — iterate over array
# ─────────────────────────────────────────────────────────────────────────────

class ForeachHandler(ActionHandler):
    """``op: foreach`` — iterate over array from source, execute nested actions.

    Schema::

        {"op": "foreach", "in": "/source/array", "as": "item",
         "do": <actions>, "skip_empty": true, "default": []}

    * ``in`` — source array pointer (supports slices)
    * ``as`` (default ``"item"``) — variable name in extended source
    * ``do`` — nested actions (each iteration sees extended source)
    * ``skip_empty`` (default ``true``) — skip if array is empty
    * ``default`` — fallback if ``in`` pointer fails

    If source is dict, converts to list of ``(key, value)`` tuples.
    If iteration fails, rollback dest to snapshot.

    Args:
        max_items: Maximum number of items to iterate (default: 100_000).
    """

    def __init__(self, max_items: int = 100_000) -> None:
        self._max_items = max_items

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        arr_ptr = ctx.engine.process_value(step["in"], ctx)

        default = copy.deepcopy(ctx.engine.process_value(step.get("default", []), ctx))
        skip_empty = bool(ctx.engine.process_value(step.get("skip_empty", True), ctx))

        try:
            arr = ctx.engine.processor.get(arr_ptr, ctx)
        except Exception:
            arr = default

        if not arr and skip_empty:
            return ctx.dest

        if isinstance(arr, dict):
            arr = list(arr.items())

        # Check array size
        arr_len = len(arr) if hasattr(arr, '__len__') else sum(1 for _ in arr)
        if arr_len > self._max_items:
            raise ValueError(
                f"Foreach array size ({arr_len}) exceeds maximum ({self._max_items})"
            )

        var = ctx.engine.process_value(step.get("as", "item"), ctx)
        body = step["do"]
        snapshot = copy.deepcopy(ctx.dest)

        try:
            for elem in arr:
                extended = {"_": ctx.source, var: elem}
                foreach_ctx = ctx.copy(new_source=extended)
                ctx.dest = ctx.engine.apply_to_context(body, foreach_ctx)
        except Exception:
            # Rollback on error
            ctx.dest = snapshot
            raise

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# while — loop with condition
# ─────────────────────────────────────────────────────────────────────────────

class WhileHandler(ActionHandler):
    """``op: while`` — loop while condition holds.

    Schema (path-based)::

        {"op": "while", "do": <actions>,
         "do_while": false, "path": "/check/path",
         "equals": <expected>, "exists": true}

    Schema (expression-based)::
        {"op": "while", "do": <actions>, "do_while": false, "cond": <expression>}

    * ``cond`` — expression evaluated as boolean each iteration
    * ``do`` — actions to execute while condition is true

    If iteration fails, rollback dest to snapshot.

    Args:
        max_iterations: Maximum number of loop iterations (default: 10_000).
    """

    def __init__(self, max_iterations: int = 10_000) -> None:
        self._max_iterations = max_iterations

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        do_while = bool(ctx.engine.process_value(step.get("do_while", False), ctx))
        snapshot = copy.deepcopy(ctx.dest)

        try:
            iteration = 0
            while True:
                if iteration >= self._max_iterations:
                    raise RuntimeError(
                        f"While loop exceeded maximum iterations ({self._max_iterations})"
                    )
                if not do_while:
                    # Evaluate condition
                    if "cond" in step:
                        raw_cond = ctx.engine.process_value(step["cond"], ctx)
                        cond_val = bool(raw_cond)
                    elif "path" in step:
                        try:
                            ptr = ctx.engine.process_value(step["path"], ctx)
                            current = ctx.engine.processor.get(ptr, ctx)
                            missing = False
                        except Exception:
                            current = None
                            missing = True

                        if "equals" in step:
                            expected = ctx.engine.process_value(step["equals"], ctx)
                            cond_val = current == expected and not missing
                        elif ctx.engine.process_value(step.get("exists", False), ctx):
                            cond_val = not missing
                        else:
                            cond_val = bool(current) and not missing
                    else:
                        raise ValueError("while operation requires 'cond' or 'path'")

                    if not cond_val:
                        break

                # Execute body, preserving metadata (functions)
                ctx.dest = ctx.engine.apply_to_context(step["do"], ctx)

                # After first iteration, always check condition
                do_while = False
                iteration += 1
        except Exception:
            # Rollback on error
            ctx.dest = snapshot
            raise

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# if — conditional execution
# ─────────────────────────────────────────────────────────────────────────────

class IfHandler(ActionHandler):
    """``op: if`` — conditionally execute nested actions.

    Schema (path-based)::

        {"op": "if", "path": "/check/path",
         "equals": <expected>, "exists": true,
         "then": <actions>, "else": <actions>}

    Schema (expression-based)::

        {"op": "if", "cond": <expression>,
         "do": <actions>, "else": <actions>}

    * ``path`` + ``equals`` → check value equality
    * ``path`` + ``exists`` → check existence
    * ``path`` alone → check truthiness
    * ``cond`` → evaluate expression as boolean

    Branches: ``then`` / ``else`` / ``do`` (``do`` = alias for ``then``).
    If condition fails, rollback dest to snapshot.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        if "path" in step:
            try:
                ptr = ctx.engine.process_value(step["path"], ctx)
                current = ctx.engine.processor.get(ptr, ctx)
                missing = False
            except Exception:
                current = None
                missing = True

            if "equals" in step:
                expected = ctx.engine.process_value(step["equals"], ctx)
                cond_val = current == expected and not missing
            elif ctx.engine.process_value(step.get("exists", False), ctx):
                cond_val = not missing
            else:
                cond_val = bool(current) and not missing
        else:
            raw_cond = ctx.engine.process_value(step.get("cond"), ctx)
            cond_val = bool(raw_cond)

        # Choose branch
        branch_key = "then" if cond_val else "else"
        branch_key = branch_key if branch_key in step else "do" if cond_val else None
        actions = step.get(branch_key)

        if not actions:
            return ctx.dest

        snapshot = copy.deepcopy(ctx.dest)
        try:
            return ctx.engine.apply_to_context(actions, ctx)
        except Exception:
            ctx.dest = snapshot
            raise


# ─────────────────────────────────────────────────────────────────────────────
# exec — execute actions from source or inline
# ─────────────────────────────────────────────────────────────────────────────

class ExecHandler(ActionHandler):
    """``op: exec`` — execute actions stored in source or inline.

    Schema::

        {"op": "exec", "from": "/actions/path", "default": <fallback>,
         "merge": false}

    or::

        {"op": "exec", "actions": <inline_actions>, "merge": false}

    * ``from`` — pointer to actions in source
    * ``actions`` — inline actions spec
    * ``merge`` (default ``false``) — if false, replace dest; if true, merge into dest
    * ``default`` — fallback if ``from`` pointer fails

    Cannot have both ``from`` and ``actions``.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_actions = "actions" in step

        if has_from and has_actions:
            raise ValueError("exec operation cannot have both 'from' and 'actions' parameters")
        if not has_from and not has_actions:
            raise ValueError("exec operation requires either 'from' or 'actions' parameter")

        if has_from:
            actions_ptr = ctx.engine.process_value(step["from"], ctx)
            try:
                actions = ctx.engine.processor.get(actions_ptr, ctx)
            except Exception:
                if "default" in step:
                    actions = ctx.engine.process_value(step["default"], ctx)
                else:
                    raise ValueError(f"Cannot find actions at {actions_ptr}")
        else:
            actions = ctx.engine.process_value(step["actions"], ctx)

        merge = bool(ctx.engine.process_value(step.get("merge", False), ctx))

        if merge:
            return ctx.engine.apply_to_context(actions, ctx)
        else:
            exec_ctx = ctx.copy(new_dest={}, deepcopy_dest=False)
            result = ctx.engine.apply_to_context(actions, exec_ctx)
            return result


# ─────────────────────────────────────────────────────────────────────────────
# update — merge mapping into target
# ─────────────────────────────────────────────────────────────────────────────

class UpdateHandler(ActionHandler):
    """``op: update`` — merge a mapping into destination path.

    Schema::

        {"op": "update", "path": "/target", "from": "/source/data",
         "create": true, "deep": false}

    or::

        {"op": "update", "path": "/target", "value": {...},
         "create": true, "deep": false}

    * ``from`` — source pointer
    * ``value`` — inline mapping
    * ``deep`` (default ``false``) — recursive merge
    * ``create`` (default ``true``) — auto-create target if missing

    Target must be a dict.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        deep = bool(ctx.engine.process_value(step.get("deep", False), ctx))

        if "from" in step:
            ptr = ctx.engine.process_value(step["from"], ctx)
            try:
                update_value = copy.deepcopy(ctx.engine.processor.get(ptr, ctx))
            except Exception:
                if "default" in step:
                    update_value = copy.deepcopy(ctx.engine.process_value(step["default"], ctx))
                else:
                    raise
        elif "value" in step:
            update_value = ctx.engine.process_value(step["value"], ctx)
        else:
            raise ValueError("update operation requires either 'from' or 'value' parameter")

        if not isinstance(update_value, Mapping):
            raise TypeError(f"update value must be a dict, got {type(update_value).__name__}")

        # Get target
        try:
            target = ctx.engine.processor.get("@:" + path, ctx)
        except Exception:
            if create:
                ctx.engine.processor.set(path, ctx, {})
                target = ctx.engine.processor.get("@:" + path, ctx)
            else:
                raise KeyError(f"{path} does not exist")

        if not isinstance(target, Mapping):
            raise TypeError(f"{path} is not a dict, cannot update")

        if deep:
            def deep_update(dst: Any, src_val: Mapping) -> None:
                for key, value in src_val.items():
                    if key in dst and isinstance(dst[key], Mapping) and isinstance(value, Mapping):
                        deep_update(dst[key], value)
                    else:
                        dst[key] = copy.deepcopy(value)

            deep_update(target, update_value)
        else:
            target.update(update_value)

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# distinct — remove duplicates from list
# ─────────────────────────────────────────────────────────────────────────────

class DistinctHandler(ActionHandler):
    """``op: distinct`` — deduplicate list in-place, preserving order.

    Schema::

        {"op": "distinct", "path": "/array", "key": "/field"}

    * ``path`` — pointer to list in dest
    * ``key`` (optional) — pointer within each element for comparison

    If ``key`` is specified, compares ``element[key]`` instead of whole element.
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        path = ctx.engine.process_value(step["path"], ctx)
        lst = ctx.engine.processor.get("@:" + path, ctx)

        if not isinstance(lst, list):
            raise TypeError(f"{path} is not a list (distinct)")

        key = step.get("key", None)
        if key is not None:
            key_path = ctx.engine.process_value(key, ctx)

        seen = set()
        unique = []
        for item in lst:
            if key is not None:
                filter_item = ctx.engine.resolver.get(key_path, item)
            else:
                filter_item = item

            # Only hashable items supported
            try:
                if filter_item not in seen:
                    seen.add(filter_item)
                    unique.append(item)
            except TypeError:
                # Unhashable - always include
                unique.append(item)

        lst[:] = unique
        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# assert — validate source value
# ─────────────────────────────────────────────────────────────────────────────

class AssertHandler(ActionHandler):
    """``op: assert`` — assert value exists in source.

    Schema::

        {"op": "assert", "path": "/source/path", "equals": <expected>, "return": False, "to_path": <path/to/return>}

    or::

        {"op": "assert", "value": <val>, "equals": <expected>, "return": False, "to_path": <path/to/return>}

    * ``path`` — pointer in **source** (template-expanded)
    * ``value`` — direct value to check (alternative to ``path``)
    * ``equals`` (optional) — expected value
    * ``return`` (optional) — if specified, return value instead rising error
    * ``return`` + ``to_path`` → pointer in source to return if assertion fails

    Either ``path`` or ``value`` must be specified, but not both.

    Raises ``AssertionError`` if:
    * Path doesn't exist (when using ``path``)
    * Value doesn't match ``equals``
    """

    def _return_value(self, step: Any, ctx: ExecutionContext, value: Any) -> Any:
        """Return value directly or set it at destination path."""
        if "to_path" in step:
            return ctx.engine.resolver.set(ctx.engine.process_value(step["to_path"], ctx), ctx.dest, value)
        return value

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_path = "path" in step
        has_value = "value" in step

        if has_path and has_value:
            raise ValueError("assert operation cannot have both 'path' and 'value' parameters")
        if not has_path and not has_value:
            raise ValueError("assert operation requires either 'path' or 'value' parameter")

        should_return = ctx.engine.process_value(step.get("return", False), ctx)

        # Get current value either from path or direct value
        if has_value:
            current = ctx.engine.process_value(step["value"], ctx)
        else:
            path = ctx.engine.process_value(step["path"], ctx)
            try:
                current = ctx.engine.processor.get(path, ctx)
            except Exception:
                # Handle missing value
                if should_return:
                    return self._return_value(step, ctx, False)
                raise AssertionError(f"'{path}' does not exist in source")

        # Check equality if specified
        if "equals" in step:
            expected = ctx.engine.process_value(step["equals"], ctx)
            if current != expected:
                if should_return:
                    return self._return_value(step, ctx, False)
                raise AssertionError(f"Value != {expected!r}")

        # Handle return mode
        if should_return:
            return self._return_value(step, ctx, current)

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# try — error handling with except and finally blocks
# ─────────────────────────────────────────────────────────────────────────────

class TryHandler(ActionHandler):
    """``op: try`` — execute actions with error handling.

    Schema::

        {
            "op": "try",
            "do": <actions>,
            "except": <error_handlers>,  // optional
            "finally": <cleanup_actions>,  // optional
        }

    Behavior:
    * Executes actions in ``do`` block
    * If an error occurs:
        - If ``except`` is specified, executes error handlers with error info in metadata
        - If ``except`` is not specified, re-raises the error
    * Always executes ``finally`` block if specified (even if error occurred)
    * Returns the final dest

    Metadata during except block:
    * ``_error_type``: The error class name (e.g., "ValueError")
    * ``_error_message``: The error message string

    Examples::

        # Basic try-except
        {
            "op": "try",
            "do": [
                {"op": "copy", "from": "/might_not_exist", "path": "/result"}
            ],
            "except": [
                {"/error": "Failed to copy value"}
            ]
        }

        # With finally cleanup
        {
            "op": "try",
            "do": [
                {"/status": "processing"},
                {"op": "exec", "from": "/dangerous_operation"}
            ],
            "except": [
                {"/status": "error"},
                {"/error_msg": "${_:/error_message}"}
            ],
            "finally": [
                {"/processed_at": "${now}"}
            ]
        }

        # Without except (finally always runs)
        {
            "op": "try",
            "do": [
                {"op": "copy", "from": "/data", "path": "/backup"}
            ],
            "finally": [
                {"/cleanup": True}
            ]
        }
    """

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        """Execute try-except-finally logic."""
        do_actions = step.get("do")
        except_actions = step.get("except")
        finally_actions = step.get("finally")

        if do_actions is None:
            raise ValueError("try operation requires 'do' parameter")

        error_occurred = False
        error_info = None

        # Try to execute the main actions
        try:
            ctx.engine.apply_to_context(do_actions, ctx)
        except Exception as e:
            error_occurred = True
            error_info = {
                "_error_type": type(e).__name__,
                "_error_message": str(e),
            }

            # If except block exists, execute it with error info in metadata
            if except_actions is not None:
                # Save current metadata and add error info
                old_metadata = ctx.metadata.copy()
                ctx.metadata.update(error_info)

                try:
                    ctx.engine.apply_to_context(except_actions, ctx)
                finally:
                    # Restore original metadata
                    ctx.metadata = old_metadata
            else:
                # No except block, re-raise the error
                # But first execute finally if it exists
                if finally_actions is not None:
                    try:
                        ctx.engine.apply_to_context(finally_actions, ctx)
                    except Exception:
                        pass  # Ignore errors in finally when re-raising original error
                raise

        # Always execute finally block if specified
        if finally_actions is not None:
            ctx.engine.apply_to_context(finally_actions, ctx)

        return ctx.dest
