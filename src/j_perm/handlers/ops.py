"""All built-in operation handlers.

Each operation is implemented as an ``ActionHandler`` that accepts steps
with ``{"op": "operation_name", ...}``.

Exports all 17 operation handlers:
* SetHandler, CopyHandler
* DeleteHandler
* ForeachHandler, WhileHandler, IfHandler, ExecHandler
* UpdateHandler, DistinctHandler
* AssertHandler
* TryHandler
* DeserializeHandler, SerializeHandler
* EncodeHandler, DecodeHandler
* HashHandler
"""

from __future__ import annotations

import base64 as _base64
import binascii as _binascii
import copy
import hashlib as _hashlib
import json as _json
from typing import Any, Mapping
from urllib.parse import quote as _url_quote, unquote as _url_unquote

import yaml as _yaml

from ..core import ActionHandler, Compound, CompiledSpec, ExecutionContext
from .merge import deep_update
from .signals import BreakSignal, ContinueSignal, ReturnSignal, ExitSignal


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

        return self._apply(ctx, path, create, extend_list, value)

    def _apply(self, ctx: ExecutionContext, path: Any, create: bool,
               extend_list: bool, value: Any) -> Any:
        """Write *value* at *path* into ``ctx.dest`` (pure, no value-resolution).

        Shared by the sync and async ``set`` handlers — both resolve
        ``path``/``create``/``extend``/``value`` first (sync vs async), then
        delegate the actual mutation here.
        """
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
        return self._apply(ctx, path, ignore)

    def _apply(self, ctx: ExecutionContext, path: Any, ignore: bool) -> Any:
        """Delete *path* from ``ctx.dest`` (pure, no value-resolution)."""
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

class ForeachHandler(ActionHandler, Compound):
    """``op: foreach`` — iterate over array from source, execute nested actions.

    Schema::

        {"op": "foreach", "in": "/source/array", "as": "item",
         "do": <actions>, "skip_empty": true, "default": []}

    * ``in`` — source array pointer (supports slices)
    * ``in_value`` — plain array value
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

    # Sentinel returned by ``_normalize_array`` when the loop should be skipped.
    _SKIP = object()

    def nested_spec_keys(self, step: Any) -> list[str]:
        return ["do"]

    @staticmethod
    def _validate_source(step: Any) -> tuple[bool, bool]:
        """Validate the ``in`` / ``in_value`` combination, return their presence."""
        has_in = "in" in step
        has_in_value = "in_value" in step
        if has_in and has_in_value:
            raise ValueError("foreach operation cannot have both 'in' and 'in_value' parameters")
        if not has_in and not has_in_value:
            raise ValueError("foreach operation requires either 'in' or 'in_value' parameter")
        return has_in, has_in_value

    def _normalize_array(self, arr: Any, skip_empty: bool) -> Any:
        """Normalise the source array (pure): skip sentinel, dict→items, size cap."""
        if not arr and skip_empty:
            return self._SKIP
        if isinstance(arr, dict):
            arr = list(arr.items())
        arr_len = len(arr) if hasattr(arr, '__len__') else sum(1 for _ in arr)
        if arr_len > self._max_items:
            raise ValueError(
                f"Foreach array size ({arr_len}) exceeds maximum ({self._max_items})"
            )
        return arr

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return self._run(step, ctx, None)

    def execute_compiled(self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return self._run(step, ctx, nested.get("do"))

    def _run(self, step: Any, ctx: ExecutionContext, compiled_body: Any) -> Any:
        has_in, has_in_value = self._validate_source(step)
        skip_empty = bool(ctx.engine.process_value(step.get("skip_empty", True), ctx))

        if has_in_value:
            arr = ctx.engine.process_value(step["in_value"], ctx)
        else:
            arr_ptr = ctx.engine.process_value(step["in"], ctx)
            default = copy.deepcopy(ctx.engine.process_value(step.get("default", []), ctx))
            try:
                arr = ctx.engine.processor.get(arr_ptr, ctx)
            except Exception:
                arr = default

        arr = self._normalize_array(arr, skip_empty)
        if arr is self._SKIP:
            return ctx.dest

        var = ctx.engine.process_value(step.get("as", "item"), ctx)
        body = step["do"]
        snapshot = copy.deepcopy(ctx.dest)

        try:
            for elem in arr:
                # Merge loop variable into existing temp_read_only so that outer
                # bindings (e.g. function parameters) remain accessible inside the body.
                foreach_ctx = ctx.copy(new_temp_read_only={**ctx.temp_read_only, var: elem})
                try:
                    if compiled_body is not None:
                        ctx.dest = compiled_body.run(foreach_ctx)
                    else:
                        ctx.dest = ctx.engine.apply_to_context(body, foreach_ctx)
                except ContinueSignal:
                    # Preserve changes made before $continue, skip to next element
                    ctx.dest = foreach_ctx.dest
                except BreakSignal:
                    # Preserve changes made before $break, stop the loop
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
            # Rollback on error
            ctx.dest = snapshot
            raise

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# while — loop with condition
# ─────────────────────────────────────────────────────────────────────────────

class WhileHandler(ActionHandler, Compound):
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

    def nested_spec_keys(self, step: Any) -> list[str]:
        return ["do"]

    def _eval_condition(self, step: Any, ctx: ExecutionContext) -> bool:
        """Evaluate the while condition. Returns True to continue looping."""
        if "cond" in step:
            return bool(ctx.engine.process_value(step["cond"], ctx))
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
                return current == expected and not missing
            elif ctx.engine.process_value(step.get("exists", False), ctx):
                return not missing
            else:
                return bool(current) and not missing
        raise ValueError("while operation requires 'cond' or 'path'")

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return self._run(step, ctx, None)

    def execute_compiled(self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return self._run(step, ctx, nested.get("do"))

    def _run(self, step: Any, ctx: ExecutionContext, compiled_body: Any) -> Any:
        do_while = bool(ctx.engine.process_value(step.get("do_while", False), ctx))
        body = step["do"]
        snapshot = copy.deepcopy(ctx.dest)

        try:
            iteration = 0
            while True:
                if iteration >= self._max_iterations:
                    raise RuntimeError(
                        f"While loop exceeded maximum iterations ({self._max_iterations})"
                    )
                if not do_while and not self._eval_condition(step, ctx):
                    break

                try:
                    if compiled_body is not None:
                        ctx.dest = compiled_body.run(ctx)
                    else:
                        ctx.dest = ctx.engine.apply_to_context(body, ctx)
                except ContinueSignal:
                    pass
                except BreakSignal:
                    break

                do_while = False
                iteration += 1
        except (ReturnSignal, ExitSignal):
            raise  # Propagate without rollback ($return / $exit)
        except Exception:
            ctx.dest = snapshot
            raise

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# if — conditional execution
# ─────────────────────────────────────────────────────────────────────────────

class IfHandler(ActionHandler, Compound):
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

    def nested_spec_keys(self, step: Any) -> list[str]:
        return [k for k in ("then", "do", "else") if k in step]

    def _eval_condition(self, step: Any, ctx: ExecutionContext) -> bool:
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
                return current == expected and not missing
            elif ctx.engine.process_value(step.get("exists", False), ctx):
                return not missing
            else:
                return bool(current) and not missing
        raw_cond = ctx.engine.process_value(step.get("cond"), ctx)
        return bool(raw_cond)

    def _get_branch_key(self, step: Any, cond_val: bool) -> Any:
        branch_key = "then" if cond_val else "else"
        return branch_key if branch_key in step else ("do" if cond_val else None)

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        return self._run(step, ctx, None)

    def execute_compiled(self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return self._run(step, ctx, nested)

    def _run(self, step: Any, ctx: ExecutionContext, nested: Any) -> Any:
        cond_val = self._eval_condition(step, ctx)
        branch_key = self._get_branch_key(step, cond_val)
        actions = step.get(branch_key)

        if not actions:
            return ctx.dest

        compiled_branch = nested.get(branch_key) if (nested and branch_key) else None
        snapshot = copy.deepcopy(ctx.dest)
        try:
            if compiled_branch is not None:
                return compiled_branch.run(ctx)
            return ctx.engine.apply_to_context(actions, ctx)
        except (BreakSignal, ContinueSignal, ReturnSignal, ExitSignal):
            raise  # Don't rollback — propagate control flow signals as-is
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

        return self._apply(ctx, path, create, deep, update_value)

    def _apply(self, ctx: ExecutionContext, path: Any, create: bool,
               deep: bool, update_value: Any) -> Any:
        """Merge *update_value* into the mapping at *path* (pure, no resolution)."""
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
        key = step.get("key", None)
        key_path = ctx.engine.process_value(key, ctx) if key is not None else None
        return self._apply(ctx, path, key, key_path)

    def _apply(self, ctx: ExecutionContext, path: Any, key: Any, key_path: Any) -> Any:
        """Deduplicate the list at *path* in place (pure, no value-resolution)."""
        lst = ctx.engine.processor.get("@:" + path, ctx)

        if not isinstance(lst, list):
            raise TypeError(f"{path} is not a list (distinct)")

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

class TryHandler(ActionHandler, Compound):
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
                {"/error_msg": "${_:/_error_message}"}
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

    def nested_spec_keys(self, step: Any) -> list[str]:
        return [k for k in ("do", "except", "finally") if k in step]

    def _run_body(self, actions: Any, compiled: Any, ctx: ExecutionContext) -> None:
        if compiled is not None:
            compiled.run(ctx)
        else:
            ctx.engine.apply_to_context(actions, ctx)

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        """Execute try-except-finally logic."""
        return self._execute_impl(step, ctx, {})

    def execute_compiled(self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        return self._execute_impl(step, ctx, nested)

    def _execute_impl(self, step: Any, ctx: ExecutionContext, nested: dict[str, CompiledSpec]) -> Any:
        do_actions = step.get("do")
        except_actions = step.get("except")
        finally_actions = step.get("finally")

        if do_actions is None:
            raise ValueError("try operation requires 'do' parameter")

        compiled_do = nested.get("do")
        compiled_except = nested.get("except")
        compiled_finally = nested.get("finally")

        try:
            self._run_body(do_actions, compiled_do, ctx)
        except (BreakSignal, ContinueSignal, ReturnSignal, ExitSignal):
            if finally_actions is not None:
                try:
                    self._run_body(finally_actions, compiled_finally, ctx)
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
                    self._run_body(except_actions, compiled_except, ctx)
                finally:
                    ctx.temp_read_only.pop('_error_type', None)
                    ctx.temp_read_only.pop('_error_message', None)
            else:
                if finally_actions is not None:
                    try:
                        self._run_body(finally_actions, compiled_finally, ctx)
                    except Exception:
                        pass
                raise

        if finally_actions is not None:
            self._run_body(finally_actions, compiled_finally, ctx)

        return ctx.dest


# ─────────────────────────────────────────────────────────────────────────────
# deserialize — parse serialized string into a value
# ─────────────────────────────────────────────────────────────────────────────

_DESERIALIZE_FORMATS = frozenset({"json", "pretty_json", "yaml"})


class DeserializeHandler(ActionHandler):
    """``op: deserialize`` — parse a serialized string into a structured value.

    Schema::

        {"op": "deserialize", "from": "/raw_string",
         "format": "json", "path": "/result",
         "create": true, "extend": true, "default": <fallback>}

    * ``from`` — source pointer to the serialized string (supports all context prefixes)
    * ``value`` — inline serialized string (mutually exclusive with ``from``)
    * ``format`` — one of ``"json"``, ``"pretty_json"`` (alias for ``"json"``), ``"yaml"``
    * ``path`` — destination pointer
    * ``create`` (default ``true``) — auto-create intermediate nodes
    * ``extend`` (default ``true``) — extend list instead of wrap when appending
    * ``default`` — fallback value if the source pointer fails or parsing fails

    Supported formats:

    * ``json`` / ``pretty_json`` — RFC 8259 JSON (compact and pretty-printed both accepted)
    * ``yaml`` — YAML document parsed with ``yaml.safe_load``
    """

    def __init__(self, set_handler: SetHandler | None = None) -> None:
        self._set = set_handler or SetHandler()

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError("deserialize cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError("deserialize requires either 'from' or 'value'")

        fmt = ctx.engine.process_value(step.get("format", "json"), ctx)
        if fmt not in _DESERIALIZE_FORMATS:
            raise ValueError(
                f"deserialize: unknown format '{fmt}'. Supported: {', '.join(sorted(_DESERIALIZE_FORMATS))}"
            )

        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        extend_list = bool(ctx.engine.process_value(step.get("extend", True), ctx))

        if has_from:
            ptr = ctx.engine.process_value(step["from"], ctx)
            try:
                raw = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    fallback = ctx.engine.process_value(step["default"], ctx)
                    return self._set.execute(
                        {"op": "set", "path": path, "value": fallback,
                         "create": create, "extend": extend_list},
                        ctx,
                    )
                raise
        else:
            raw = ctx.engine.process_value(step["value"], ctx)

        try:
            parsed = _parse_format(fmt, raw)
        except Exception as exc:
            if "default" in step:
                fallback = ctx.engine.process_value(step["default"], ctx)
                return self._set.execute(
                    {"op": "set", "path": path, "value": fallback,
                     "create": create, "extend": extend_list},
                    ctx,
                )
            raise ValueError(f"deserialize: failed to parse as '{fmt}': {exc}") from exc

        return self._set.execute(
            {"op": "set", "path": path, "value": parsed, "create": create, "extend": extend_list},
            ctx,
        )


def _parse_format(fmt: str, raw: str) -> Any:
    if fmt in ("json", "pretty_json"):
        return _json.loads(raw)
    # fmt == "yaml"
    return _yaml.safe_load(raw)


# ─────────────────────────────────────────────────────────────────────────────
# serialize — render a value into a serialized string
# ─────────────────────────────────────────────────────────────────────────────

_SERIALIZE_FORMATS = frozenset({"json", "pretty_json", "yaml"})


def _serialize_format(fmt: str, value: Any) -> str:
    if fmt == "json":
        return _json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if fmt == "pretty_json":
        return _json.dumps(value, ensure_ascii=False, indent=2)
    # fmt == "yaml"
    return _yaml.safe_dump(value, sort_keys=False, allow_unicode=True)


class SerializeHandler(ActionHandler):
    """``op: serialize`` — render a value into a serialized string.

    Schema::

        {"op": "serialize", "from": "/data",
         "format": "json", "path": "/result",
         "create": true, "extend": true, "default": <fallback>}

    * ``from`` — source pointer to the value (supports all context prefixes)
    * ``value`` — inline value (mutually exclusive with ``from``)
    * ``format`` — one of ``"json"`` (compact), ``"pretty_json"`` (indented),
      ``"yaml"``
    * ``path`` — destination pointer
    * ``create`` (default ``true``) — auto-create intermediate nodes
    * ``extend`` (default ``true``) — extend list instead of wrap when appending
    * ``default`` — fallback value if the source pointer fails or serialization fails

    Inverse of :class:`DeserializeHandler`.
    """

    def __init__(self, set_handler: SetHandler | None = None) -> None:
        self._set = set_handler or SetHandler()

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError("serialize cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError("serialize requires either 'from' or 'value'")

        fmt = ctx.engine.process_value(step.get("format", "json"), ctx)
        if fmt not in _SERIALIZE_FORMATS:
            raise ValueError(
                f"serialize: unknown format '{fmt}'. Supported: {', '.join(sorted(_SERIALIZE_FORMATS))}"
            )

        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        extend_list = bool(ctx.engine.process_value(step.get("extend", True), ctx))

        if has_from:
            ptr = ctx.engine.process_value(step["from"], ctx)
            try:
                value = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    return self._write_default(step, ctx, path, create, extend_list)
                raise
        else:
            value = ctx.engine.process_value(step["value"], ctx)

        try:
            rendered = _serialize_format(fmt, value)
        except Exception as exc:
            if "default" in step:
                return self._write_default(step, ctx, path, create, extend_list)
            raise ValueError(f"serialize: failed to render as '{fmt}': {exc}") from exc

        return self._set.execute(
            {"op": "set", "path": path, "value": rendered, "create": create, "extend": extend_list},
            ctx,
        )

    def _write_default(self, step: Any, ctx: ExecutionContext, path: Any,
                       create: bool, extend_list: bool) -> Any:
        fallback = ctx.engine.process_value(step["default"], ctx)
        return self._set.execute(
            {"op": "set", "path": path, "value": fallback, "create": create, "extend": extend_list},
            ctx,
        )


# ─────────────────────────────────────────────────────────────────────────────
# encode / decode — text ↔ text through a base/url codec
# ─────────────────────────────────────────────────────────────────────────────

_CODECS = frozenset({
    "base64", "base64url", "base32", "base16", "hex", "base85", "ascii85", "url",
})


def _encode_codec(codec: str, raw: str, encoding: str) -> str:
    """Encode text *raw* into an ASCII codec string."""
    if codec == "url":
        return _url_quote(raw, safe="", encoding=encoding)
    data = raw.encode(encoding)
    if codec == "base64":
        return _base64.b64encode(data).decode("ascii")
    if codec == "base64url":
        return _base64.urlsafe_b64encode(data).decode("ascii")
    if codec == "base32":
        return _base64.b32encode(data).decode("ascii")
    if codec == "base16":
        return _base64.b16encode(data).decode("ascii")
    if codec == "hex":
        return _binascii.hexlify(data).decode("ascii")
    if codec == "base85":
        return _base64.b85encode(data).decode("ascii")
    # codec == "ascii85"
    return _base64.a85encode(data).decode("ascii")


def _decode_codec(codec: str, raw: str, encoding: str) -> str:
    """Decode a codec string *raw* back into text."""
    if codec == "url":
        return _url_unquote(raw, encoding=encoding)
    ascii_bytes = raw.encode("ascii")
    if codec == "base64":
        data = _base64.b64decode(ascii_bytes, validate=True)
    elif codec == "base64url":
        data = _base64.urlsafe_b64decode(ascii_bytes)
    elif codec == "base32":
        data = _base64.b32decode(ascii_bytes)
    elif codec == "base16":
        data = _base64.b16decode(ascii_bytes)
    elif codec == "hex":
        data = _binascii.unhexlify(ascii_bytes)
    elif codec == "base85":
        data = _base64.b85decode(ascii_bytes)
    else:  # codec == "ascii85"
        data = _base64.a85decode(ascii_bytes)
    return data.decode(encoding)


class _CodecHandler(ActionHandler):
    """Shared ``from``/``value`` + codec resolution for ``encode`` / ``decode``.

    Subclasses set :attr:`_op` (the op name, for messages) and implement
    :meth:`_transform` (the pure text→text codec step).
    """

    _op: str = ""

    def __init__(self, set_handler: SetHandler | None = None) -> None:
        self._set = set_handler or SetHandler()

    def _transform(self, codec: str, raw: str, encoding: str) -> str:  # pragma: no cover
        raise NotImplementedError

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError(f"{self._op} cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError(f"{self._op} requires either 'from' or 'value'")

        codec = ctx.engine.process_value(step.get("codec", "base64"), ctx)
        if codec not in _CODECS:
            raise ValueError(
                f"{self._op}: unknown codec '{codec}'. Supported: {', '.join(sorted(_CODECS))}"
            )

        encoding = ctx.engine.process_value(step.get("encoding", "utf-8"), ctx)
        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        extend_list = bool(ctx.engine.process_value(step.get("extend", True), ctx))

        if has_from:
            ptr = ctx.engine.process_value(step["from"], ctx)
            try:
                raw = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    return self._write_default(step, ctx, path, create, extend_list)
                raise
        else:
            raw = ctx.engine.process_value(step["value"], ctx)

        try:
            result = self._transform(codec, raw, encoding)
        except Exception as exc:
            if "default" in step:
                return self._write_default(step, ctx, path, create, extend_list)
            verb = "encode" if self._op == "encode" else "decode"
            raise ValueError(f"{self._op}: failed to {verb} as '{codec}': {exc}") from exc

        return self._set.execute(
            {"op": "set", "path": path, "value": result, "create": create, "extend": extend_list},
            ctx,
        )

    def _write_default(self, step: Any, ctx: ExecutionContext, path: Any,
                       create: bool, extend_list: bool) -> Any:
        fallback = ctx.engine.process_value(step["default"], ctx)
        return self._set.execute(
            {"op": "set", "path": path, "value": fallback, "create": create, "extend": extend_list},
            ctx,
        )


class EncodeHandler(_CodecHandler):
    """``op: encode`` — encode a text string through a base/url codec.

    Schema::

        {"op": "encode", "from": "/text",
         "codec": "base64", "encoding": "utf-8", "path": "/result",
         "create": true, "extend": true, "default": <fallback>}

    * ``from`` — source pointer to the text (supports all context prefixes)
    * ``value`` — inline text (mutually exclusive with ``from``)
    * ``codec`` — ``base64`` (default), ``base64url``, ``base32``, ``base16``,
      ``hex``, ``base85``, ``ascii85``, ``url`` (percent-encoding)
    * ``encoding`` (default ``"utf-8"``) — text encoding before/after the codec
    * ``path`` — destination pointer
    * ``create`` / ``extend`` — as in ``set``
    * ``default`` — fallback if the source pointer or the codec fails

    Text → ``encoding`` bytes → codec → ASCII string.  Inverse of ``decode``.
    """

    _op = "encode"

    def _transform(self, codec: str, raw: str, encoding: str) -> str:
        return _encode_codec(codec, raw, encoding)


class DecodeHandler(_CodecHandler):
    """``op: decode`` — decode a codec string back into text.

    Schema mirrors :class:`EncodeHandler`.  Codec string → bytes →
    ``encoding``-decoded text.  Inverse of ``encode``.
    """

    _op = "decode"

    def _transform(self, codec: str, raw: str, encoding: str) -> str:
        return _decode_codec(codec, raw, encoding)


# ─────────────────────────────────────────────────────────────────────────────
# hash — deterministic digest of data
# ─────────────────────────────────────────────────────────────────────────────

_HASH_ALGOS = frozenset({
    "sha256", "sha512", "sha1", "md5", "sha3_256", "sha3_512", "blake2b", "blake2s",
})
_HASH_OUTPUTS = frozenset({"hex", "base64", "base64url"})


def _hash_input_bytes(value: Any, encoding: str) -> bytes:
    """Turn *value* into the bytes to hash: a string uses its own encoding, any
    other value is canonically serialized so equal objects hash equally."""
    if isinstance(value, str):
        return value.encode(encoding)
    canonical = _json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return canonical.encode(encoding)


def _hash_digest(algo: str, output: str, data: bytes) -> str:
    digest = _hashlib.new(algo, data).digest()
    if output == "hex":
        return _binascii.hexlify(digest).decode("ascii")
    if output == "base64":
        return _base64.b64encode(digest).decode("ascii")
    # output == "base64url"
    return _base64.urlsafe_b64encode(digest).decode("ascii")


class HashHandler(ActionHandler):
    """``op: hash`` — compute a deterministic digest of a value.

    Schema::

        {"op": "hash", "from": "/obj",
         "algo": "sha256", "output": "hex", "encoding": "utf-8",
         "path": "/checksum", "create": true, "extend": true, "default": <fallback>}

    * ``from`` — source pointer to the data (supports all context prefixes)
    * ``value`` — inline value (mutually exclusive with ``from``)
    * ``algo`` — ``sha256`` (default), ``sha512``, ``sha1``, ``md5``,
      ``sha3_256``, ``sha3_512``, ``blake2b``, ``blake2s``
    * ``output`` — digest encoding: ``hex`` (default), ``base64``, ``base64url``
    * ``encoding`` (default ``"utf-8"``) — text encoding for string / canonical inputs
    * ``path`` — destination pointer
    * ``create`` / ``extend`` — as in ``set``
    * ``default`` — fallback if the source pointer fails

    A string input is hashed as its ``encoding`` bytes; any other value is first
    canonically serialized (``json.dumps(value, sort_keys=True)``) so equal
    objects hash equally.  Deterministic — a checksum check is just
    ``hash`` + ``assert``.
    """

    def __init__(self, set_handler: SetHandler | None = None) -> None:
        self._set = set_handler or SetHandler()

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        has_from = "from" in step
        has_value = "value" in step

        if has_from and has_value:
            raise ValueError("hash cannot have both 'from' and 'value'")
        if not has_from and not has_value:
            raise ValueError("hash requires either 'from' or 'value'")

        algo = ctx.engine.process_value(step.get("algo", "sha256"), ctx)
        if algo not in _HASH_ALGOS:
            raise ValueError(
                f"hash: unknown algo '{algo}'. Supported: {', '.join(sorted(_HASH_ALGOS))}"
            )

        output = ctx.engine.process_value(step.get("output", "hex"), ctx)
        if output not in _HASH_OUTPUTS:
            raise ValueError(
                f"hash: unknown output '{output}'. Supported: {', '.join(sorted(_HASH_OUTPUTS))}"
            )

        encoding = ctx.engine.process_value(step.get("encoding", "utf-8"), ctx)
        path = ctx.engine.process_value(step["path"], ctx)
        create = bool(ctx.engine.process_value(step.get("create", True), ctx))
        extend_list = bool(ctx.engine.process_value(step.get("extend", True), ctx))

        if has_from:
            ptr = ctx.engine.process_value(step["from"], ctx)
            try:
                value = ctx.engine.processor.get(ptr, ctx)
            except Exception:
                if "default" in step:
                    fallback = ctx.engine.process_value(step["default"], ctx)
                    return self._set.execute(
                        {"op": "set", "path": path, "value": fallback,
                         "create": create, "extend": extend_list},
                        ctx,
                    )
                raise
        else:
            value = ctx.engine.process_value(step["value"], ctx)

        digest = _hash_digest(algo, output, _hash_input_bytes(value, encoding))

        return self._set.execute(
            {"op": "set", "path": path, "value": digest, "create": create, "extend": extend_list},
            ctx,
        )
