"""Shorthand expansion — turns compact DSL syntax into explicit op-dicts.

Architecture
------------
Instead of a separate "first-match" tree, shorthand expansion is now implemented
as a sequence of ``StageProcessor``s registered in the main ``StageRegistry``
with different priorities:

* ``AssertShorthandStage``  (priority 100) — extracts ``~assert`` / ``~assertD``
* ``DeleteShorthandStage``  (priority  50) — extracts ``~delete``
* ``AssignShorthandStage``  (priority   0) — fallback for ``/path`` and ``/path[]``

Each processor:
1. Iterates over steps
2. For dicts without ``"op"``, extracts ITS specific keys and expands them
3. Leaves remaining keys in the dict for subsequent processors

This eliminates the need for ``ShorthandNode`` / ``ShorthandRegistry`` — we
reuse the existing ``StageNode`` / ``StageRegistry`` infrastructure.

Example transformation
----------------------
Input::

    {"~assert": "/required", "~delete": "/tmp", "/result": "/value"}

After AssertShorthandStage (priority 100)::

    [{"op": "assert", "path": "/required"}, {"~delete": "/tmp", "/result": "/value"}]

After DeleteShorthandStage (priority 50)::

    [{"op": "assert", "path": "/required"}, {"op": "delete", "path": "/tmp"}, {"/result": "/value"}]

After AssignShorthandStage (priority 0)::

    [{"op": "assert", "path": "/required"}, {"op": "delete", "path": "/tmp"}, {"op": "copy", "from": "/value", "path": "/result"}]
"""

from __future__ import annotations

from typing import Any, List, Mapping

from ..core import ExecutionContext, StageMatcher, StageNode, StageProcessor, StageRegistry


# ─────────────────────────────────────────────────────────────────────────────
# ~assert / ~assertD
# ─────────────────────────────────────────────────────────────────────────────


class AssertShorthandMatcher(StageMatcher):
    """Match if any step is a dict containing ``~assert`` or ``~assertD`` keys."""

    def matches(self, steps: List[Any], ctx: ExecutionContext) -> bool:
        return any(
            isinstance(s, Mapping) and "op" not in s and ("~assert" in s or "~assertD" in s)
            for s in steps
        )


class AssertShorthandProcessor(StageProcessor):
    """Extract and expand ``~assert`` / ``~assertD`` keys from shorthand dicts.

    Expansion rules:
    * Value is a mapping  → each entry becomes ``{op, path, equals}``
    * Value is a list     → each item becomes ``{op, path}``
    * Value is a scalar   → single ``{op, path}``

    Leaves other keys in the dict for subsequent processors.
    """

    def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        out: List[Any] = []
        for step in steps:
            if isinstance(step, Mapping) and "op" not in step:
                expanded = []
                remaining = {}

                for key, value in step.items():
                    if key in ("~assert", "~assertD"):
                        op = "assertD" if key == "~assertD" else "assert"
                        if isinstance(value, Mapping):
                            expanded.extend(
                                [{"op": op, "path": p, "equals": eq} for p, eq in value.items()]
                            )
                        else:
                            paths = value if isinstance(value, list) else [value]
                            expanded.extend([{"op": op, "path": p} for p in paths])
                    else:
                        remaining[key] = value

                out.extend(expanded)
                if remaining:
                    out.append(remaining)
            else:
                out.append(step)
        return out


# ─────────────────────────────────────────────────────────────────────────────
# ~delete
# ─────────────────────────────────────────────────────────────────────────────


class DeleteShorthandMatcher(StageMatcher):
    """Match if any step is a dict containing ``~delete`` key."""

    def matches(self, steps: List[Any], ctx: ExecutionContext) -> bool:
        return any(
            isinstance(s, Mapping) and "op" not in s and "~delete" in s
            for s in steps
        )


class DeleteShorthandProcessor(StageProcessor):
    """Extract and expand ``~delete`` keys from shorthand dicts.

    Expansion rule:
    * One ``delete`` op per path (value can be a single path or a list).
    """

    def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        out: List[Any] = []
        for step in steps:
            if isinstance(step, Mapping) and "op" not in step:
                expanded = []
                remaining = {}

                for key, value in step.items():
                    if key == "~delete":
                        paths = value if isinstance(value, list) else [value]
                        expanded.extend([{"op": "delete", "path": p} for p in paths])
                    else:
                        remaining[key] = value

                out.extend(expanded)
                if remaining:
                    out.append(remaining)
            else:
                out.append(step)
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Fallback: /path or /path[] → set / copy / append
# ─────────────────────────────────────────────────────────────────────────────


class AssignShorthandMatcher(StageMatcher):
    """Match if any step is a dict without ``"op"`` (fallback for all remaining keys)."""

    def matches(self, steps: List[Any], ctx: ExecutionContext) -> bool:
        return any(isinstance(s, Mapping) and "op" not in s for s in steps)


class AssignShorthandProcessor(StageProcessor):
    """Expand remaining pointer-keyed shorthand dicts into ``set`` / ``copy`` ops.

    Expansion rules:
    * Key ends with ``[]``         → append (path rewritten to ``path/-``)
    * Value is a string starting
      with ``/``                   → ``copy`` (pointer reference)
    * Otherwise                    → ``set`` (literal value)

    If no keys remain after previous processors, the step is removed (empty dict).
    """

    def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        out: List[Any] = []
        for step in steps:
            if isinstance(step, Mapping) and "op" not in step:
                if not step:
                    # Empty dict after all extractions — skip it
                    continue

                for key, value in step.items():
                    append = key.endswith("[]")
                    dst = f"{key[:-2]}/-" if append else key

                    if isinstance(value, str) and value.startswith("/"):
                        out.append({"op": "copy", "from": value, "path": dst, "ignore_missing": True})
                    else:
                        out.append({"op": "set", "path": dst, "value": value})
            else:
                out.append(step)
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Factory helper
# ─────────────────────────────────────────────────────────────────────────────


def build_default_shorthand_stages() -> StageRegistry:
    """Create a ``StageRegistry`` pre-loaded with the standard shorthand stages.

    Stages (by priority):
    * ``AssertShorthandProcessor``  (100) — ``~assert`` / ``~assertD``
    * ``DeleteShorthandProcessor``  ( 50) — ``~delete``
    * ``AssignShorthandProcessor``  (  0) — fallback for ``/path`` / ``/path[]``

    Returns a ``StageRegistry`` that can be used directly as the ``stages``
    parameter of a ``Pipeline``, or merged with other stages.
    """
    registry = StageRegistry()

    registry.register(StageNode(
        name="assert_shorthand",
        priority=100,
        matcher=AssertShorthandMatcher(),
        processor=AssertShorthandProcessor(),
    ))

    registry.register(StageNode(
        name="delete_shorthand",
        priority=50,
        matcher=DeleteShorthandMatcher(),
        processor=DeleteShorthandProcessor(),
    ))

    registry.register(StageNode(
        name="assign_shorthand",
        priority=0,
        matcher=AssignShorthandMatcher(),
        processor=AssignShorthandProcessor(),
    ))

    return registry
