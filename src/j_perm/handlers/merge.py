"""Document-merge utilities shared by handlers.

Two distinct merge semantics live here so that call sites pick the one they
mean explicitly:

``deep_update``
    Dict-recursive merge where any **non-dict** value (including lists) from the
    source *replaces* the destination slot.  This is the semantics of
    ``op: update`` (``UpdateHandler``).

``deep_merge``
    Dict-recursive merge where **lists are concatenated** (source appended to
    destination) and scalars replace.  This is the semantics used by the
    parallel ``foreach`` ordered merge, where each isolated iteration produces a
    *delta* document (built on a fresh ``{}`` dest) and the deltas are folded
    into the accumulator in input order — so append-style bodies
    (``{"op": "set", "path": "/result/-", ...}``) accumulate correctly.
"""

from __future__ import annotations

import copy
from typing import Any, Mapping


def deep_update(dst: Any, src: Mapping[Any, Any]) -> None:
    """Recursively merge mapping *src* into mapping *dst* in place.

    Nested dicts are merged key-by-key; any non-dict value (including lists)
    replaces the destination slot with a deep copy.  ``dst`` is mutated; there
    is no return value (both arguments must already be mappings).
    """
    for key, value in src.items():
        if key in dst and isinstance(dst[key], Mapping) and isinstance(value, Mapping):
            deep_update(dst[key], value)
        else:
            dst[key] = copy.deepcopy(value)


def deep_merge(dst: Any, src: Any) -> Any:
    """Recursively merge *src* into *dst*, concatenating lists.

    * Two mappings  → merged key-by-key (recursing).
    * Two lists     → *src* (deep-copied) is appended to *dst*.
    * Anything else → *src* (deep-copied) replaces *dst*.

    Returns the merged value.  Mappings and lists are mutated in place and
    returned; for the replace case a fresh deep copy is returned, so callers
    must use the return value (``acc = deep_merge(acc, delta)``) to handle a
    scalar/top-level replacement correctly.
    """
    if isinstance(dst, Mapping) and isinstance(src, Mapping):
        for key, value in src.items():
            if key in dst:
                dst[key] = deep_merge(dst[key], value)
            else:
                dst[key] = copy.deepcopy(value)
        return dst
    if isinstance(dst, list) and isinstance(src, list):
        dst.extend(copy.deepcopy(src))
        return dst
    return copy.deepcopy(src)
