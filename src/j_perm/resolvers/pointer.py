"""JSON-Pointer-based ValueResolver.

Self-contained implementation with full support for:

* RFC 6901 JSON Pointer (``/a/b/0``)
* ``..`` parent reference
* Slice notation ``[start:end]``
* Custom escape tokens (``~0`` … ``~3``)
* ``-`` for list-append in *set*
* Root references (``""``, ``"/"``, ``"."``) work on **scalars** too
"""

from __future__ import annotations

import re
from typing import Any, List, Tuple

from ..core import ValueResolver


class PointerResolver(ValueResolver):
    """Self-contained ``ValueResolver`` with JSON Pointer semantics.

    Unlike the old ``PointerManager`` wrapper, this implementation:
    - Has no external dependencies
    - Supports root references on scalars (``get(".", 42)`` → ``42``)
    - Fully inlined and optimized for the new architecture
    """

    _SLICE_RE = re.compile(r"(.+)\[(-?\d*):(-?\d*)]$")

    # -- read ---------------------------------------------------------------

    def get(self, path: str, data: Any) -> Any:
        """Read value at *path* (supports slices).

        Examples::

            get("/a/b", {"a": {"b": 42}})       → 42
            get("/arr/0", {"arr": [1, 2]})      → 1
            get("/arr[1:]", {"arr": [1, 2, 3]}) → [2, 3]
            get(".", 42)                        → 42  # scalars work!
            get("/", {"": "root"})              → "root"
        """
        return self._maybe_slice(path, data)

    def set(self, path: str, data: Any, value: Any) -> Any:
        """Write *value* at *path*.

        Special cases:

        * ``path`` in ``("", "/", ".")`` → replace the entire root with *value*.
        * Leaf ``"-"``                   → append to parent list.
        * Numeric leaf on a list         → extend with ``None`` if index is out
                                           of range (auto-grow).

        Examples::

            set("/a", {}, 1)          → {"a": 1}
            set("/arr/-", {"arr": []}, 2) → {"arr": [2]}
            set(".", None, 42)        → 42  # root replacement works on scalars
        """
        if path in ("", "/", "."):
            return value

        parent, leaf = self._ensure_parent(data, path, create=True)

        if leaf == "-":
            if not isinstance(parent, list):
                raise TypeError(f"{path}: parent is not a list (append '-')")
            parent.append(value)
        elif isinstance(parent, list):
            idx = int(leaf)
            while idx >= len(parent):
                parent.append(None)
            parent[idx] = value
        else:
            parent[leaf] = value

        return data

    def delete(self, path: str, data: Any) -> Any:
        """Remove the value at *path*.

        Works on both dicts (by key) and lists (by integer index).
        """
        parent, leaf = self._ensure_parent(data, path, create=False)
        if isinstance(parent, list):
            del parent[int(leaf)]
        else:
            del parent[leaf]
        return data

    # -- internal helpers ---------------------------------------------------

    def _decode(self, tok: str) -> str:
        """Decode a single JSON Pointer token (RFC6901 + custom escapes)."""
        return (
            tok.replace("~0", "~")
            .replace("~1", "/")
            .replace("~2", "$")
            .replace("~3", ".")
        )

    def _get_pointer(self, doc: Any, ptr: str) -> Any:
        """Read value by JSON Pointer, supporting root and '..' segments."""
        if ptr in ("", "/", "."):
            return doc

        tokens = ptr.lstrip("/").split("/")
        cur: Any = doc
        parents: List[Tuple[Any, Any]] = []

        for raw_tok in tokens:
            if raw_tok == "..":
                if parents:
                    cur, _ = parents.pop()
                else:
                    cur = doc
                continue

            key = self._decode(raw_tok)

            if isinstance(cur, (list, tuple)):
                idx = int(key)
                parents.append((cur, idx))
                cur = cur[idx]
            else:
                parents.append((cur, key))
                cur = cur[key]

        return cur

    def _maybe_slice(self, ptr: str, src: Any) -> Any:
        """Resolve a pointer and optional Python-style slice suffix ``[start:end]`` for arrays."""
        m = self._SLICE_RE.match(ptr)
        if m:
            base, s, e = m.groups()
            seq = self._get_pointer(src, base)
            if not isinstance(seq, (list, tuple)):
                raise TypeError(f"{base} is not a list (slice requested)")

            start = int(s) if s else None
            end = int(e) if e else None
            return seq[start:end]

        return self._get_pointer(src, ptr)

    def _ensure_parent(
            self,
            doc: Any,
            ptr: str,
            *,
            create: bool = False,
    ) -> Tuple[Any, str]:
        """Return (container, leaf_key) for *ptr*, optionally creating intermediate nodes."""
        raw_parts = ptr.lstrip("/").split("/")
        parts: List[str] = []

        for raw in raw_parts:
            if raw == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(raw)

        if not parts:
            return doc, ""

        cur: Any = doc

        for raw in parts[:-1]:
            token = self._decode(raw)

            if isinstance(cur, list):
                idx = int(token)
                if idx >= len(cur):
                    if create:
                        while idx >= len(cur):
                            cur.append({})
                    else:
                        raise IndexError(f"{ptr}: index {idx} out of range")
                cur = cur[idx]
            else:
                if token not in cur:
                    if create:
                        cur[token] = {}
                    else:
                        raise KeyError(f"{ptr}: missing key '{token}'")
                cur = cur[token]

        leaf = self._decode(parts[-1])
        return cur, leaf
