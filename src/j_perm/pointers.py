from __future__ import annotations

import re
from typing import Any, Mapping, MutableMapping, List, Tuple


class PointerManager:
    _SLICE_RE = re.compile(r"(.+)\[(-?\d*):(-?\d*)]$")

    def _decode(self, tok: str) -> str:
        """Decode a single JSON Pointer token, handling RFC6901-style escapes."""
        return (
            tok.replace("~0", "~")
            .replace("~1", "/")
            .replace("~2", "$")
            .replace("~3", ".")
        )

    def get_pointer(self, doc: Any, ptr: str) -> Any:
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

    def maybe_slice(self, ptr: str, src: Mapping[str, Any]) -> Any:
        """Resolve a pointer and optional Python-style slice suffix '[start:end]' for arrays."""
        m = self._SLICE_RE.match(ptr)
        if m:
            base, s, e = m.groups()
            seq = self.get_pointer(src, base)
            if not isinstance(seq, (list, tuple)):
                raise TypeError(f"{base} is not a list (slice requested)")

            start = int(s) if s else None
            end = int(e) if e else None
            return seq[start:end]

        return self.get_pointer(src, ptr)

    def ensure_parent(
            self,
            doc: MutableMapping[str, Any],
            ptr: str,
            *,
            create: bool = False,
    ):
        """Return (container, leaf_key) for ptr, optionally creating intermediate nodes."""
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
