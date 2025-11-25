from __future__ import annotations

import copy
import json
from typing import Any, Mapping, List

import jmespath

from ..jmes_ext import JP_OPTIONS

_CASTERS = {
    "int": int,
    "float": float,
    "str": str,
    "bool": lambda x: bool(int(x)) if isinstance(x, (int, str)) else bool(x),
}


def _resolve_expr(expr: str, data: Mapping[str, Any]) -> Any:
    """Resolve a single ${...} expression body."""
    from .pointers import maybe_slice  # local import to avoid cycles

    expr = expr.strip()

    # 1) simple casters like int:/path
    for prefix, fn in _CASTERS.items():
        tag = f"{prefix}:"
        if expr.startswith(tag):
            inner = expr[len(tag):]
            value = flat_substitute(inner, data)
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                value = flat_substitute(value, data)
            return fn(value)

    # 2) JMESPath expression ? <expr>
    if expr.startswith("?"):
        query_raw = expr[1:].lstrip()
        query_expanded = flat_substitute(query_raw, data)
        return jmespath.search(query_expanded, data, options=JP_OPTIONS)

    # 3) nested template ${...}
    if expr.startswith("${") and expr.endswith("}"):
        return flat_substitute(expr, data)

    # 4) default: treat as JSON Pointer (relative to root)
    pointer = "/" + expr.lstrip("/")
    try:
        return maybe_slice(pointer, data)  # type: ignore[arg-type]
    except Exception:
        return None


def flat_substitute(tmpl: str, data: Mapping[str, Any]) -> Any:
    """One-pass interpolation that replaces all ${...} occurrences in a string."""
    if "${" not in tmpl:
        return tmpl

    # Entire string is a single ${...}
    if tmpl.startswith("${") and tmpl.endswith("}"):
        body = tmpl[2:-1]
        return copy.deepcopy(_resolve_expr(body, data))

    out: List[str] = []
    i = 0
    while i < len(tmpl):
        if tmpl[i:i + 2] == "${":
            depth = 0
            j = i + 2

            while j < len(tmpl):
                ch = tmpl[j]

                if ch == "{" and tmpl[j - 1] == "$":
                    depth += 1
                elif ch == "}":
                    if depth == 0:
                        expr = tmpl[i + 2:j]
                        val = _resolve_expr(expr, data)

                        if isinstance(val, (Mapping, list)):
                            rendered = json.dumps(val, ensure_ascii=False)
                        else:
                            rendered = str(val)

                        out.append(rendered)
                        i = j + 1
                        break
                    depth -= 1

                j += 1
            else:
                # no closing brace found, treat '${' as a literal
                out.append(tmpl[i])
                i += 1
        else:
            out.append(tmpl[i])
            i += 1

    return "".join(out)


def deep_substitute(obj: Any, data: Mapping[str, Any], _depth: int = 0) -> Any:
    """Recursively apply interpolation to strings, mapping keys/values and sequences."""
    if _depth > 50:
        raise RecursionError("too deep interpolation")

    if isinstance(obj, str):
        out = flat_substitute(obj, data)
        if isinstance(out, str) and "${" in out:
            return deep_substitute(out, data, _depth + 1)
        return out

    if isinstance(obj, list):
        return [deep_substitute(item, data, _depth) for item in obj]

    if isinstance(obj, tuple):
        return [deep_substitute(item, data, _depth) for item in obj]

    if isinstance(obj, Mapping):
        out: dict[Any, Any] = {}
        for k, v in obj.items():
            new_key = deep_substitute(k, data, _depth) if isinstance(k, str) else k
            if new_key in out:
                raise KeyError(f"duplicate key after substitution: {new_key!r}")
            out[new_key] = deep_substitute(v, data, _depth)
        return out

    return obj


# Public alias mirroring the original _substitute name
substitute = deep_substitute
substitute.__name__ = "substitute"
