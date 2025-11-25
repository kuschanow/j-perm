from __future__ import annotations

import copy
from typing import Any, List, Mapping, MutableMapping, Union

from .registry import get_handler
from .utils.tuples import tuples_to_lists


def _is_pointer_string(v: Any) -> bool:
    return isinstance(v, str) and v.startswith("/")


def _to_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else [x]


def _expand_shorthand(obj: Mapping[str, Any]) -> List[dict]:
    """Expand shorthand mapping form into explicit op steps."""
    steps: List[dict] = []

    for key, val in obj.items():
        if key == "~delete":
            for p in _to_list(val):
                steps.append({"op": "delete", "path": p})
            continue

        if key == "~assert":
            if isinstance(val, Mapping):
                for p, eq in val.items():
                    steps.append({"op": "assert", "path": p, "equals": eq})
            else:
                for p in _to_list(val):
                    steps.append({"op": "assert", "path": p})
            continue

        append = key.endswith("[]")
        dst = f"{key[:-2]}/-" if append else key

        if _is_pointer_string(val):
            steps.append({"op": "copy", "from": val, "path": dst, "ignore_missing": True})
        else:
            steps.append({"op": "set", "path": dst, "value": val})

    return steps


def normalize_actions(spec: Any) -> List[dict]:
    """Normalize DSL script into a flat list of step dicts."""
    if isinstance(spec, list):
        out: List[dict] = []
        for item in spec:
            if isinstance(item, Mapping) and "op" not in item:
                out.extend(_expand_shorthand(item))
            else:
                out.append(item)
        return out

    if isinstance(spec, Mapping):
        return _expand_shorthand(spec)

    raise TypeError("spec must be dict or list")


def apply_actions(
        actions: Any,
        *,
        dest: Union[MutableMapping[str, Any], List[Any]],
        source: Union[Mapping[str, Any], List[Any]],
) -> Mapping[str, Any]:
    """Execute a DSL script against dest with a given source context."""
    steps = normalize_actions(actions)
    result = copy.deepcopy(dest)

    source = tuples_to_lists(source)

    try:
        for step in steps:
            handler = get_handler(step["op"])
            result = handler(step, result, source)  # type: ignore[arg-type]
    except ValueError:
        raise
    except Exception:
        raise

    return copy.deepcopy(result)
