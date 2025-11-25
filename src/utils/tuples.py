from __future__ import annotations

from typing import Any, Mapping


def tuples_to_lists(obj: Any) -> Any:
    """Recursively convert all tuples into lists so JMESPath indexers work reliably."""
    if isinstance(obj, tuple):
        return [tuples_to_lists(x) for x in obj]

    if isinstance(obj, list):
        return [tuples_to_lists(x) for x in obj]

    if isinstance(obj, Mapping):
        return {k: tuples_to_lists(v) for k, v in obj.items()}

    return obj
