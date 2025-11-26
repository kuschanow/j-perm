from __future__ import annotations

from typing import Any, Callable, Dict, MutableMapping, Mapping, TypeAlias

Handler: TypeAlias = Callable[
    [dict, MutableMapping[str, Any], Mapping[str, Any]],
    MutableMapping[str, Any],
]

_OP_HANDLERS: Dict[str, Handler] = {}


def register_op(name: str) -> Callable[[Handler], Handler]:
    """Decorator that registers a DSL operation handler under a given name."""

    def decorator(func: Handler) -> Handler:
        if name in _OP_HANDLERS:
            raise ValueError(f"Handler for op '{name}' is already registered")
        _OP_HANDLERS[name] = func
        return func

    return decorator


def get_handler(name: str) -> Handler:
    """Return a registered handler or raise ValueError if it does not exist."""
    try:
        return _OP_HANDLERS[name]
    except KeyError:
        raise ValueError(f"Unknown op '{name}'") from None
