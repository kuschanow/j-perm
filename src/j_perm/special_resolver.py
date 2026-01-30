from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, MutableMapping

# =============================================================================
# Types
# =============================================================================

SpecialHandler = Callable[[Mapping[str, Any], Mapping[str, Any], "ActionEngine"], Any]


# =============================================================================
# Special registry (register constructs one-by-one)
# =============================================================================

class SpecialRegistry:
    """
    Registry of special constructs keyed by the marker key in a dict,
    e.g. {"$ref": ...}, {"$eval": ...}.

    Registration is per construct KEY (no "set" wrappers).

    IMPORTANT:
      If you rely on 'None => all registered', you must ensure modules that call
      SpecialRegistry.register(...) are imported somewhere at startup.
    """

    _handlers: MutableMapping[str, SpecialHandler] = {}

    @classmethod
    def register(cls, key: str) -> Callable[[SpecialHandler], SpecialHandler]:
        """
        Decorator to register a special handler under a dict key (e.g. "$ref").

        By default we raise on duplicates to avoid accidental overrides.
        If you want overrides, change the duplicate check logic.
        """

        def decorator(fn: SpecialHandler) -> SpecialHandler:
            if key in cls._handlers:
                raise ValueError(f"special key already registered: {key!r}")
            cls._handlers[key] = fn
            return fn

        return decorator

    @classmethod
    def get(cls, key: str) -> SpecialHandler:
        """Return a registered handler or raise KeyError."""
        return cls._handlers[key]

    @classmethod
    def all(cls) -> dict[str, SpecialHandler]:
        """Return a copy of all registered handlers."""
        return dict(cls._handlers)


# =============================================================================
# SpecialResolver (configurable)
# =============================================================================

_MISSING = object()


@dataclass(slots=True)
class SpecialResolver:
    """
    Walk a value tree and resolve registered special constructs.

    Configuration:
      - substitutor: used for template expansion inside constructs (e.g. "$ref" value)
      - specials:
          * None -> use all registered keys/handlers from SpecialRegistry
          * list[str] -> use only these keys from registry
          * Mapping[str, SpecialHandler] -> explicit mapping, bypass registry
    """

    specials: list[str] | Mapping[str, SpecialHandler] | None = None

    _specials: dict[str, SpecialHandler] = field(init=False)

    def __post_init__(self) -> None:
        # Build the active special-handler map.
        if self.specials is None:
            self._specials = SpecialRegistry.all()
        elif isinstance(self.specials, Mapping):
            self._specials = dict(self.specials)
        else:
            all_sp = SpecialRegistry.all()
            self._specials = {k: all_sp[k] for k in self.specials}

    def resolve(self, val: Any, src: Mapping[str, Any], engine: "ActionEngine") -> Any:
        """
        Resolve special constructs inside an arbitrary value tree.

        Semantics:
          - If a dict contains a registered special key, that handler "takes over"
            and the whole dict is replaced with the handler result.
          - Otherwise we recurse into dict/list/tuple.
        """
        if isinstance(val, dict):
            for key, fn in self._specials.items():
                if key in val:
                    return fn(val, src, engine)

            return {k: self.resolve(v, src, engine) for k, v in val.items()}

        if isinstance(val, list):
            return [self.resolve(x, src, engine) for x in val]

        if isinstance(val, tuple):
            return tuple(self.resolve(x, src, engine) for x in val)

        return val
