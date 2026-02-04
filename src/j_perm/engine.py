from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, List, Mapping, TypeAlias, MutableMapping, Union

from .normalizer import Normalizer
from .op_handler import Handlers
from .pointers import PointerManager
from .special_resolver import SpecialResolver
from .subst import TemplateSubstitutor

JsonLikeMapping: TypeAlias = MutableMapping[str, Any]
JsonLikeSource: TypeAlias = Union[Mapping[str, Any], List[Any]]
JsonLikeDest: TypeAlias = Union[MutableMapping[str, Any], List[Any]]


# =============================================================================
# Small utilities
# =============================================================================

def tuples_to_lists(obj: Any) -> Any:
    """Recursively convert all tuples into lists so JMESPath indexers work reliably."""
    if isinstance(obj, tuple):
        return [tuples_to_lists(x) for x in obj]

    if isinstance(obj, list):
        return [tuples_to_lists(x) for x in obj]

    if isinstance(obj, Mapping):
        return {k: tuples_to_lists(v) for k, v in obj.items()}

    return obj


@dataclass(slots=True)
class ActionEngine:
    """
    Executes a DSL script against dest with a given source context.

    Configuration:
      - handlers: a Handlers object (has get_handler)
      - special: a SpecialResolver (resolves $ref/$eval/... inside the actions spec, if you want)
      - substitutor: a TemplateSubstitutor (optional: expand ${...} in action specs)
      - normalizer: a Normalizer (for shorthand expansion)
    """

    handlers: Handlers = field(default_factory=Handlers)
    special: SpecialResolver = field(default_factory=SpecialResolver)
    substitutor: TemplateSubstitutor = field(default_factory=TemplateSubstitutor)
    normalizer: Normalizer = field(default_factory=Normalizer)
    pointer_manager: PointerManager = field(default_factory=PointerManager)

    max_depth: int = 50

    def apply_actions(
            self,
            actions: Any,
            *,
            dest: JsonLikeDest,
            source: JsonLikeSource,
    ) -> Any:
        """External API: apply a DSL script."""
        # Work on copies to keep the function referentially safe.
        result = copy.deepcopy(dest)

        # Normalize actions into a flat list of step dicts.
        steps = self.normalize_actions(actions)

        # Convert tuples in source to lists.
        source_norm = tuples_to_lists(source)

        try:
            for step in steps:
                op = step.get("op")
                if not op:
                    raise ValueError(f"Invalid step without 'op': {step!r}")

                handler = self.handlers.get_handler(op)
                result = handler(step, result, source_norm, self)
        except ValueError:
            raise
        except Exception:
            raise

        return copy.deepcopy(result)

    # -------------------------------------------------------------------------
    # Normalization (ported from your original code)
    # -------------------------------------------------------------------------

    def normalize_actions(self, spec: Any) -> List[dict]:
        """Normalize DSL script into a flat list of step dicts."""
        if isinstance(spec, list):
            out: List[dict] = []
            for item in spec:
                if isinstance(item, Mapping) and "op" not in item:
                    out.extend(self.normalizer.expand_shorthand(item))
                else:
                    out.append(item)
            return out

        if isinstance(spec, Mapping):
            return self.normalizer.expand_shorthand(spec)

        raise TypeError("spec must be dict or list")


# Create a default engine instance for convenience
default_engine = ActionEngine()


def apply_actions(
        actions: Any,
        *,
        dest: Dest,
        source: JsonLikeSource,
) -> Any:
    """Convenience function: apply a DSL script using the default engine."""
    return default_engine.apply_actions(actions, dest=dest, source=source)
