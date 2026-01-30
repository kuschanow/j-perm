from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, List, Mapping, TypeAlias, MutableMapping, Union

from .op_handler import Handlers
from .special_resolver import SpecialResolver
from .subst import TemplateSubstitutor

JsonLikeMapping: TypeAlias = MutableMapping[str, Any]
JsonLikeSource: TypeAlias = Union[Mapping[str, Any], List[Any]]
JsonLikeDest: TypeAlias = Union[MutableMapping[str, Any], List[Any]]


# =============================================================================
# Small utilities
# =============================================================================

def _is_pointer_string(v: Any) -> bool:
    return isinstance(v, str) and v.startswith("/")


def _to_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else [x]


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
      - resolve_special_in_actions: enable resolving special constructs inside the actions spec
      - substitute_templates_in_actions: enable template expansion inside the actions spec

    Notes on order:
      1) Optionally substitute templates in 'actions' using 'source' as context
      2) Optionally resolve special constructs in 'actions' using 'source' as context
      3) Normalize to flat step list
      4) Execute handlers
    """

    handlers: Handlers = field(default_factory=Handlers)
    special: SpecialResolver = field(default_factory=SpecialResolver)
    substitutor: TemplateSubstitutor = field(default_factory=TemplateSubstitutor)

    resolve_special_in_actions: bool = True
    substitute_templates_in_actions: bool = True

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
                    out.extend(self._expand_shorthand(item))
                else:
                    out.append(item)
            return out

        if isinstance(spec, Mapping):
            return self._expand_shorthand(spec)

        raise TypeError("spec must be dict or list")

    @staticmethod
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

            append = isinstance(key, str) and key.endswith("[]")
            dst = f"{key[:-2]}/-" if append else key

            if _is_pointer_string(val):
                steps.append({"op": "copy", "from": val, "path": dst, "ignore_missing": True})
            else:
                steps.append({"op": "set", "path": dst, "value": val})

        return steps


# Create a default engine instance for convenience
default_engine = ActionEngine()


def apply_actions(
        actions: Any,
        *,
        dest: JsonLikeDest,
        source: JsonLikeSource,
) -> Any:
    """Convenience function: apply a DSL script using the default engine."""
    return default_engine.apply_actions(actions, dest=dest, source=source)
