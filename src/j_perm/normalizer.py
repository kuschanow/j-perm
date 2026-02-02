from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, List

ExpandResult = Optional[List[dict]]
Rule = Callable[[str, Any], ExpandResult]


class ShorthandRegistry:
    _rules: dict[str, Rule] = {}
    _default_priority: dict[str, int] = {}  # name -> order (10, 5, 0, -5, -10...)

    @classmethod
    def register(cls, name: str, *, priority: int = 0):
        def deco(fn: Rule) -> Rule:
            if name in cls._rules:
                raise ValueError(f"shorthand rule already registered: {name!r}")
            cls._rules[name] = fn
            cls._default_priority[name] = priority
            return fn

        return deco

    @classmethod
    def ordered(
            cls,
            rules: Mapping[str, Rule] | None = None,
            *,
            priority: Mapping[str, int] | None = None,
    ) -> list[Rule]:
        rules = dict(cls._rules if rules is None else rules)

        _priority = dict(cls._default_priority)
        if priority:
            _priority.update(priority)

        names = sorted(
            rules.keys(),
            key=lambda n: (_priority.get(n, 0), n),
            reverse=True,
        )
        return [rules[n] for n in names]

    @classmethod
    def get(cls, name: str) -> Rule:
        try:
            return cls._rules[name]
        except KeyError:
            raise ValueError(f"Unknown rule {name!r}") from None

    @classmethod
    def all(cls) -> dict[str, Rule]:
        return dict(cls._rules)


@dataclass(slots=True)
class Normalizer:
    rules: list[str] | Mapping[str, Rule] | None = None
    priority: Mapping[str, int] | None = None

    _rules: list[Rule] = field(init=False)

    def __post_init__(self) -> None:
        if self.rules is None:
            rules = ShorthandRegistry.all()
        elif isinstance(self.rules, Mapping):
            rules = dict(self.rules)
        else:
            all_rules = ShorthandRegistry.all()
            rules = {}
            for n in self.rules:
                if n not in all_rules:
                    raise ValueError(f"Unknown rule {n!r}")
                rules[n] = all_rules[n]

        self._rules = ShorthandRegistry.ordered(rules, priority=self.priority)

    def expand_shorthand(self, obj: Mapping[str, Any]) -> List[dict]:
        steps: List[dict] = []
        for k, v in obj.items():
            for rule in self._rules:
                out = rule(k, v)
                if out is not None:
                    steps.extend(out)
                    break
            else:
                raise ValueError(f"Unhandled shorthand key: {k!r}")
        return steps
