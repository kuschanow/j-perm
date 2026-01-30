from dataclasses import dataclass, field
from typing import Mapping, Any, Callable

OpHandler = Callable[[dict, Any, Any, "ActionEngine"], Any]


class OpRegistry:
    _ops: dict[str, OpHandler] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[OpHandler], OpHandler]:
        def deco(fn: OpHandler) -> OpHandler:
            if name in cls._ops:
                raise ValueError(f"op already registered: {name!r}")
            cls._ops[name] = fn
            return fn

        return deco

    @classmethod
    def get(cls, name: str) -> OpHandler:
        try:
            return cls._ops[name]
        except KeyError:
            raise ValueError(f"Unknown op {name!r}") from None

    @classmethod
    def all(cls) -> dict[str, OpHandler]:
        return dict(cls._ops)


@dataclass(slots=True)
class Handlers:
    """
    If ops is None -> use all registered.
    If ops is list[str] -> use only those names.
    If ops is Mapping[str, handler] -> use that explicit mapping (no registry).
    """
    ops: list[str] | Mapping[str, OpHandler] | None = None
    on_conflict: str = "error"  # "error" | "override"
    _ops: dict[str, OpHandler] = field(init=False, repr=False)

    def get_handler(self, name: str) -> OpHandler:
        return self._ops[name]

    def __post_init__(self) -> None:
        if self.ops is None:
            ops_map = OpRegistry.all()
        elif isinstance(self.ops, Mapping):
            ops_map = dict(self.ops)
        else:
            all_ops = OpRegistry.all()
            ops_map = {}
            for n in self.ops:
                ops_map[n] = all_ops[n]

        self._ops = ops_map
