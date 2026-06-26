"""Text-syntax stages for ``sql{ … }`` / ``sql_write{ … }`` blocks.

Each is an ordinary :class:`~j_perm.core.StageProcessor` registered (by
``install_sql`` / ``install_sql_write``) on the engine's existing
``main_pipeline.stages`` — no new engine API.  It runs at a higher priority than
the core text stage, claims string steps shaped like ``[/dest =] <tag>{ … }``,
parses the inner SQL with this plugin's own standalone parser, and emits an
``{op: "sql"|"sql_write", query: …, to?: …}`` op-dict.  Non-matching steps pass
through untouched, so SQL, core text and raw JSON mix freely in one list.
"""
from __future__ import annotations

import re
from typing import Any, List

from j_perm.core import ExecutionContext, StageMatcher, StageProcessor

from . import transform as _t
from ._generated import lexer as _lexer
from ._generated import parser as _parser

_POINTER = r"(?:[@&!_]:)?/[^\s=]*"


def _header_re(tag: str) -> "re.Pattern[str]":
    return re.compile(r"^\s*(?:(" + _POINTER + r")\s*=\s*)?" + tag + r"\s*\{")


def parse_sql(text: str) -> Any:
    """Parse inner SQL text into a j_perm_sql query construct (op-dict)."""
    tokens = [t for t in _lexer.Lexer().tokenize(text) if t.type != "EOF"]
    return _t.transform(_parser.parse(tokens))


def _extract(s: str, header: "re.Pattern[str]", tag: str):
    """Return ``(dest_or_None, inner_sql)`` for a ``[/dest =] tag{ … }`` string."""
    m = header.match(s)
    open_idx = s.index("{", m.start())
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                rest = s[i + 1:].strip()
                if rest:
                    raise ValueError(
                        f"unexpected text after {tag}{{...}} block: {rest!r}; "
                        f"put separate statements in separate list elements"
                    )
                return m.group(1), s[open_idx + 1:i]
    raise ValueError(f"unbalanced braces in {tag}{{ … }} block")


class SqlBlockMatcher(StageMatcher):
    context_aware = False

    def __init__(self, header: "re.Pattern[str]") -> None:
        self._header = header

    def matches(self, steps: List[Any], ctx: ExecutionContext) -> bool:
        return any(isinstance(s, str) and self._header.match(s) for s in steps)


class SqlBlockStage(StageProcessor):
    context_aware = False

    def __init__(self, tag: str, op: str, *, read_only: bool) -> None:
        self.tag = tag
        self.op = op
        self.read_only = read_only
        self._header = _header_re(tag)

    def apply(self, steps: List[Any], ctx: ExecutionContext) -> List[Any]:
        out: List[Any] = []
        for s in steps:
            if isinstance(s, str) and self._header.match(s):
                dest, inner = _extract(s, self._header, self.tag)
                query = parse_sql(inner)
                if self.read_only and "$select" not in query:
                    raise ValueError(
                        "sql{ … } is read-only; use sql_write{ … } for "
                        "INSERT/UPDATE/DELETE"
                    )
                step: dict = {"op": self.op, "query": query}
                if dest is not None:
                    step["to"] = dest
                out.append(step)
            else:
                out.append(s)
        return out


def register_sql_text_stage(engine, *, tag: str, op: str, read_only: bool,
                            priority: int = 1500) -> None:
    """Register a SQL block stage on *engine*'s main pipeline (existing registry)."""
    from j_perm.core import StageNode

    engine.main_pipeline.stages.register(StageNode(
        name=f"{tag}_text",
        priority=priority,
        matcher=SqlBlockMatcher(_header_re(tag)),
        processor=SqlBlockStage(tag, op, read_only=read_only),
    ))
