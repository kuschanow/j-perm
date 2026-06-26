"""Text syntax for j-perm — a readable surface that compiles to op-dicts.

Public surface:

* :func:`parse_text` — parse a source string into a list of op-dicts.
* :func:`register_text_stage` — register :class:`ParseTextStage` on an engine's
  main pipeline (used by the ``text_syntax`` flag of the engine builders).

The runtime depends only on the committed standalone modules in
``_generated/`` (no ``langforge`` import) plus the hand-written
:mod:`~j_perm.text.transform`.  Regenerate ``_generated`` with
``scripts/regen_grammar.py`` after changing the grammar.
"""
from __future__ import annotations

from typing import Any, List

from . import transform as _t
from ._generated import lexer as _lexer
from ._generated import parser as _parser

__all__ = ["parse_text", "register_text_stage", "ParseTextStage"]


def parse_text(src: str) -> List[Any]:
    """Parse *src* (one or more statements) into a list of op-dicts."""
    if not src.endswith("\n"):
        src += "\n"
    tokens = [t for t in _lexer.Lexer().tokenize(src) if t.type != "EOF"]
    tree = _parser.parse(tokens)
    return _t.transform(tree)


def register_text_stage(engine, *, priority: int = 1000) -> None:
    """Register :class:`ParseTextStage` on *engine*'s main pipeline.

    Idempotent-ish: callers (the engine builders) invoke this once when
    ``text_syntax=True``.  Uses the existing ``StageRegistry`` — no new engine API.
    """
    from ..core import StageNode
    from .stage import ParseTextStage, TextStageMatcher

    engine.main_pipeline.stages.register(StageNode(
        name="parse_text",
        priority=priority,
        matcher=TextStageMatcher(),
        processor=ParseTextStage(),
    ))


# Re-export for convenience / typing.
from .stage import ParseTextStage  # noqa: E402  (after parse_text to avoid cycle)
