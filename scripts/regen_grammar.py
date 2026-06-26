#!/usr/bin/env python
"""Regenerate the standalone j-perm text-syntax lexer/parser from the grammar.

DEV-ONLY tool.  This is the ONLY place that imports ``langforge``.  It reads the
grammar sources under ``src/j_perm/text/grammar/`` and writes self-contained
Python modules to ``src/j_perm/text/_generated/`` that the runtime imports
without any ``langforge`` dependency.

Run after editing ``lexer_config.py`` or ``core.g``::

    python scripts/regen_grammar.py
"""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GRAMMAR = os.path.join(ROOT, "src", "j_perm", "text", "grammar")
GENERATED = os.path.join(ROOT, "src", "j_perm", "text", "_generated")

sys.path.insert(0, GRAMMAR)


def main() -> None:
    from langforge.lexer import Lexer
    from langforge.parser import Parser
    from lexer_config import LEXER_CONFIG

    with open(os.path.join(GRAMMAR, "core.g")) as f:
        grammar_text = f.read()

    os.makedirs(GENERATED, exist_ok=True)

    Lexer(LEXER_CONFIG).save_lexer(os.path.join(GENERATED, "lexer.py"))
    Parser({"grammar": "earley", "grammar_text": grammar_text}).save_parser(
        os.path.join(GENERATED, "parser.py")
    )

    # sanity: the generated code must not reference langforge
    for name in ("lexer.py", "parser.py"):
        with open(os.path.join(GENERATED, name)) as f:
            if "langforge" in f.read():
                raise SystemExit(f"ERROR: generated {name} references langforge")

    print("Regenerated j_perm/text/_generated/{lexer,parser}.py")


if __name__ == "__main__":
    main()
