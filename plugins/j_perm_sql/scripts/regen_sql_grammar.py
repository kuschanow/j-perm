#!/usr/bin/env python
"""Regenerate the standalone SQL sublanguage lexer/parser (DEV-ONLY).

The ONLY place that imports ``langforge``.  Reads the grammar under
``src/j_perm_sql/text/grammar/`` and writes self-contained modules to
``src/j_perm_sql/text/_generated/`` that the runtime imports without langforge.

    python scripts/regen_sql_grammar.py
"""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GRAMMAR = os.path.join(ROOT, "src", "j_perm_sql", "text", "grammar")
GENERATED = os.path.join(ROOT, "src", "j_perm_sql", "text", "_generated")

sys.path.insert(0, GRAMMAR)


def main() -> None:
    from langforge.lexer import Lexer
    from langforge.parser import Parser
    from sql_lexer_config import SQL_LEXER_CONFIG

    with open(os.path.join(GRAMMAR, "sql.g")) as f:
        grammar_text = f.read()

    os.makedirs(GENERATED, exist_ok=True)
    Lexer(SQL_LEXER_CONFIG).save_lexer(os.path.join(GENERATED, "lexer.py"))
    Parser({"grammar": "earley", "grammar_text": grammar_text}).save_parser(
        os.path.join(GENERATED, "parser.py")
    )
    for name in ("lexer.py", "parser.py"):
        with open(os.path.join(GENERATED, name)) as f:
            if "langforge" in f.read():
                raise SystemExit(f"ERROR: generated {name} references langforge")
    print("Regenerated j_perm_sql/text/_generated/{lexer,parser}.py")


if __name__ == "__main__":
    main()
