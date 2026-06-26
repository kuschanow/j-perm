"""SQL text syntax: ``sql{ … }`` (read) and ``sql_write{ … }`` (DML).

Independent of the core j-perm text parser — this package ships its own
standalone SQL lexer/parser in ``_generated/`` (no ``langforge`` at runtime;
regenerate with ``scripts/regen_sql_grammar.py``).  Wiring lives in
``install_sql`` / ``install_sql_write``.
"""
from __future__ import annotations

from .block import parse_sql, register_sql_text_stage

__all__ = ["parse_sql", "register_sql_text_stage"]
