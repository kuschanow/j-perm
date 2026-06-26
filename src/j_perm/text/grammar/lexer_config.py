"""Lexer configuration for the j-perm text syntax (langforge config).

This module is the *source* of the lexer grammar.  It is consumed only by
``scripts/regen_grammar.py`` (a dev-time tool that imports ``langforge`` to
generate the standalone lexer in ``j_perm/text/_generated/lexer.py``).  The
runtime never imports this module or ``langforge`` — it uses the generated code.

Indentation handling
---------------------
Newlines emit ``NEWLINE`` plus Python-style ``INDENT`` / ``DEDENT`` driven by a
``meta.indent_stack`` (seeded lazily to ``[0]``).  ``meta.paren_depth`` tracks
open ``(`` / ``[`` / ``$(`` so newlines are suppressed inside them (implicit line
joining); ``{`` / ``}`` deliberately do *not* suppress, so brace blocks keep
newline-separated statements while single-line dict literals carry no newline.
"""

LEXER_CONFIG = {
    "pattern_defs": {
        "WS": "[ \\t]",
        "DIGIT": "[0-9]",
        "ALPHA": "[a-zA-Z_]",
        "IDENT": "%{ALPHA}(?:%{ALPHA}|%{DIGIT})*",
    },
    "states": {
        "root": {
            "on_eof": {"emit": ["NEWLINE"]},
            "rules": [
                # newline + indentation; suppressed inside ( [ $( via paren_depth
                {
                    "pattern": "\\n(?P<ws>%{WS}*)",
                    "emit": [
                        # lazily initialise indent_stack to [0] at top level
                        {
                            "token": "INIT", "skip": True,
                            "if": {"and": [
                                {"meta.paren_depth": {"falsy": True}},
                                {"meta.indent_stack": {"empty": True}},
                            ]},
                            "update_meta": {"indent_stack": {"push": 0}},
                        },
                        {"token": "NEWLINE", "if": {"meta.paren_depth": {"falsy": True}}},
                        {
                            "token": "DEDENT",
                            "if": {"meta.paren_depth": {"falsy": True}},
                            "repeat": {
                                "while": {"meta.indent_stack": {"top_gt": {"capture_len": "ws"}}},
                                "each": {"update_meta": {"indent_stack": {"pop": True}}},
                            },
                        },
                        {
                            "token": "INDENT",
                            "if": {
                                "and": [
                                    {"meta.paren_depth": {"falsy": True}},
                                    {"meta.indent_stack": {"top_lt": {"capture_len": "ws"}}},
                                ]
                            },
                            "update_meta": {"indent_stack": {"push": {"capture_len": "ws"}}},
                        },
                    ],
                },
                {"pattern": "%{WS}+", "skip": True},
                {"pattern": "#[^\\n]*", "skip": True},  # comment

                # strings (opaque; ${...} interpolation kept literal for runtime)
                {"pattern": "\"(?:[^\"\\\\]|\\\\.)*\"", "emit": "STRING"},

                # $( read-pointer open (balances against ')')
                {"exact": "$(", "emit": "DOLLAR_LPAREN", "update_meta": {"paren_depth": {"inc": 1}}},

                # pointers: optional prefix, '/', then a letter/_ and path chars
                {"pattern": "(?:[@&!_]:)?/%{ALPHA}[A-Za-z0-9_./~-]*", "emit": "POINTER"},
                {"pattern": "(?:[@&!_]:)?/", "emit": "POINTER"},

                # numbers
                {"pattern": "%{DIGIT}+\\.%{DIGIT}+", "emit": "FLOAT"},
                {"pattern": "%{DIGIT}+", "emit": "INT"},

                # multi-char operators (before single-char)
                {"exact": "<-!", "emit": "COPY_STRICT"},
                {"exact": "<-", "emit": "COPY"},
                {"exact": "==", "emit": "EQ"},
                {"exact": "!=", "emit": "NE"},
                {"exact": "<=", "emit": "LE"},
                {"exact": ">=", "emit": "GE"},
                {"exact": "**", "emit": "POW"},
                {"exact": "??", "emit": "COALESCE"},

                # keywords (hard reserved words).  Soft words used both as syntax
                # and as identifiers/arg-names (default, context, on_failure,
                # merge, select) are NOT reserved — they stay IDENT and the grammar
                # matches them by value via inline literals.
                {"keywords": [
                    "foreach", "in", "while", "do", "if", "elif", "else",
                    "try", "except", "finally", "def", "return", "raise",
                    "break", "continue", "exec", "and", "or", "not", "exists",
                    "true", "false", "null", "del", "assert", "op", "raw",
                ], "emit": "KW"},

                # identifiers
                {"pattern": "%{IDENT}", "emit": "IDENT"},

                # single-char punctuation / operators
                {"exact": "(", "emit": "LPAREN", "update_meta": {"paren_depth": {"inc": 1}}},
                {"exact": ")", "emit": "RPAREN", "update_meta": {"paren_depth": {"dec": 1}}},
                {"exact": "[", "emit": "LBRACKET", "update_meta": {"paren_depth": {"inc": 1}}},
                {"exact": "]", "emit": "RBRACKET", "update_meta": {"paren_depth": {"dec": 1}}},
                {"exact": "{", "emit": "LBRACE"},
                {"exact": "}", "emit": "RBRACE"},
                {"exact": "<", "emit": "LT"},
                {"exact": ">", "emit": "GT"},
                {"exact": "+", "emit": "PLUS"},
                {"exact": "-", "emit": "MINUS"},
                {"exact": "*", "emit": "STAR"},
                {"exact": "/", "emit": "SLASH"},
                {"exact": "%", "emit": "PERCENT"},
                {"exact": "=", "emit": "ASSIGN"},
                {"exact": ",", "emit": "COMMA"},
                {"exact": ":", "emit": "COLON"},
                {"exact": ";", "emit": "SEMI"},
                {"exact": ".", "emit": "DOT"},
                {"exact": "!", "emit": "BANG"},
            ],
        }
    },
}
