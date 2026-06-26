"""SQL sublanguage lexer config (no indentation; whitespace insignificant)."""
SQL_LEXER_CONFIG = {
    "pattern_defs": {
        "WS": "[ \\t\\r\\n]",
        "DIGIT": "[0-9]",
        "ALPHA": "[a-zA-Z_]",
        "IDENT": "%{ALPHA}(?:%{ALPHA}|%{DIGIT})*",
    },
    "states": {
        "root": {
            "on_eof": {"emit": ["EOF"]},
            "rules": [
                {"pattern": "%{WS}+", "skip": True},
                {"pattern": "--[^\\n]*", "skip": True},
                # strings
                {"pattern": "'(?:[^'\\\\]|\\\\.)*'", "emit": "STRING"},
                {"pattern": "\"(?:[^\"\\\\]|\\\\.)*\"", "emit": "STRING"},
                # read-pointer
                {"exact": "$(", "emit": "DOLLAR_LPAREN"},
                # pointers (for $( ... ) content)
                {"pattern": "(?:[@&!_]:)?/%{ALPHA}[A-Za-z0-9_./~-]*", "emit": "POINTER"},
                # numbers
                {"pattern": "%{DIGIT}+\\.%{DIGIT}+", "emit": "FLOAT"},
                {"pattern": "%{DIGIT}+", "emit": "INT"},
                # multi-char operators
                {"exact": "<=", "emit": "LE"},
                {"exact": ">=", "emit": "GE"},
                {"exact": "<>", "emit": "NE"},
                {"exact": "!=", "emit": "NE2"},
                {"exact": "??", "emit": "COALESCE"},
                # keywords (lowercase)
                {"keywords": [
                    "select", "from", "where", "and", "or", "not", "in", "as",
                    "order", "by", "asc", "desc", "limit", "offset", "group",
                    "having", "join", "on", "inner", "left", "right", "full",
                    "cross", "insert", "into", "values", "update", "set",
                    "delete", "null", "true", "false", "all", "distinct",
                ], "emit": "KW"},
                {"pattern": "%{IDENT}", "emit": "IDENT"},
                # single-char
                {"exact": "(", "emit": "LPAREN"},
                {"exact": ")", "emit": "RPAREN"},
                {"exact": "*", "emit": "STAR"},
                {"exact": ",", "emit": "COMMA"},
                {"exact": ".", "emit": "DOT"},
                {"exact": "=", "emit": "EQ"},
                {"exact": "<", "emit": "LT"},
                {"exact": ">", "emit": "GT"},
                {"exact": "+", "emit": "PLUS"},
                {"exact": "-", "emit": "MINUS"},
                {"exact": "/", "emit": "SLASH"},
                {"exact": "%", "emit": "PERCENT"},
            ],
        }
    },
}
