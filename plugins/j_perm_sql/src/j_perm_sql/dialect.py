"""Dialect / rendering options for the SQL plugin.

Everything that genuinely differs between SQL databases lives here so the
construct handlers can stay dialect-agnostic:

* ``paramstyle``       – how bound parameters appear (PEP 249 styles).
* ``identifier_quote`` – the character used to quote identifiers.
* ``pagination``       – ``"limit"`` (``LIMIT n OFFSET m``) or ``"fetch"``
  (``OFFSET m ROWS FETCH FIRST n ROWS ONLY``).
* ``concat_operator``  – string concatenation operator (``||`` or ``+``).

The construct handlers render with a single neutral placeholder token
(:data:`PLACEHOLDER`, ``"?"``) and :meth:`RenderOptions.finalize` rewrites
those into the configured ``paramstyle`` at the very end.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

#: Neutral placeholder emitted during rendering; rewritten by ``finalize``.
PLACEHOLDER = "?"

_PARAMSTYLES = frozenset({"qmark", "format", "numeric", "named"})
_PAGINATIONS = frozenset({"limit", "fetch"})

# A single, unqualified identifier part.  Conservative on purpose: the strict
# charset both prevents SQL injection through identifiers and guarantees the
# rendered SQL never contains a stray ``?`` (so ``finalize`` can rewrite
# placeholders by a simple left-to-right scan).
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# A SQL type expression for ``CAST`` (e.g. ``VARCHAR(255)``, ``DECIMAL(10, 2)``).
_TYPE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_ ,()]*$")

# A bare function name (rendered as-is, never quoted).
_FUNC_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class RenderOptions:
    """Immutable knobs controlling how SQL is rendered for a target dialect."""

    paramstyle: str = "qmark"
    identifier_quote: str = '"'
    pagination: str = "limit"
    concat_operator: str = "||"

    def __post_init__(self) -> None:
        if self.paramstyle not in _PARAMSTYLES:
            raise ValueError(
                f"paramstyle must be one of {sorted(_PARAMSTYLES)}, got {self.paramstyle!r}"
            )
        if self.pagination not in _PAGINATIONS:
            raise ValueError(
                f"pagination must be one of {sorted(_PAGINATIONS)}, got {self.pagination!r}"
            )
        if len(self.identifier_quote) != 1:
            raise ValueError("identifier_quote must be a single character")

    # -- identifiers --------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        """Validate and quote a single identifier part (e.g. a column name)."""
        if not isinstance(name, str) or not _IDENT_RE.match(name):
            raise ValueError(f"invalid SQL identifier: {name!r}")
        q = self.identifier_quote
        return f"{q}{name}{q}"

    def quote_ref(self, ref: str) -> str:
        """Quote a possibly dotted/star reference (e.g. ``u.id``, ``u.*``, ``*``)."""
        if ref == "*":
            return "*"
        parts = ref.split(".")
        out = [p if p == "*" else self.quote_identifier(p) for p in parts]
        return ".".join(out)

    def validate_type(self, type_str: str) -> str:
        """Validate a CAST target type string and return it unchanged."""
        if not isinstance(type_str, str) or not _TYPE_RE.match(type_str):
            raise ValueError(f"invalid SQL type: {type_str!r}")
        return type_str

    def validate_func_name(self, name: str) -> str:
        """Validate a function name and return it unchanged (never quoted)."""
        if not isinstance(name, str) or not _FUNC_RE.match(name):
            raise ValueError(f"invalid SQL function name: {name!r}")
        return name

    # -- placeholders -------------------------------------------------------

    def finalize(self, sql: str, params: list) -> tuple[str, list | dict]:
        """Rewrite neutral ``?`` placeholders into the configured paramstyle.

        Returns ``(sql, params)`` where *params* is a list for positional
        styles and a dict for ``named``.
        """
        count = sql.count(PLACEHOLDER)
        if count != len(params):
            raise ValueError(
                f"placeholder/param mismatch: {count} placeholders, {len(params)} params"
            )
        if self.paramstyle == "qmark":
            return sql, list(params)
        if self.paramstyle == "format":
            return sql.replace(PLACEHOLDER, "%s"), list(params)

        # numeric / named — rewrite each placeholder positionally
        out: list[str] = []
        idx = 0
        for ch in sql:
            if ch == PLACEHOLDER:
                idx += 1
                out.append(f"${idx}" if self.paramstyle == "numeric" else f":p{idx}")
            else:
                out.append(ch)
        rewritten = "".join(out)
        if self.paramstyle == "numeric":
            return rewritten, list(params)
        return rewritten, {f"p{i + 1}": v for i, v in enumerate(params)}
