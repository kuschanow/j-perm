"""Template substitution — everything that touches ``${…}`` syntax.

Self-contained implementation with built-in casters and JMESPath extensions.

Exports
-------
TemplMatcher
    The ActionMatcher that fires on strings with unescaped placeholders.

TemplSubstHandler
    The ActionHandler that performs the actual ``${…}`` expansion.

template_unescape
    Post-stabilisation unescape function.  Registered as an ``UnescapeRule``
    by the factory; converts ``$${`` → ``${`` and ``$$`` → ``$`` throughout
    an arbitrary value tree.

Built-in casters
----------------
* ``int``   – cast to integer
* ``float`` – cast to float
* ``bool``  – cast to boolean (``int(x)`` then ``bool``)
* ``str``   – cast to string

Built-in JMESPath functions
---------------------------
* ``add(a, b)`` – arithmetic addition
* ``subtract(a, b)`` – arithmetic subtraction

JMESPath data structure
-----------------------
JMESPath expressions (``${?...}``) operate on a data structure with explicit
namespaces to prevent key conflicts:

* ``source.*`` – access source document fields
* ``dest.*`` – access destination document fields

Example: ``${?add(dest.total, source.order.total)}``
"""

from __future__ import annotations

import json
from typing import Any, Callable, Mapping

import jmespath
from jmespath import functions as _jp_funcs

from ..core import ActionHandler, ActionMatcher, ExecutionContext

# ─────────────────────────────────────────────────────────────────────────────
# Built-in casters
# ─────────────────────────────────────────────────────────────────────────────

_BUILTIN_CASTERS: dict[str, Callable[[Any], Any]] = {
    "int": lambda x: int(x),
    "float": lambda x: float(x),
    "bool": lambda x: bool(int(x)) if isinstance(x, (int, str)) else bool(x),
    "str": lambda x: str(x),
}


# ─────────────────────────────────────────────────────────────────────────────
# Built-in JMESPath functions
# ─────────────────────────────────────────────────────────────────────────────

class _BuiltinJMESFunctions(_jp_funcs.Functions):
    """Custom JMESPath functions for template expressions."""

    @_jp_funcs.signature({"types": ["number"]}, {"types": ["number"]})
    def _func_add(self, a: float, b: float) -> float:
        """Arithmetic addition: ``add(a, b)`` → ``a + b``."""
        return a + b

    @_jp_funcs.signature({"types": ["number"]}, {"types": ["number"]})
    def _func_subtract(self, a: float, b: float) -> float:
        """Arithmetic subtraction: ``subtract(a, b)`` → ``a - b``."""
        return a - b


_BUILTIN_JMES_OPTIONS = jmespath.Options(custom_functions=_BuiltinJMESFunctions())


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────


def _has_unescaped_placeholder(s: str) -> bool:
    """Return True if *s* contains at least one ``${…}`` that is **not**
    preceded by a ``$`` escape character.
    """
    i = 0
    while True:
        j = s.find("${", i)
        if j == -1:
            return False
        if j > 0 and s[j - 1] == "$":
            i = j + 2
            continue
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Matcher
# ─────────────────────────────────────────────────────────────────────────────


class TemplMatcher(ActionMatcher):
    """Match strings that contain at least one unescaped ``${…}`` placeholder."""

    def matches(self, step: Any) -> bool:
        return isinstance(step, str) and _has_unescaped_placeholder(step)


# ─────────────────────────────────────────────────────────────────────────────
# Handler
# ─────────────────────────────────────────────────────────────────────────────


class TemplSubstHandler(ActionHandler):
    """Expand ``${…}`` placeholders in a single string.

    Expression dispatch order (inside ``${…}``):

    1. **Caster**        ``${int:…}``      – resolve inner, apply type cast.
    2. **JMESPath**      ``${? expr}``     – query against ``ctx.source``.
    3. **Nested**        ``${${…}}``       – inner placeholder first.
    4. **JSON Pointer**  ``${/path}``      – fallback read from source.

    ``$${…}`` is preserved verbatim during substitution; the corresponding
    ``$${`` → ``${`` unescape is handled by ``template_unescape`` *after*
    the stabilisation loop.

    Configuration
    -------------
    casters
        ``None``    → built-in casters (int, float, bool, str).
        ``Mapping`` → explicit ``{name: callable}`` (replaces built-ins).

    jmes_options
        Custom ``jmespath.Options`` instance. If ``None``, uses built-in
        subtract function.
    """

    def __init__(
            self,
            *,
            casters: Mapping[str, Callable] | None = None,
            jmes_options: jmespath.Options | None = None,
    ) -> None:
        self._casters = dict(casters) if casters else _BUILTIN_CASTERS
        self._jp_options = jmes_options if jmes_options else _BUILTIN_JMES_OPTIONS

    # -- public -------------------------------------------------------------

    def execute(self, step: Any, ctx: ExecutionContext) -> Any:
        if not isinstance(step, str):
            return step

        # If entire string is one template expression, return native type
        if self._is_single_expression(step):
            expr = step[2:-1]  # Strip ${ and }
            return self._resolve_expr(expr, ctx)

        return self._flat_substitute(step, ctx)

    # -- internal -----------------------------------------------------------

    def _is_single_expression(self, s: str) -> bool:
        """Check if string is exactly '${...}' with no surrounding text."""
        if not (s.startswith("${") and s.endswith("}")):
            return False

        # Find the matching closing brace
        depth = 0
        for i in range(2, len(s)):
            if i > 0 and s[i - 1 : i + 1] == "${":
                depth += 1
            elif s[i] == "}":
                if depth == 0:
                    # Found matching close at position i
                    # Check if it's the last character
                    return i == len(s) - 1
                else:
                    depth -= 1

        return False

    def _flat_substitute(self, tmpl: str, ctx: ExecutionContext) -> Any:
        """Single-pass expansion with brace-depth tracking.

        Always returns a *string*.  Type coercion is the caller's job.
        """
        out: list[str] = []
        i = 0

        while i < len(tmpl):
            if tmpl[i:i + 3] == "$${":  # escaped $${  – keep literal
                out.append("$${")
                i += 3
                continue
            if tmpl[i:i + 2] == "$$":  # escaped $$   – keep literal
                out.append("$$")
                i += 2
                continue

            if tmpl[i:i + 2] == "${":
                depth = 0
                j = i + 2

                while j < len(tmpl):
                    ch = tmpl[j]
                    if ch == "{" and tmpl[j - 1] == "$":
                        depth += 1
                    elif ch == "}":
                        if depth == 0:
                            expr = tmpl[i + 2:j]
                            val = self._resolve_expr(expr, ctx)

                            if isinstance(val, (Mapping, list)):
                                rendered = json.dumps(val, ensure_ascii=False)
                            else:
                                rendered = str(val)

                            out.append(rendered)
                            i = j + 1
                            break
                        depth -= 1
                    j += 1
                else:
                    # unclosed brace – emit ``$`` as literal, retry from ``{``
                    out.append(tmpl[i])
                    i += 1
            else:
                out.append(tmpl[i])
                i += 1

        return "".join(out)

    def _resolve_expr(self, expr: str, ctx: ExecutionContext) -> Any:
        """Dispatch a single extracted expression."""
        expr = expr.strip()
        # Build JMESPath data with explicit source/dest namespaces
        # In value pipeline, dest is the current value, real dest is in metadata
        real_dest = ctx.metadata.get('_real_dest', ctx.dest)
        data = {"source": ctx.source, "dest": real_dest, "metadata": ctx.metadata}

        # 1) Casters
        for prefix, fn in self._casters.items():
            tag = f"{prefix}:"
            if expr.startswith(tag):
                inner = expr[len(tag):]
                # Recursively resolve the inner expression (may be pointer, template, etc.)
                value = self._resolve_expr(inner, ctx)
                return fn(value)

        # 2) JMESPath
        if expr.startswith("?"):
            query_raw = expr[1:].lstrip()
            query_expanded = self._flat_substitute(query_raw, ctx)
            return jmespath.search(query_expanded, data, options=self._jp_options)

        # 3) Nested template
        if _has_unescaped_placeholder(expr):
            return self._flat_substitute(expr, ctx)

        # 4) JSON Pointer (with prefix support: @:/, _:/, or regular /)
        # Processor handles prefix resolution automatically
        pointer = expr if expr.startswith(("@:", "_:")) else ("/" + expr.lstrip("/"))
        try:
            return ctx.engine.processor.get(pointer, ctx)
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# Unescape function (registered as UnescapeRule by factory)
# ─────────────────────────────────────────────────────────────────────────────


def template_unescape(obj: Any) -> Any:
    """Recursively strip the template escape layer: ``$${`` → ``${``, ``$$`` → ``$``.

    Walks lists, tuples, and mappings (keys included).  All other types pass
    through unchanged.

    This is the counterpart of the ``$${`` / ``$$`` literals that
    ``TemplSubstHandler`` preserves during substitution.
    """
    if isinstance(obj, str):
        return obj.replace("$${", "${").replace("$$", "$")
    if isinstance(obj, list):
        return [template_unescape(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(template_unescape(x) for x in obj)
    if isinstance(obj, Mapping):
        return {
            template_unescape(k) if isinstance(k, str) else k: template_unescape(v)
            for k, v in obj.items()
        }
    return obj
