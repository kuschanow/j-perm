from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

import jmespath
from jmespath import functions as _jp_funcs

# =============================================================================
# Types
# =============================================================================

Caster = Callable[[Any], Any]
JpMethod = Callable[..., Any]  # method-like callable: (self, ...) -> Any


# =============================================================================
# Caster registry (register casters one-by-one)
# =============================================================================

class CasterRegistry:
    """
    Registry for value casters used in template expressions like:

        ${int:/path}
        ${json:${/raw}}

    Casters are registered by name (prefix before ':').
    """

    _casters: dict[str, Caster] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[Caster], Caster]:
        """
        Decorator to register a caster.

        Example:
            @CasterRegistry.register("int")
            def cast_int(x): ...
        """

        def decorator(fn: Caster) -> Caster:
            if name in cls._casters:
                raise ValueError(f"caster already registered: {name!r}")
            cls._casters[name] = fn
            return fn

        return decorator

    @classmethod
    def all(cls) -> dict[str, Caster]:
        """Return a copy of all registered casters."""
        return dict(cls._casters)


# =============================================================================
# JMESPath function registry (register functions one-by-one)
# =============================================================================

class JpFuncRegistry:
    """
    Registry for JMESPath custom functions registered one-by-one.

    JMESPath discovers methods named `_func_<name>` on a
    `jmespath.functions.Functions` instance. We dynamically build such a class
    from registered method callables.

    IMPORTANT:
      If you rely on 'None => all registered', you must ensure all modules that
      call JpFuncRegistry.register(...) are imported at startup.
    """

    _methods: dict[str, JpMethod] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[JpMethod], JpMethod]:
        """
        Decorator to register a JMESPath function under public name `name`.

        The decorated function:
          - MUST be method-like: (self, ...)
          - SHOULD be decorated with @_jp_funcs.signature(...)
        """

        def decorator(fn: JpMethod) -> JpMethod:
            if name in cls._methods:
                raise ValueError(f"JMESPath function already registered: {name!r}")
            cls._methods[name] = fn
            return fn

        return decorator

    @classmethod
    def all(cls) -> dict[str, JpMethod]:
        """Return a copy of all registered functions."""
        return dict(cls._methods)

    @classmethod
    def build(cls, funcs: list[str] | Mapping[str, JpMethod] | None) -> _jp_funcs.Functions:
        """
        Build a single `jmespath.functions.Functions` instance.

        funcs:
          - None -> use ALL registered
          - list[str] -> use only those names
          - Mapping[str, fn] -> explicit mapping, bypass registry
        """
        if funcs is None:
            methods = cls._methods
        elif isinstance(funcs, Mapping):
            methods = dict(funcs)
        else:
            methods = {name: cls._methods[name] for name in funcs}

        namespace = {f"_func_{name}": fn for name, fn in methods.items()}
        Combined = type("UserJMESFunctions", (_jp_funcs.Functions,), namespace)
        return Combined()


def build_jmespath_options(
        *,
        funcs: list[str] | Mapping[str, JpMethod] | None = None,
        **kwargs: Any,
) -> jmespath.Options:
    """
    Convenience helper to build jmespath.Options(custom_functions=...).

    funcs:
      - None -> all registered
      - list[str] -> subset
      - Mapping[str, fn] -> explicit mapping
    """
    user_funcs = JpFuncRegistry.build(funcs)
    return jmespath.Options(custom_functions=user_funcs, **kwargs)


# =============================================================================
# TemplateSubstitutor
# =============================================================================

@dataclass(slots=True)
class TemplateSubstitutor:
    """
    Template interpolator with:
      - caster prefixes: ${int:/path}
      - JMESPath: ${? expr}
      - nested templates
      - JSON Pointer fallback

    Configuration:
      - casters:
          * None -> use all registered casters
          * list[str] -> use only selected casters
          * Mapping[str, Caster] -> explicit mapping
      - jmes_funcs:
          * None -> use all registered JMES funcs
          * list[str] -> subset
          * Mapping[str, JpMethod] -> explicit mapping
    """

    casters: list[str] | Mapping[str, Caster] | None = None
    jmes_funcs: list[str] | Mapping[str, JpMethod] | None = None

    _casters: dict[str, Caster] = field(init=False)
    _jp_options: jmespath.Options = field(init=False)

    def __post_init__(self) -> None:
        # Build casters map
        if self.casters is None:
            self._casters = CasterRegistry.all()
        elif isinstance(self.casters, Mapping):
            self._casters = dict(self.casters)
        else:
            all_c = CasterRegistry.all()
            self._casters = {k: all_c[k] for k in self.casters}

        # Build JMESPath options
        self._jp_options = build_jmespath_options(funcs=self.jmes_funcs)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def substitute(self, obj: Any, data: Mapping[str, Any], engine: "ActionEngine") -> Any:
        res = self.deep_substitute(obj, data, engine)
        return self._final_unescape(res)

    def flat_substitute(self, tmpl: str, data: Mapping[str, Any], engine: "ActionEngine") -> Any:
        if not self._has_unescaped_placeholder(tmpl):
            return tmpl

        out: list[str] = []
        i = 0

        while i < len(tmpl):
            # literal "${"
            if tmpl[i:i + 3] == "$${":
                out.append("$${")
                i += 3
                continue

            # literal "$"
            if tmpl[i:i + 2] == "$$":
                out.append("$$")
                i += 2
                continue

            if tmpl[i: i + 2] == "${":
                depth = 0
                j = i + 2

                while j < len(tmpl):
                    ch = tmpl[j]

                    if ch == "{" and tmpl[j - 1] == "$":
                        depth += 1
                    elif ch == "}":
                        if depth == 0:
                            expr = tmpl[i + 2: j]
                            val = self._resolve_expr(expr, data, engine)

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
                    out.append(tmpl[i])
                    i += 1
            else:
                out.append(tmpl[i])
                i += 1

        return "".join(out)

    def deep_substitute(self, obj: Any, data: Mapping[str, Any], engine: "ActionEngine", _depth: int = 0) -> Any:
        if _depth > engine.max_depth:
            raise RecursionError("too deep interpolation")

        if isinstance(obj, str):
            out = self.flat_substitute(obj, data, engine)
            if isinstance(out, str) and self._has_unescaped_placeholder(out):
                return self.deep_substitute(out, data, engine, _depth + 1)
            return out

        if isinstance(obj, list):
            return [self.deep_substitute(item, data, engine, _depth) for item in obj]

        if isinstance(obj, tuple):
            return [self.deep_substitute(item, data, engine, _depth) for item in obj]

        if isinstance(obj, Mapping):
            out: dict[Any, Any] = {}
            for k, v in obj.items():
                new_key = self.deep_substitute(k, data, engine, _depth) if isinstance(k, str) else k
                if new_key in out:
                    raise KeyError(f"duplicate key after substitution: {new_key!r}")
                out[new_key] = self.deep_substitute(v, data, engine, _depth)
            return out

        return obj

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _final_unescape(self, obj: Any) -> Any:
        if isinstance(obj, str):
            return obj.replace("$${", "${").replace("$$", "$")
        if isinstance(obj, list):
            return [self._final_unescape(x) for x in obj]
        if isinstance(obj, tuple):
            return tuple(self._final_unescape(x) for x in obj)
        if isinstance(obj, Mapping):
            return {self._final_unescape(k) if isinstance(k, str) else k: self._final_unescape(v)
                    for k, v in obj.items()}
        return obj

    @staticmethod
    def _has_unescaped_placeholder(s: str) -> bool:
        i = 0
        while True:
            j = s.find("${", i)
            if j == -1:
                return False
            if j > 0 and s[j - 1] == "$":
                i = j + 2
                continue
            return True

        return False

    def _resolve_expr(self, expr: str, data: Mapping[str, Any], engine: "ActionEngine") -> Any:
        expr = expr.strip()

        # 1) Casters
        for prefix, fn in self._casters.items():
            tag = f"{prefix}:"
            if expr.startswith(tag):
                inner = expr[len(tag):]
                value = self.flat_substitute(inner, data, engine)

                if isinstance(value, str) and self._has_unescaped_placeholder(value):
                    value = self.flat_substitute(value, data, engine)

                return fn(value)

        # 2) JMESPath
        if expr.startswith("?"):
            query_raw = expr[1:].lstrip()
            query_expanded = self.flat_substitute(query_raw, data, engine)
            return jmespath.search(query_expanded, data, options=self._jp_options)

        # 3) Nested template
        if self._has_unescaped_placeholder(expr):
            return self.flat_substitute(expr, data, engine)

        # 4) JSON Pointer fallback
        pointer = "/" + expr.lstrip("/")
        try:
            return engine.pointer_manager.maybe_slice(pointer, data)
        except Exception:
            return None
