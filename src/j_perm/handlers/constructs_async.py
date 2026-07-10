"""Async twins of the value-pipeline constructs (``handlers/constructs.py``).

Each function mirrors its synchronous counterpart exactly, awaiting
``process_value_async`` when it resolves its operands.  Registered into the
async value pipeline's ``$``-specials dict so that an async construct (or an
async ``$func``) nested inside *any* construct's arguments — ``$add``, ``$eq``,
``$eval`` bodies, etc. — is awaited.

The pure post-resolution computation (comparisons, arithmetic with security
limits, string/regex work) is delegated to the same private helpers used by the
sync constructs, so the non-async logic stays single-sourced and the security
limits cannot drift between the two pipelines.
"""

from __future__ import annotations

import copy
from typing import Any, Mapping, Callable

from ..core import ExecutionContext
from .signals import RawValueSignal
from . import constructs as _c

AsyncSpecialFn = Callable[[Mapping[str, Any], ExecutionContext], Any]

_MISSING = _c._MISSING


# ─────────────────────────────────────────────────────────────────────────────
# $ref / $eval
# ─────────────────────────────────────────────────────────────────────────────

async def ref_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    ptr = await ctx.engine.process_value_async(node["$ref"], ctx, _unescape=False)
    dflt = node.get("$default", _MISSING)
    try:
        return copy.deepcopy(ctx.engine.processor.get(ptr, ctx))
    except Exception:
        if dflt is not _MISSING:
            return await ctx.engine.process_value_async(copy.deepcopy(dflt), ctx)
        raise


async def eval_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    eval_ctx = ctx.copy(new_dest={})
    old_real_dest = eval_ctx.metadata.pop('_real_dest', None)
    try:
        result = await ctx.engine.apply_to_context_async(node["$eval"], eval_ctx)
    finally:
        if old_real_dest is not None:
            eval_ctx.metadata['_real_dest'] = old_real_dest

    if "$select" in node:
        sel_ptr = await ctx.engine.process_value_async(node["$select"], ctx, _unescape=False)
        temp_ctx = ctx.copy(new_source=result)
        return ctx.engine.processor.get(sel_ptr, temp_ctx)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# boolean logic
# ─────────────────────────────────────────────────────────────────────────────

async def and_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    last_result = None
    for action in node["$and"]:
        last_result = await ctx.engine.process_value_async(action, ctx)
        if not last_result:
            return last_result
    return last_result


async def or_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    last_result = None
    for action in node["$or"]:
        last_result = await ctx.engine.process_value_async(action, ctx)
        if last_result:
            return last_result
    return last_result


async def not_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    result = await ctx.engine.process_value_async(node["$not"], ctx)
    return not result


async def if_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    if await ctx.engine.process_value_async(node["$if"], ctx):
        return await ctx.engine.process_value_async(node.get("$then"), ctx)
    return await ctx.engine.process_value_async(node.get("$else"), ctx)


# ─────────────────────────────────────────────────────────────────────────────
# comparisons
# ─────────────────────────────────────────────────────────────────────────────

async def _binary(key: str, op: Callable[[Any, Any], Any],
                  node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    if not isinstance(node[key], list) or len(node[key]) != 2:
        raise ValueError(f"{key} requires a list of exactly 2 values")
    left = await ctx.engine.process_value_async(node[key][0], ctx)
    right = await ctx.engine.process_value_async(node[key][1], ctx)
    return op(left, right)


async def gt_handler(node, ctx):
    return await _binary("$gt", lambda a, b: a > b, node, ctx)


async def gte_handler(node, ctx):
    return await _binary("$gte", lambda a, b: a >= b, node, ctx)


async def lt_handler(node, ctx):
    return await _binary("$lt", lambda a, b: a < b, node, ctx)


async def lte_handler(node, ctx):
    return await _binary("$lte", lambda a, b: a <= b, node, ctx)


async def eq_handler(node, ctx):
    return await _binary("$eq", lambda a, b: a == b, node, ctx)


async def ne_handler(node, ctx):
    return await _binary("$ne", lambda a, b: a != b, node, ctx)


async def in_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    if not isinstance(node["$in"], list) or len(node["$in"]) != 2:
        raise ValueError("$in requires a list of exactly 2 values: [value, container]")
    value = await ctx.engine.process_value_async(node["$in"][0], ctx)
    container = await ctx.engine.process_value_async(node["$in"][1], ctx)
    return value in container


async def exists_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    ptr = await ctx.engine.process_value_async(node["$exists"], ctx, _unescape=False)
    return ctx.engine.processor.exists(ptr, ctx)


# ─────────────────────────────────────────────────────────────────────────────
# arithmetic (security limits reused from the sync compute helpers)
# ─────────────────────────────────────────────────────────────────────────────

def make_add_handler(max_number_result: float = 1e15,
                     max_string_result: int = 100_000_000) -> AsyncSpecialFn:
    async def add_handler(node, ctx):
        if not isinstance(node["$add"], list) or len(node["$add"]) < 1:
            raise ValueError("$add requires a list of at least 1 value")
        values = [await ctx.engine.process_value_async(v, ctx) for v in node["$add"]]
        return _c._add_reduce(values, max_number_result, max_string_result)
    return add_handler


def make_sub_handler(max_number_result: float = 1e15) -> AsyncSpecialFn:
    async def sub_handler(node, ctx):
        if not isinstance(node["$sub"], list) or len(node["$sub"]) < 1:
            raise ValueError("$sub requires a list of at least 1 value")
        values = [await ctx.engine.process_value_async(v, ctx) for v in node["$sub"]]
        return _c._sub_reduce(values, max_number_result)
    return sub_handler


def make_mul_handler(max_string_result: int = 1_000_000,
                     max_operand: float = 1e9) -> AsyncSpecialFn:
    async def mul_handler(node, ctx):
        if not isinstance(node["$mul"], list) or len(node["$mul"]) < 1:
            raise ValueError("$mul requires a list of at least 1 value")
        values = [await ctx.engine.process_value_async(v, ctx) for v in node["$mul"]]
        return _c._mul_reduce(values, max_string_result, max_operand)
    return mul_handler


async def div_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    if not isinstance(node["$div"], list) or len(node["$div"]) < 1:
        raise ValueError("$div requires a list of at least 1 value")
    values = [await ctx.engine.process_value_async(v, ctx) for v in node["$div"]]
    result = values[0]
    for val in values[1:]:
        result = result / val
    return result


def make_pow_handler(max_base: float = 1e6, max_exponent: float = 1000) -> AsyncSpecialFn:
    async def pow_handler(node, ctx):
        if not isinstance(node["$pow"], list) or len(node["$pow"]) < 1:
            raise ValueError("$pow requires a list of at least 1 value")
        values = [await ctx.engine.process_value_async(v, ctx) for v in node["$pow"]]
        return _c._pow_reduce(values, max_base, max_exponent)
    return pow_handler


async def mod_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    if not isinstance(node["$mod"], list) or len(node["$mod"]) < 1:
        raise ValueError("$mod requires a list of at least 1 value")
    values = [await ctx.engine.process_value_async(v, ctx) for v in node["$mod"]]
    result = values[0]
    for val in values[1:]:
        result = result % val
    return result


async def round_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    spec = node["$round"]
    if isinstance(spec, dict) and "value" in spec:
        value = await ctx.engine.process_value_async(spec["value"], ctx)
        ndigits = await ctx.engine.process_value_async(spec.get("ndigits", None), ctx)
        mode = await ctx.engine.process_value_async(spec.get("mode", "round"), ctx)
    else:
        value = await ctx.engine.process_value_async(spec, ctx)
        ndigits = None
        mode = "round"
    return _c._round_compute(value, ndigits, mode)


# ─────────────────────────────────────────────────────────────────────────────
# string operations
# ─────────────────────────────────────────────────────────────────────────────

def make_str_split_handler(max_results: int = 100_000) -> AsyncSpecialFn:
    async def str_split_handler(node, ctx):
        spec = node["$str_split"]
        if isinstance(spec, str):
            raise ValueError("$str_split requires a dict with 'string' and 'delimiter'")
        string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
        delimiter = await ctx.engine.process_value_async(spec.get("delimiter", " "), ctx)
        maxsplit = await ctx.engine.process_value_async(spec.get("maxsplit", -1), ctx)
        return _c._split_compute(string, delimiter, maxsplit, max_results)
    return str_split_handler


def make_str_join_handler(max_result_length: int = 10_000_000) -> AsyncSpecialFn:
    async def str_join_handler(node, ctx):
        spec = node["$str_join"]
        if isinstance(spec, str):
            raise ValueError("$str_join requires a dict with 'array' and 'separator'")
        array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
        separator = await ctx.engine.process_value_async(spec.get("separator", ""), ctx)
        return _c._join_compute(array, separator, max_result_length)
    return str_join_handler


async def str_slice_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    spec = node["$str_slice"]
    if isinstance(spec, str):
        raise ValueError("$str_slice requires a dict with 'string' and slice parameters")
    string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
    start = await ctx.engine.process_value_async(spec.get("start"), ctx)
    end = await ctx.engine.process_value_async(spec.get("end"), ctx)
    if not isinstance(string, str):
        raise ValueError(f"$str_slice 'string' must be a string, got {type(string).__name__}")
    return string[start:end]


async def str_upper_handler(node, ctx):
    string = await ctx.engine.process_value_async(node["$str_upper"], ctx)
    if not isinstance(string, str):
        raise ValueError(f"$str_upper requires a string, got {type(string).__name__}")
    return string.upper()


async def str_lower_handler(node, ctx):
    string = await ctx.engine.process_value_async(node["$str_lower"], ctx)
    if not isinstance(string, str):
        raise ValueError(f"$str_lower requires a string, got {type(string).__name__}")
    return string.lower()


async def _strip(key: str, method: str, node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    spec = node[key]
    if isinstance(spec, str):
        string = await ctx.engine.process_value_async(spec, ctx)
        if not isinstance(string, str):
            raise ValueError(f"{key} requires a string, got {type(string).__name__}")
        return getattr(string, method)()
    string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
    chars = await ctx.engine.process_value_async(spec.get("chars"), ctx)
    if not isinstance(string, str):
        raise ValueError(f"{key} 'string' must be a string, got {type(string).__name__}")
    return getattr(string, method)(chars)


async def str_strip_handler(node, ctx):
    return await _strip("$str_strip", "strip", node, ctx)


async def str_lstrip_handler(node, ctx):
    return await _strip("$str_lstrip", "lstrip", node, ctx)


async def str_rstrip_handler(node, ctx):
    return await _strip("$str_rstrip", "rstrip", node, ctx)


def make_str_replace_handler(max_result_length: int = 10_000_000) -> AsyncSpecialFn:
    async def str_replace_handler(node, ctx):
        spec = node["$str_replace"]
        string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
        old = await ctx.engine.process_value_async(spec["old"], ctx)
        new = await ctx.engine.process_value_async(spec["new"], ctx)
        count = await ctx.engine.process_value_async(spec.get("count", -1), ctx)
        return _c._replace_compute(string, old, new, count, max_result_length)
    return str_replace_handler


async def _str_check(key: str, arg_key: str, method: str,
                     node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    spec = node[key]
    string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
    arg = await ctx.engine.process_value_async(spec[arg_key], ctx)
    if not isinstance(string, str):
        raise ValueError(f"{key} 'string' must be a string, got {type(string).__name__}")
    if method == "contains":
        return arg in string
    return getattr(string, method)(arg)


async def str_contains_handler(node, ctx):
    return await _str_check("$str_contains", "substring", "contains", node, ctx)


async def str_startswith_handler(node, ctx):
    return await _str_check("$str_startswith", "prefix", "startswith", node, ctx)


async def str_endswith_handler(node, ctx):
    return await _str_check("$str_endswith", "suffix", "endswith", node, ctx)


# ─────────────────────────────────────────────────────────────────────────────
# regex (compute + flag validation reused from the sync helpers)
# ─────────────────────────────────────────────────────────────────────────────

def _make_regex(key: str, compute: Callable[..., Any],
                timeout: float, allowed_flags: int | None) -> AsyncSpecialFn:
    resolved_flags = _c._resolve_allowed_flags(allowed_flags)

    async def handler(node, ctx):
        spec = node[key]
        pattern = await ctx.engine.process_value_async(spec["pattern"], ctx)
        string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
        flags = await ctx.engine.process_value_async(spec.get("flags", 0), ctx)
        return compute(key, pattern, string, flags, resolved_flags, timeout)
    return handler


def make_regex_match_handler(timeout: float = 2.0, allowed_flags: int | None = None) -> AsyncSpecialFn:
    return _make_regex("$regex_match", _c._regex_match_compute, timeout, allowed_flags)


def make_regex_search_handler(timeout: float = 2.0, allowed_flags: int | None = None) -> AsyncSpecialFn:
    return _make_regex("$regex_search", _c._regex_search_compute, timeout, allowed_flags)


def make_regex_findall_handler(timeout: float = 2.0, allowed_flags: int | None = None) -> AsyncSpecialFn:
    return _make_regex("$regex_findall", _c._regex_findall_compute, timeout, allowed_flags)


def make_regex_groups_handler(timeout: float = 2.0, allowed_flags: int | None = None) -> AsyncSpecialFn:
    resolved_flags = _c._resolve_allowed_flags(allowed_flags)

    async def handler(node, ctx):
        spec = node["$regex_groups"]
        pattern = await ctx.engine.process_value_async(spec["pattern"], ctx)
        string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
        flags = await ctx.engine.process_value_async(spec.get("flags", 0), ctx)
        named = bool(await ctx.engine.process_value_async(spec.get("named", False), ctx))
        return _c._regex_groups_compute(
            "$regex_groups", pattern, string, flags, resolved_flags, timeout, named)
    return handler


async def _regex_replace_handler_impl(spec, ctx, resolved_flags, timeout):
    pattern = await ctx.engine.process_value_async(spec["pattern"], ctx)
    replacement = await ctx.engine.process_value_async(spec["replacement"], ctx)
    string = await ctx.engine.process_value_async(spec.get("string", ""), ctx)
    count = await ctx.engine.process_value_async(spec.get("count", 0), ctx)
    flags = await ctx.engine.process_value_async(spec.get("flags", 0), ctx)
    return _c._regex_replace_compute(pattern, replacement, string, count, flags, resolved_flags, timeout)


def make_regex_replace_handler(timeout: float = 2.0, allowed_flags: int | None = None) -> AsyncSpecialFn:
    resolved_flags = _c._resolve_allowed_flags(allowed_flags)

    async def regex_replace_handler(node, ctx):
        return await _regex_replace_handler_impl(node["$regex_replace"], ctx, resolved_flags, timeout)
    return regex_replace_handler


# ─────────────────────────────────────────────────────────────────────────────
# $cast / $raw
# ─────────────────────────────────────────────────────────────────────────────

def make_cast_handler(casters: Mapping[str, Any]) -> AsyncSpecialFn:
    _casters = dict(casters)

    async def cast_handler(node, ctx):
        cast_spec = node.get("$cast")
        if not isinstance(cast_spec, Mapping):
            raise ValueError(
                f"$cast construct requires a dict with 'value' and 'type' keys, "
                f"got {type(cast_spec).__name__}: {cast_spec!r}"
            )
        if "value" not in cast_spec or "type" not in cast_spec:
            raise ValueError(
                f"$cast construct requires both 'value' and 'type' keys, "
                f"got keys: {list(cast_spec.keys())}"
            )
        value = await ctx.engine.process_value_async(cast_spec["value"], ctx)
        type_name = await ctx.engine.process_value_async(cast_spec["type"], ctx)
        if not isinstance(type_name, str):
            raise ValueError(
                f"$cast type must be a string, got {type(type_name).__name__}: {type_name!r}"
            )
        if type_name not in _casters:
            raise KeyError(
                f"Unknown cast type '{type_name}'. "
                f"Available types: {sorted(_casters.keys())}"
            )
        return _casters[type_name](value)
    return cast_handler


async def raw_handler(node: Mapping[str, Any], ctx: ExecutionContext) -> Any:
    raise RawValueSignal(node["$raw"])


# ─────────────────────────────────────────────────────────────────────────────
# Collection / value constructs (compute reused from the sync helpers)
# ─────────────────────────────────────────────────────────────────────────────

async def len_handler(node, ctx):
    return _c._len_compute(await ctx.engine.process_value_async(node["$len"], ctx))


async def keys_handler(node, ctx):
    return _c._keys_compute(await ctx.engine.process_value_async(node["$keys"], ctx))


async def values_handler(node, ctx):
    return _c._values_compute(await ctx.engine.process_value_async(node["$values"], ctx))


async def items_handler(node, ctx):
    return _c._items_compute(await ctx.engine.process_value_async(node["$items"], ctx))


async def reverse_handler(node, ctx):
    return _c._reverse_compute(await ctx.engine.process_value_async(node["$reverse"], ctx))


async def slice_handler(node, ctx):
    spec = node["$slice"]
    if isinstance(spec, str):
        raise ValueError("$slice requires a dict with 'array' and slice parameters")
    array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
    start = await ctx.engine.process_value_async(spec.get("start"), ctx)
    end = await ctx.engine.process_value_async(spec.get("end"), ctx)
    step = await ctx.engine.process_value_async(spec.get("step"), ctx)
    return _c._slice_compute(array, start, end, step)


async def flatten_handler(node, ctx):
    spec = node["$flatten"]
    if isinstance(spec, Mapping):
        array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
        depth = await ctx.engine.process_value_async(spec.get("depth", 1), ctx)
    else:
        array = await ctx.engine.process_value_async(spec, ctx)
        depth = 1
    return _c._flatten_compute(array, depth)


async def type_handler(node, ctx):
    return _c._type_compute(await ctx.engine.process_value_async(node["$type"], ctx))


async def sum_handler(node, ctx):
    return _c._sum_compute(await ctx.engine.process_value_async(node["$sum"], ctx))


async def avg_handler(node, ctx):
    return _c._avg_compute(await ctx.engine.process_value_async(node["$avg"], ctx))


async def _resolve_key_fn(spec, ctx):
    if "key" not in spec:
        return None
    key_ptr = await ctx.engine.process_value_async(spec["key"], ctx)
    return lambda item: ctx.engine.resolver.get(key_ptr, item)


async def min_handler(node, ctx):
    spec = node["$min"]
    if isinstance(spec, Mapping):
        array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
        key_fn = await _resolve_key_fn(spec, ctx)
    else:
        array = await ctx.engine.process_value_async(spec, ctx)
        key_fn = None
    return _c._minmax_compute("$min", array, "min", key_fn)


async def max_handler(node, ctx):
    spec = node["$max"]
    if isinstance(spec, Mapping):
        array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
        key_fn = await _resolve_key_fn(spec, ctx)
    else:
        array = await ctx.engine.process_value_async(spec, ctx)
        key_fn = None
    return _c._minmax_compute("$max", array, "max", key_fn)


async def sort_handler(node, ctx):
    spec = node["$sort"]
    if isinstance(spec, Mapping):
        array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
        key_fn = await _resolve_key_fn(spec, ctx)
        reverse = bool(await ctx.engine.process_value_async(spec.get("reverse", False), ctx))
    else:
        array = await ctx.engine.process_value_async(spec, ctx)
        key_fn = None
        reverse = False
    return _c._sort_compute(array, key_fn, reverse)


async def unique_handler(node, ctx):
    spec = node["$unique"]
    if isinstance(spec, Mapping):
        array = await ctx.engine.process_value_async(spec.get("array", []), ctx)
        key_fn = await _resolve_key_fn(spec, ctx)
    else:
        array = await ctx.engine.process_value_async(spec, ctx)
        key_fn = None
    return _c._unique_compute(array, key_fn)


async def abs_handler(node, ctx):
    return _c._abs_compute(await ctx.engine.process_value_async(node["$abs"], ctx))


async def floor_handler(node, ctx):
    return _c._floor_compute(await ctx.engine.process_value_async(node["$floor"], ctx))


async def ceil_handler(node, ctx):
    return _c._ceil_compute(await ctx.engine.process_value_async(node["$ceil"], ctx))


def make_map_handler(max_items: int = 100_000) -> AsyncSpecialFn:
    async def map_handler(node, ctx):
        spec = node["$map"]
        if isinstance(spec, str):
            raise ValueError("$map requires a dict with 'in' and 'expr'")
        array = await ctx.engine.process_value_async(spec["in"], ctx)
        var = await ctx.engine.process_value_async(spec.get("as", "item"), ctx)
        expr = spec["expr"]
        items = _c._normalize_map_iterable("$map", array, max_items)
        result = []
        for elem in items:
            sub = ctx.copy(new_temp_read_only={**ctx.temp_read_only, var: elem})
            result.append(await ctx.engine.process_value_async(expr, sub))
        return result
    return map_handler


map_handler = make_map_handler()


def make_filter_handler(max_items: int = 100_000) -> AsyncSpecialFn:
    async def filter_handler(node, ctx):
        spec = node["$filter"]
        if isinstance(spec, str):
            raise ValueError("$filter requires a dict with 'in' and 'cond'")
        array = await ctx.engine.process_value_async(spec["in"], ctx)
        var = await ctx.engine.process_value_async(spec.get("as", "item"), ctx)
        cond = spec["cond"]
        items = _c._normalize_map_iterable("$filter", array, max_items)
        result = []
        for elem in items:
            sub = ctx.copy(new_temp_read_only={**ctx.temp_read_only, var: elem})
            if await ctx.engine.process_value_async(cond, sub):
                result.append(elem)
        return result
    return filter_handler


filter_handler = make_filter_handler()
