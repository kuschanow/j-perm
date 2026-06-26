"""Tests for the j-perm text syntax (parse_text + ParseTextStage + engine)."""
import pytest

from j_perm import build_default_engine, build_default_async_engine
from j_perm.text import parse_text


def steps(src):
    return parse_text(src)


def one(src):
    s = parse_text(src)
    assert len(s) == 1, s
    return s[0]


# ───────────────────────── statements ──────────────────────────────────────

def test_set_literal():
    assert one('/a = 1') == {"op": "set", "path": "/a", "value": 1}


def test_set_append():
    assert one('/a[] = 1') == {"op": "set", "path": "/a/-", "value": 1}


def test_copy():
    assert one('/a <- /b') == {"op": "copy", "from": "/b", "path": "/a", "ignore_missing": True}


def test_copy_default():
    assert one('/a <- /b ?? 5') == {
        "op": "copy", "from": "/b", "path": "/a", "ignore_missing": True, "default": 5}


def test_copy_strict():
    assert one('/a <-! /b') == {"op": "copy", "from": "/b", "path": "/a", "ignore_missing": False}


def test_del():
    assert steps('del /a, /b') == [
        {"op": "delete", "path": "/a", "ignore_missing": True},
        {"op": "delete", "path": "/b", "ignore_missing": True},
    ]


def test_del_strict():
    assert one('del! /a') == {"op": "delete", "path": "/a", "ignore_missing": False}


def test_assert_exists():
    assert one('assert /a') == {"op": "assert", "path": "/a"}


def test_assert_equals():
    assert one('assert /a == 10') == {"op": "assert", "path": "/a", "equals": 10}


def test_if_only():
    assert one('if $(/x): /a = 1') == {
        "op": "if", "cond": {"$ref": "/x"}, "then": [{"op": "set", "path": "/a", "value": 1}]}


def test_if_else():
    d = one('if $(/x): /a = 1\nelse: /a = 2')
    assert d["else"] == [{"op": "set", "path": "/a", "value": 2}]


def test_if_elif_else():
    src = 'if $(/x) == 1: /a = 1\nelif $(/x) == 2: /a = 2\nelse: /a = 3'
    d = one(src)
    assert d["cond"] == {"$eq": [{"$ref": "/x"}, 1]}
    inner = d["else"][0]
    assert inner["op"] == "if"
    assert inner["cond"] == {"$eq": [{"$ref": "/x"}, 2]}
    assert inner["else"] == [{"op": "set", "path": "/a", "value": 3}]


def test_foreach_in_pointer():
    d = one('foreach it in /xs:\n    /o[] = $(&:/it)')
    assert d == {"op": "foreach", "as": "it", "in": "/xs",
                 "do": [{"op": "set", "path": "/o/-", "value": {"$ref": "&:/it"}}]}


def test_foreach_in_value():
    d = one('foreach it in [1, 2]:\n    /o[] = $(&:/it)')
    assert d["in_value"] == [1, 2]
    assert "in" not in d


def test_foreach_default():
    d = one('foreach it in /xs default []:\n    /o[] = $(&:/it)')
    assert d["in"] == "/xs"
    assert d["default"] == []


def test_while():
    d = one('while $(@:/n) < 3:\n    /n = $(@:/n) + 1')
    assert d["op"] == "while"
    assert d["cond"] == {"$lt": [{"$ref": "@:/n"}, 3]}


def test_do_while():
    d = one('do:\n    /n = 1\nwhile $(@:/n) < 3')
    assert d["op"] == "while" and d["do_while"] is True


def test_try_except_finally():
    d = one('try:\n    /a = 1\nexcept:\n    /b = 2\nfinally:\n    /c = 3')
    assert d["do"] == [{"op": "set", "path": "/a", "value": 1}]
    assert d["except"] == [{"op": "set", "path": "/b", "value": 2}]
    assert d["finally"] == [{"op": "set", "path": "/c", "value": 3}]


def test_try_only():
    d = one('try:\n    /a = 1')
    assert "except" not in d and "finally" not in d


def test_def_basic():
    d = one('def f():\n    /a = 1')
    assert d == {"$def": "f", "params": [], "body": [{"op": "set", "path": "/a", "value": 1}]}


def test_def_params_context_failure():
    src = ('def f(x, y) context=new:\n    /a = $(&:/x)\n'
           'on_failure:\n    /err = true')
    d = one(src)
    assert d["params"] == ["x", "y"]
    assert d["context"] == "new"
    assert d["on_failure"] == [{"op": "set", "path": "/err", "value": True}]


def test_return_bare():
    assert one('return') == {"$return": None}


def test_return_value():
    assert one('return $(@:/x)') == {"$return": {"$ref": "@:/x"}}


def test_raise():
    assert one('raise "boom"') == {"$raise": "boom"}


def test_break_continue():
    assert one('break') == {"$break": None}
    assert one('continue') == {"$continue": None}


def test_exec_from():
    assert one('exec /script') == {"op": "exec", "from": "/script"}


def test_exec_from_merge():
    assert one('exec /script merge') == {"op": "exec", "from": "/script", "merge": True}


def test_exec_suite():
    d = one('exec:\n    /a = 1')
    assert d == {"op": "exec", "actions": [{"op": "set", "path": "/a", "value": 1}]}


def test_op_generic_with_block():
    d = one('op "custom"(x: 1):\n    /a = 2')
    assert d == {"op": "custom", "x": 1, "do": [{"op": "set", "path": "/a", "value": 2}]}


def test_op_generic_no_block():
    assert one('op "custom"(x: 1)') == {"op": "custom", "x": 1}


def test_callstmt():
    assert one('f("x", 2)') == {"$func": "f", "args": ["x", 2]}


# ───────────────────────── expressions ─────────────────────────────────────

def test_arithmetic_precedence():
    assert one('/r = 1 + 2 * 3')["value"] == {"$add": [1, {"$mul": [2, 3]}]}


def test_sub_div_mod_pow():
    assert one('/r = 10 - 2')["value"] == {"$sub": [10, 2]}
    assert one('/r = 10 / 2')["value"] == {"$div": [10, 2]}
    assert one('/r = 10 % 3')["value"] == {"$mod": [10, 3]}
    assert one('/r = 2 ** 3')["value"] == {"$pow": [2, 3]}


def test_comparisons():
    cases = {"==": "$eq", "!=": "$ne", "<": "$lt", "<=": "$lte", ">": "$gt", ">=": "$gte"}
    for op, key in cases.items():
        assert one(f'/r = 1 {op} 2')["value"] == {key: [1, 2]}


def test_in_operator():
    assert one('/r = 1 in [1, 2]')["value"] == {"$in": [1, [1, 2]]}


def test_logical_and_or_not_coalesce():
    assert one('/r = $(/a) and $(/b)')["value"] == {"$and": [{"$ref": "/a"}, {"$ref": "/b"}]}
    assert one('/r = $(/a) or $(/b)')["value"] == {"$or": [{"$ref": "/a"}, {"$ref": "/b"}]}
    assert one('/r = not $(/a)')["value"] == {"$not": {"$ref": "/a"}}
    assert one('/r = $(/a) ?? 1')["value"] == {"$or": [{"$ref": "/a"}, 1]}


def test_unary_minus_number_and_expr():
    assert one('/r = -5')["value"] == -5
    assert one('/r = -$(/a)')["value"] == {"$sub": [0, {"$ref": "/a"}]}


def test_exists():
    assert one('/r = exists /a')["value"] == {"$exists": "/a"}


def test_atoms():
    assert one('/r = 3.14')["value"] == 3.14
    assert one('/r = true')["value"] is True
    assert one('/r = false')["value"] is False
    assert one('/r = null')["value"] is None
    assert one('/r = /ptr')["value"] == "/ptr"  # bare pointer = string literal
    assert one('/r = (1 + 2)')["value"] == {"$add": [1, 2]}


def test_read_variants():
    assert one('/r = $(/p)')["value"] == {"$ref": "/p"}
    assert one('/r = $(/p) raw')["value"] == {"$ref": "/p", "$raw": True}
    assert one('/r = $(/p ?? 9)')["value"] == {"$ref": "/p", "$default": 9}


def test_string_unescape():
    assert one(r'/r = "a\nb\tc\"d\\e\x"')["value"] == 'a\nb\tc"d\\e\\x'


def test_casts():
    assert one('/r = int("42")')["value"] == {"$cast": {"value": "42", "type": "int"}}
    assert one('/r = float("1.5")')["value"] == {"$cast": {"value": "1.5", "type": "float"}}
    assert one('/r = bool("1")')["value"] == {"$cast": {"value": "1", "type": "bool"}}
    assert one('/r = str(5)')["value"] == {"$cast": {"value": 5, "type": "str"}}


def test_str_funcs():
    assert one('/r = upper("a")')["value"] == {"$str_upper": "a"}
    assert one('/r = lower("A")')["value"] == {"$str_lower": "A"}
    assert one('/r = strip("  a  ")')["value"] == {"$str_strip": "  a  "}
    assert one('/r = strip("xax", "x")')["value"] == {"$str_strip": {"string": "xax", "chars": "x"}}
    assert one('/r = lstrip("xa", "x")')["value"] == {"$str_lstrip": {"string": "xa", "chars": "x"}}
    assert one('/r = rstrip("ax", "x")')["value"] == {"$str_rstrip": {"string": "ax", "chars": "x"}}
    assert one('/r = split("a,b", ",")')["value"] == {"$str_split": {"string": "a,b", "delimiter": ","}}
    assert one('/r = split("a,b,c", ",", maxsplit: 1)')["value"] == {
        "$str_split": {"string": "a,b,c", "delimiter": ",", "maxsplit": 1}}
    assert one('/r = join([1, 2], "-")')["value"] == {"$str_join": {"array": [1, 2], "separator": "-"}}
    assert one('/r = replace("aa", "a", "b")')["value"] == {
        "$str_replace": {"string": "aa", "old": "a", "new": "b"}}
    assert one('/r = replace("aa", "a", "b", count: 1)')["value"] == {
        "$str_replace": {"string": "aa", "old": "a", "new": "b", "count": 1}}
    assert one('/r = contains("ab", "b")')["value"] == {"$str_contains": {"string": "ab", "substring": "b"}}
    assert one('/r = startswith("ab", "a")')["value"] == {"$str_startswith": {"string": "ab", "prefix": "a"}}
    assert one('/r = endswith("ab", "b")')["value"] == {"$str_endswith": {"string": "ab", "suffix": "b"}}


def test_slice():
    assert one('/r = slice("hello")')["value"] == {"$str_slice": {"string": "hello"}}
    assert one('/r = slice("hello", 1)')["value"] == {"$str_slice": {"string": "hello", "start": 1}}
    assert one('/r = slice("hello", 1, 4)')["value"] == {
        "$str_slice": {"string": "hello", "start": 1, "end": 4}}


def test_round():
    assert one('/r = round(3.7)')["value"] == {"$round": 3.7}
    assert one('/r = round(3.14159, 2)')["value"] == {"$round": {"value": 3.14159, "ndigits": 2}}
    assert one('/r = round(3.1, 2, mode: "ceil")')["value"] == {
        "$round": {"value": 3.1, "ndigits": 2, "mode": "ceil"}}


def test_regex():
    assert one('/r = regex_match("a", "abc")')["value"] == {
        "$regex_match": {"pattern": "a", "string": "abc"}}
    assert one('/r = regex_search("a", "abc", flags: 2)')["value"] == {
        "$regex_search": {"pattern": "a", "string": "abc", "flags": 2}}
    assert one('/r = regex_findall("a", "aa")')["value"] == {
        "$regex_findall": {"pattern": "a", "string": "aa"}}
    assert one('/r = regex_groups("(a)", "a")')["value"] == {
        "$regex_groups": {"pattern": "(a)", "string": "a"}}
    assert one('/r = regex_replace("a", "b", "aa")')["value"] == {
        "$regex_replace": {"pattern": "a", "replacement": "b", "string": "aa"}}
    assert one('/r = regex_replace("a", "b", "aa", count: 1, flags: 2)')["value"] == {
        "$regex_replace": {"pattern": "a", "replacement": "b", "string": "aa", "count": 1, "flags": 2}}


def test_ref_and_raw_calls():
    assert one('/r = ref(/p)')["value"] == {"$ref": "/p"}
    assert one('/r = ref(/p, default: 9)')["value"] == {"$ref": "/p", "$default": 9}
    assert one('/r = raw($(/p))')["value"] == {"$raw": {"$ref": "/p"}}


def test_user_func_fallback():
    assert one('/r = myfunc(1, 2)')["value"] == {"$func": "myfunc", "args": [1, 2]}


def test_no_arg_call():
    assert one('/r = myfunc()')["value"] == {"$func": "myfunc", "args": []}
    assert one('thing()') == {"$func": "thing", "args": []}


def test_list_and_dict():
    assert one('/r = []')["value"] == []
    assert one('/r = {}')["value"] == {}
    assert one('/r = [1, "x", true]')["value"] == [1, "x", True]
    assert one('/r = {"k": 1, ident: 2}')["value"] == {"k": 1, "ident": 2}


def test_eval():
    assert one('/r = eval { /x = 1 }')["value"] == {
        "$eval": [{"op": "set", "path": "/x", "value": 1}]}
    assert one('/r = eval { /x = 1 } select /x')["value"] == {
        "$eval": [{"op": "set", "path": "/x", "value": 1}], "$select": "/x"}


# ───────────────────────── block styles & mixing ───────────────────────────

def test_brace_block():
    d = one('if $(/x) > 0 { /a = 1 } else { /a = 2 }')
    assert d["then"] == [{"op": "set", "path": "/a", "value": 1}]
    assert d["else"] == [{"op": "set", "path": "/a", "value": 2}]


def test_semicolon_separator():
    assert steps('/a = 1; /b = 2') == [
        {"op": "set", "path": "/a", "value": 1},
        {"op": "set", "path": "/b", "value": 2},
    ]


def test_blank_lines_and_comments():
    src = '\n# comment\n/a = 1\n\n# another\n/b = 2\n'
    assert steps(src) == [
        {"op": "set", "path": "/a", "value": 1},
        {"op": "set", "path": "/b", "value": 2},
    ]


def test_empty_program():
    assert steps('\n\n') == []


# ───────────────────────── engine round-trips ──────────────────────────────

def test_engine_text():
    eng = build_default_engine()
    r = eng.apply('/total = $(/price) * $(/qty) + 10', source={"price": 5, "qty": 3}, dest={})
    assert r == {"total": 25}


def test_engine_mixed_list():
    eng = build_default_engine()
    spec = ['/a = 1', {"op": "set", "path": "/b", "value": 2}, '/c = $(@:/a) + $(@:/b)']
    assert eng.apply(spec, source={}, dest={}) == {"a": 1, "b": 2, "c": 3}


def test_engine_block_and_func():
    eng = build_default_engine()
    prog = ('def greet(name):\n    /msg = "Hi ${&:/name}"\n    return $(@:/msg)\n'
            '/r = greet("Bob")')
    assert eng.apply(prog, source={}, dest={}) == {"r": "Hi Bob"}


def test_compile_mixed():
    eng = build_default_engine()
    spec = ['/a = 1', '/b = $(@:/a) + 1']
    compiled = eng.compile(spec)
    assert compiled is not None
    assert compiled.apply(source={}, dest={}) == {"a": 1, "b": 2}


def test_async_text():
    import asyncio
    aeng = build_default_async_engine()
    r = asyncio.run(aeng.apply_async('/x = $(/y) + 1', source={"y": 9}, dest={}))
    assert r == {"x": 10}


def test_text_syntax_disabled():
    eng = build_default_engine(text_syntax=False)
    with pytest.raises(ValueError):
        eng.apply('/a = 1', source={}, dest={})


def test_no_langforge_import_at_runtime():
    import sys
    # importing the text package must not pull in langforge
    import j_perm.text  # noqa: F401
    assert "langforge" not in sys.modules
