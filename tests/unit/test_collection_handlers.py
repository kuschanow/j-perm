"""Tests for the collection / value / math constructs and regex named groups.

Covers ``$len``, ``$keys``, ``$values``, ``$items``, ``$reverse``, ``$slice``,
``$flatten``, ``$type``, ``$sum``, ``$avg``, ``$min``, ``$max``, ``$sort``,
``$unique``, ``$abs``, ``$floor``, ``$ceil``, ``$map``, ``$filter`` (sync +
async + text DSL + error branches) and the ``named`` option on ``$regex_groups``.
"""

import pytest

from j_perm import build_default_engine, build_default_async_engine
from j_perm.text import parse_text
from j_perm.handlers import constructs as _c


def ev(value, source=None):
    engine = build_default_engine()
    return engine.apply({"/r": value}, source=source or {}, dest={})["r"]


async def aev(value, source=None):
    engine = build_default_async_engine()
    result = await engine.apply_async({"/r": value}, source=source or {}, dest={})
    return result["r"]


def one(src):
    steps = parse_text(src)
    assert len(steps) == 1, steps
    return steps[0]["value"]


# ─────────────────────────────────────────────────────────────────────────────
# Sync happy paths
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectionSync:
    def test_len_list(self):
        assert ev({"$len": [1, 2, 3]}) == 3

    def test_len_dict(self):
        assert ev({"$len": {"a": 1, "b": 2}}) == 2

    def test_len_string(self):
        assert ev({"$len": "hello"}) == 5

    def test_keys(self):
        assert ev({"$keys": {"a": 1, "b": 2}}) == ["a", "b"]

    def test_values(self):
        assert ev({"$values": {"a": 1, "b": 2}}) == [1, 2]

    def test_items(self):
        assert ev({"$items": {"a": 1, "b": 2}}) == [["a", 1], ["b", 2]]

    def test_reverse_list(self):
        assert ev({"$reverse": [1, 2, 3]}) == [3, 2, 1]

    def test_reverse_string(self):
        assert ev({"$reverse": "abc"}) == "cba"

    def test_slice_start_end(self):
        assert ev({"$slice": {"array": [1, 2, 3, 4, 5], "start": 1, "end": 4}}) == [2, 3, 4]

    def test_slice_step(self):
        assert ev({"$slice": {"array": [1, 2, 3, 4, 5], "step": 2}}) == [1, 3, 5]

    def test_slice_negative(self):
        assert ev({"$slice": {"array": [1, 2, 3], "start": -2}}) == [2, 3]

    def test_flatten_simple(self):
        assert ev({"$flatten": [[1, 2], [3, 4]]}) == [1, 2, 3, 4]

    def test_flatten_depth(self):
        assert ev({"$flatten": {"array": [1, [2, [3]]], "depth": 1}}) == [1, 2, [3]]

    def test_flatten_deep(self):
        assert ev({"$flatten": {"array": [1, [2, [3]]], "depth": -1}}) == [1, 2, 3]

    def test_type_variants(self):
        assert ev({"$type": "hi"}) == "string"
        assert ev({"$type": 42}) == "number"
        assert ev({"$type": True}) == "bool"
        assert ev({"$type": [1, 2]}) == "list"
        assert ev({"$type": {"a": 1}}) == "dict"
        assert ev({"$type": None}) == "null"

    def test_sum(self):
        assert ev({"$sum": [1, 2, 3]}) == 6

    def test_sum_empty(self):
        assert ev({"$sum": []}) == 0

    def test_avg(self):
        assert ev({"$avg": [2, 4, 6]}) == 4.0

    def test_min_simple(self):
        assert ev({"$min": [3, 1, 2]}) == 1

    def test_max_simple(self):
        assert ev({"$max": [3, 1, 2]}) == 3

    def test_min_key(self):
        assert ev({"$min": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}) == {"n": 1}

    def test_max_key(self):
        assert ev({"$max": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}) == {"n": 3}

    def test_sort_simple(self):
        assert ev({"$sort": [3, 1, 2]}) == [1, 2, 3]

    def test_sort_reverse(self):
        assert ev({"$sort": {"array": [3, 1, 2], "reverse": True}}) == [3, 2, 1]

    def test_sort_key(self):
        assert ev({"$sort": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}) == [{"n": 1}, {"n": 3}]

    def test_unique_simple(self):
        assert ev({"$unique": [1, 2, 2, 3, 1]}) == [1, 2, 3]

    def test_unique_key(self):
        assert ev({"$unique": {"array": [{"id": 1}, {"id": 1}, {"id": 2}], "key": "/id"}}) == [
            {"id": 1}, {"id": 2},
        ]

    def test_unique_unhashable(self):
        # Unhashable elements (lists) are always kept.
        assert ev({"$unique": [[1], [1], [2]]}) == [[1], [1], [2]]

    def test_abs(self):
        assert ev({"$abs": -5}) == 5

    def test_floor(self):
        assert ev({"$floor": 3.7}) == 3

    def test_ceil(self):
        assert ev({"$ceil": 3.2}) == 4

    def test_map(self):
        assert ev({"$map": {"in": [1, 2, 3], "as": "n", "expr": {"$mul": ["${&:/n}", 2]}}}) == [2, 4, 6]

    def test_map_dict_input(self):
        # dict source → [key, value] pairs; project the value.
        assert ev({"$map": {"in": {"a": 1}, "as": "kv", "expr": {"$ref": "&:/kv/1"}}}) == [1]

    def test_map_default_var(self):
        assert ev({"$map": {"in": [1, 2], "expr": {"$add": ["${&:/item}", 10]}}}) == [11, 12]

    def test_filter(self):
        assert ev({"$filter": {"in": [1, 2, 3, 4], "as": "n", "cond": {"$gt": ["${&:/n}", 2]}}}) == [3, 4]

    def test_dynamic_params(self):
        # Every option flows through process_value → can be a reference.
        source = {"rev": True, "key": "/n", "arr": [{"n": 3}, {"n": 1}]}
        # reverse resolves to True → descending sort by /n.
        assert ev({"$sort": {"array": {"$ref": "/arr"}, "key": "${/key}", "reverse": "${/rev}"}},
                  source) == [{"n": 3}, {"n": 1}]


# ─────────────────────────────────────────────────────────────────────────────
# Error branches
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectionErrors:
    def test_len_scalar(self):
        with pytest.raises(ValueError):
            ev({"$len": 5})

    def test_keys_not_dict(self):
        with pytest.raises(ValueError):
            ev({"$keys": [1, 2]})

    def test_values_not_dict(self):
        with pytest.raises(ValueError):
            ev({"$values": [1, 2]})

    def test_items_not_dict(self):
        with pytest.raises(ValueError):
            ev({"$items": [1, 2]})

    def test_reverse_scalar(self):
        with pytest.raises(ValueError):
            ev({"$reverse": 5})

    def test_slice_not_list(self):
        with pytest.raises(ValueError):
            ev({"$slice": {"array": "abc"}})

    def test_slice_str_spec(self):
        with pytest.raises(ValueError):
            ev({"$slice": "oops"})

    def test_flatten_not_list(self):
        with pytest.raises(ValueError):
            ev({"$flatten": "abc"})

    def test_sum_not_list(self):
        with pytest.raises(ValueError):
            ev({"$sum": "abc"})

    def test_sum_non_number_element(self):
        with pytest.raises(ValueError):
            ev({"$sum": [1, "x"]})

    def test_sum_rejects_bool(self):
        with pytest.raises(ValueError):
            ev({"$sum": [True, False]})

    def test_avg_empty(self):
        with pytest.raises(ValueError):
            ev({"$avg": []})

    def test_min_empty(self):
        with pytest.raises(ValueError):
            ev({"$min": []})

    def test_min_not_list(self):
        with pytest.raises(ValueError):
            ev({"$min": "abc"})

    def test_sort_not_list(self):
        with pytest.raises(ValueError):
            ev({"$sort": "abc"})

    def test_unique_not_list(self):
        with pytest.raises(ValueError):
            ev({"$unique": "abc"})

    def test_abs_not_number(self):
        with pytest.raises(ValueError):
            ev({"$abs": "x"})

    def test_floor_not_number(self):
        with pytest.raises(ValueError):
            ev({"$floor": "x"})

    def test_ceil_not_number(self):
        with pytest.raises(ValueError):
            ev({"$ceil": "x"})

    def test_map_str_spec(self):
        with pytest.raises(ValueError):
            ev({"$map": "oops"})

    def test_filter_str_spec(self):
        with pytest.raises(ValueError):
            ev({"$filter": "oops"})

    def test_map_input_not_iterable(self):
        with pytest.raises(ValueError):
            ev({"$map": {"in": 5, "expr": 1}})

    def test_map_size_cap(self):
        engine = build_default_engine(map_filter_max_items=1)
        with pytest.raises(ValueError):
            engine.apply({"/r": {"$map": {"in": [1, 2], "expr": 0}}}, source={}, dest={})


# ─────────────────────────────────────────────────────────────────────────────
# Direct compute-helper coverage for branches not reachable through JSON
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeDirect:
    def test_type_fallback(self):
        # A value that is none of the JSON types → Python type name.
        assert _c._type_compute({1, 2}) == "set"

    def test_flatten_tuple_element(self):
        assert _c._flatten_compute([(1, 2), 3], 1) == [1, 2, 3]

    def test_len_tuple(self):
        assert _c._len_compute((1, 2, 3)) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Async twins
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectionAsync:
    async def test_len(self):
        assert await aev({"$len": [1, 2, 3]}) == 3

    async def test_keys(self):
        assert await aev({"$keys": {"a": 1}}) == ["a"]

    async def test_values(self):
        assert await aev({"$values": {"a": 1}}) == [1]

    async def test_items(self):
        assert await aev({"$items": {"a": 1}}) == [["a", 1]]

    async def test_reverse(self):
        assert await aev({"$reverse": "abc"}) == "cba"

    async def test_slice(self):
        assert await aev({"$slice": {"array": [1, 2, 3, 4], "start": 1, "end": 3}}) == [2, 3]

    async def test_slice_str_spec(self):
        with pytest.raises(ValueError):
            await aev({"$slice": "oops"})

    async def test_flatten_simple(self):
        assert await aev({"$flatten": [[1], [2]]}) == [1, 2]

    async def test_flatten_depth(self):
        assert await aev({"$flatten": {"array": [1, [2, [3]]], "depth": -1}}) == [1, 2, 3]

    async def test_type(self):
        assert await aev({"$type": None}) == "null"

    async def test_sum(self):
        assert await aev({"$sum": [1, 2, 3]}) == 6

    async def test_avg(self):
        assert await aev({"$avg": [2, 4]}) == 3.0

    async def test_min_simple(self):
        assert await aev({"$min": [3, 1, 2]}) == 1

    async def test_min_key(self):
        assert await aev({"$min": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}) == {"n": 1}

    async def test_max_simple(self):
        assert await aev({"$max": [3, 1, 2]}) == 3

    async def test_max_key(self):
        assert await aev({"$max": {"array": [{"n": 3}, {"n": 1}], "key": "/n"}}) == {"n": 3}

    async def test_sort_simple(self):
        assert await aev({"$sort": [3, 1, 2]}) == [1, 2, 3]

    async def test_sort_key_reverse(self):
        assert await aev({"$sort": {"array": [{"n": 1}, {"n": 3}], "key": "/n", "reverse": True}}) == [
            {"n": 3}, {"n": 1},
        ]

    async def test_sort_dict_no_key(self):
        # dict-form spec without a 'key' → no key extractor.
        assert await aev({"$sort": {"array": [3, 1, 2], "reverse": True}}) == [3, 2, 1]

    async def test_unique_simple(self):
        assert await aev({"$unique": [1, 1, 2]}) == [1, 2]

    async def test_unique_key(self):
        assert await aev({"$unique": {"array": [{"id": 1}, {"id": 1}], "key": "/id"}}) == [{"id": 1}]

    async def test_abs(self):
        assert await aev({"$abs": -5}) == 5

    async def test_floor(self):
        assert await aev({"$floor": 3.7}) == 3

    async def test_ceil(self):
        assert await aev({"$ceil": 3.2}) == 4

    async def test_map(self):
        assert await aev({"$map": {"in": [1, 2, 3], "as": "n", "expr": {"$mul": ["${&:/n}", 2]}}}) == [2, 4, 6]

    async def test_map_str_spec(self):
        with pytest.raises(ValueError):
            await aev({"$map": "oops"})

    async def test_filter(self):
        assert await aev({"$filter": {"in": [1, 2, 3, 4], "as": "n", "cond": {"$gt": ["${&:/n}", 2]}}}) == [3, 4]

    async def test_filter_str_spec(self):
        with pytest.raises(ValueError):
            await aev({"$filter": "oops"})

    async def test_map_size_cap(self):
        engine = build_default_async_engine(map_filter_max_items=1)
        with pytest.raises(ValueError):
            await engine.apply_async({"/r": {"$map": {"in": [1, 2], "expr": 0}}}, source={}, dest={})


# ─────────────────────────────────────────────────────────────────────────────
# Regex named groups
# ─────────────────────────────────────────────────────────────────────────────

class TestRegexNamed:
    def test_named_dict(self):
        assert ev({"$regex_groups": {
            "pattern": r"(?P<u>\w+)@(?P<d>\w+)", "string": "user@domain", "named": True,
        }}) == {"u": "user", "d": "domain"}

    def test_named_default_positional(self):
        assert ev({"$regex_groups": {
            "pattern": r"(\w+)@(\w+)", "string": "user@domain",
        }}) == ["user", "domain"]

    def test_named_no_match_dict(self):
        assert ev({"$regex_groups": {
            "pattern": r"(?P<u>\d+)", "string": "abc", "named": True,
        }}) == {}

    def test_named_no_match_list(self):
        assert ev({"$regex_groups": {"pattern": r"(\d+)", "string": "abc"}}) == []

    async def test_named_async(self):
        assert await aev({"$regex_groups": {
            "pattern": r"(?P<u>\w+)@(?P<d>\w+)", "string": "a@b", "named": True,
        }}) == {"u": "a", "d": "b"}

    async def test_named_async_no_match(self):
        assert await aev({"$regex_groups": {
            "pattern": r"(?P<u>\d+)", "string": "abc", "named": True,
        }}) == {}


# ─────────────────────────────────────────────────────────────────────────────
# Text DSL translation
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectionDSL:
    def test_val1_functions(self):
        assert one("/r = len([1,2,3])") == {"$len": [1, 2, 3]}
        assert one("/r = keys($(/d))") == {"$keys": {"$ref": "/d"}}
        assert one("/r = values($(/d))") == {"$values": {"$ref": "/d"}}
        assert one("/r = items($(/d))") == {"$items": {"$ref": "/d"}}
        assert one("/r = reverse([1,2])") == {"$reverse": [1, 2]}
        assert one("/r = type(42)") == {"$type": 42}
        assert one("/r = sum([1,2])") == {"$sum": [1, 2]}
        assert one("/r = avg([1,2])") == {"$avg": [1, 2]}
        assert one("/r = abs(-5)") == {"$abs": -5}
        assert one("/r = floor(3.7)") == {"$floor": 3.7}
        assert one("/r = ceil(3.2)") == {"$ceil": 3.2}

    def test_sort_simple(self):
        assert one("/r = sort([3,1,2])") == {"$sort": [3, 1, 2]}

    def test_sort_key_reverse(self):
        assert one('/r = sort([3,1,2], key: "/n", reverse: true)') == {
            "$sort": {"array": [3, 1, 2], "key": "/n", "reverse": True},
        }

    def test_min_max_unique_key(self):
        assert one('/r = min([1], key: "/n")') == {"$min": {"array": [1], "key": "/n"}}
        assert one("/r = max([1])") == {"$max": [1]}
        assert one('/r = unique([1,1], key: "/id")') == {"$unique": {"array": [1, 1], "key": "/id"}}

    def test_flatten(self):
        assert one("/r = flatten([[1],[2]])") == {"$flatten": [[1], [2]]}
        assert one("/r = flatten([[1]], depth: 2)") == {"$flatten": {"array": [[1]], "depth": 2}}

    def test_lslice(self):
        assert one("/r = lslice([1,2,3,4])") == {"$slice": {"array": [1, 2, 3, 4]}}
        assert one("/r = lslice([1,2,3,4], 1)") == {"$slice": {"array": [1, 2, 3, 4], "start": 1}}
        assert one("/r = lslice([1,2,3,4], 1, 3)") == {
            "$slice": {"array": [1, 2, 3, 4], "start": 1, "end": 3},
        }
        assert one("/r = lslice([1,2,3,4,5], 0, 5, 2)") == {
            "$slice": {"array": [1, 2, 3, 4, 5], "start": 0, "end": 5, "step": 2},
        }

    def test_map(self):
        assert one('/r = map([1,2], $(&:/n), as: "n")') == {
            "$map": {"in": [1, 2], "expr": {"$ref": "&:/n"}, "as": "n"},
        }

    def test_map_default_var(self):
        assert one("/r = map([1,2], $(&:/item))") == {
            "$map": {"in": [1, 2], "expr": {"$ref": "&:/item"}},
        }

    def test_filter(self):
        assert one('/r = filter([1,2], $(&:/n) > 1, as: "n")') == {
            "$filter": {"in": [1, 2], "cond": {"$gt": [{"$ref": "&:/n"}, 1]}, "as": "n"},
        }

    def test_filter_default_var(self):
        assert one("/r = filter([1,2], $(&:/item) > 1)") == {
            "$filter": {"in": [1, 2], "cond": {"$gt": [{"$ref": "&:/item"}, 1]}},
        }

    def test_regex_groups_named(self):
        assert one('/r = regex_groups("(?P<u>a)", "a", named: true)') == {
            "$regex_groups": {"pattern": "(?P<u>a)", "string": "a", "named": True},
        }

    def test_dsl_end_to_end(self):
        engine = build_default_engine()
        spec = parse_text('/r = map([1,2,3], $(&:/n) * 2, as: "n")')
        assert engine.apply(spec=spec, source={}, dest={}) == {"r": [2, 4, 6]}
