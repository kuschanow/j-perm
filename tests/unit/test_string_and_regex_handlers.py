"""Tests for string and regex handlers."""

import pytest
from j_perm import build_default_engine


class TestStringOperations:
    """Test string operation constructs."""

    # --- $str_split ---
    def test_str_split_basic(self):
        """$str_split splits string by delimiter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_split": {"string": "a,b,c", "delimiter": ","}}},
            source={},
            dest={},
        )

        assert result == {"result": ["a", "b", "c"]}

    def test_str_split_with_maxsplit(self):
        """$str_split respects maxsplit parameter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_split": {"string": "a:b:c", "delimiter": ":", "maxsplit": 1}}},
            source={},
            dest={},
        )

        assert result == {"result": ["a", "b:c"]}

    # --- $str_join ---
    def test_str_join_basic(self):
        """$str_join joins list with separator."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_join": {"array": ["a", "b", "c"], "separator": "-"}}},
            source={},
            dest={},
        )

        assert result == {"result": "a-b-c"}

    def test_str_join_numbers(self):
        """$str_join converts numbers to strings."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_join": {"array": [1, 2, 3], "separator": ","}}},
            source={},
            dest={},
        )

        assert result == {"result": "1,2,3"}

    # --- $str_slice ---
    def test_str_slice_basic(self):
        """$str_slice extracts substring."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_slice": {"string": "hello", "start": 1, "end": 4}}},
            source={},
            dest={},
        )

        assert result == {"result": "ell"}

    def test_str_slice_from_start(self):
        """$str_slice from start index."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_slice": {"string": "hello", "start": 2}}},
            source={},
            dest={},
        )

        assert result == {"result": "llo"}

    def test_str_slice_to_end(self):
        """$str_slice to end index."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_slice": {"string": "hello", "end": 3}}},
            source={},
            dest={},
        )

        assert result == {"result": "hel"}

    def test_str_slice_negative_index(self):
        """$str_slice supports negative indices."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_slice": {"string": "hello", "start": -3}}},
            source={},
            dest={},
        )

        assert result == {"result": "llo"}

    # --- $str_upper / $str_lower ---
    def test_str_upper(self):
        """$str_upper converts to uppercase."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_upper": "hello"}},
            source={},
            dest={},
        )

        assert result == {"result": "HELLO"}

    def test_str_lower(self):
        """$str_lower converts to lowercase."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_lower": "HELLO"}},
            source={},
            dest={},
        )

        assert result == {"result": "hello"}

    # --- $str_strip ---
    def test_str_strip_whitespace(self):
        """$str_strip removes leading/trailing whitespace."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_strip": "  hello  "}},
            source={},
            dest={},
        )

        assert result == {"result": "hello"}

    def test_str_strip_chars(self):
        """$str_strip with chars parameter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_strip": {"string": "***hello***", "chars": "*"}}},
            source={},
            dest={},
        )

        assert result == {"result": "hello"}

    def test_str_strip_multiple_chars(self):
        """$str_strip with multiple chars."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_strip": {"string": "xyzabcxyz", "chars": "xyz"}}},
            source={},
            dest={},
        )

        assert result == {"result": "abc"}

    def test_str_lstrip(self):
        """$str_lstrip removes leading whitespace."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_lstrip": "  hello  "}},
            source={},
            dest={},
        )

        assert result == {"result": "hello  "}

    def test_str_lstrip_chars(self):
        """$str_lstrip with chars parameter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_lstrip": {"string": "___hello", "chars": "_"}}},
            source={},
            dest={},
        )

        assert result == {"result": "hello"}

    def test_str_rstrip(self):
        """$str_rstrip removes trailing whitespace."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_rstrip": "  hello  "}},
            source={},
            dest={},
        )

        assert result == {"result": "  hello"}

    def test_str_rstrip_chars(self):
        """$str_rstrip with chars parameter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_rstrip": {"string": "hello___", "chars": "_"}}},
            source={},
            dest={},
        )

        assert result == {"result": "hello"}

    # --- $str_replace ---
    def test_str_replace_basic(self):
        """$str_replace replaces substring."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_replace": {"string": "hello", "old": "ll", "new": "rr"}}},
            source={},
            dest={},
        )

        assert result == {"result": "herro"}

    def test_str_replace_with_count(self):
        """$str_replace with count parameter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_replace": {"string": "aaa", "old": "a", "new": "b", "count": 2}}},
            source={},
            dest={},
        )

        assert result == {"result": "bba"}

    # --- String checks ---
    def test_str_contains_true(self):
        """$str_contains returns True if substring found."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_contains": {"string": "hello world", "substring": "world"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_str_contains_false(self):
        """$str_contains returns False if substring not found."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_contains": {"string": "hello", "substring": "x"}}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_str_startswith_true(self):
        """$str_startswith returns True if string starts with prefix."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_startswith": {"string": "hello", "prefix": "he"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_str_startswith_false(self):
        """$str_startswith returns False if string doesn't start with prefix."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_startswith": {"string": "hello", "prefix": "x"}}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_str_endswith_true(self):
        """$str_endswith returns True if string ends with suffix."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_endswith": {"string": "hello", "suffix": "lo"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_str_endswith_false(self):
        """$str_endswith returns False if string doesn't end with suffix."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$str_endswith": {"string": "hello", "suffix": "x"}}},
            source={},
            dest={},
        )

        assert result == {"result": False}


class TestStringSliceInPointer:
    """Test string slicing via JSON Pointer (like array slicing)."""

    def test_pointer_slice_string_range(self):
        """JSON Pointer supports string slicing with [start:end]."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/text[0:5]"}},
            source={"text": "hello world"},
            dest={},
        )

        assert result == {"result": "hello"}

    def test_pointer_slice_string_from_start(self):
        """JSON Pointer supports string slicing from start index."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/text[6:]"}},
            source={"text": "hello world"},
            dest={},
        )

        assert result == {"result": "world"}

    def test_pointer_slice_string_to_end(self):
        """JSON Pointer supports string slicing to end index."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/text[:5]"}},
            source={"text": "hello world"},
            dest={},
        )

        assert result == {"result": "hello"}

    def test_pointer_slice_string_negative_index(self):
        """JSON Pointer supports negative indices in string slicing."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/text[-5:]"}},
            source={"text": "hello world"},
            dest={},
        )

        assert result == {"result": "world"}


class TestRegexOperations:
    """Test regex operation constructs."""

    def test_regex_match_true(self):
        """$regex_match returns True if pattern matches."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": "^\\d+$", "string": "123"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_regex_match_false(self):
        """$regex_match returns False if pattern doesn't match."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": "^\\d+$", "string": "abc"}}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_regex_search_found(self):
        """$regex_search returns matched string."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_search": {"pattern": "\\d+", "string": "abc123def"}}},
            source={},
            dest={},
        )

        assert result == {"result": "123"}

    def test_regex_search_not_found(self):
        """$regex_search returns None if not found."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_search": {"pattern": "\\d+", "string": "abc"}}},
            source={},
            dest={},
        )

        assert result == {"result": None}

    def test_regex_findall(self):
        """$regex_findall returns list of all matches."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_findall": {"pattern": "\\d+", "string": "a1b2c3"}}},
            source={},
            dest={},
        )

        assert result == {"result": ["1", "2", "3"]}

    def test_regex_findall_empty(self):
        """$regex_findall returns empty list if no matches."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_findall": {"pattern": "\\d+", "string": "abc"}}},
            source={},
            dest={},
        )

        assert result == {"result": []}

    def test_regex_replace(self):
        """$regex_replace replaces pattern matches."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_replace": {"pattern": "\\d+", "replacement": "X", "string": "a1b2c3"}}},
            source={},
            dest={},
        )

        assert result == {"result": "aXbXcX"}

    def test_regex_replace_with_backreference(self):
        """$regex_replace supports backreferences."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_replace": {"pattern": "(\\w+)@(\\w+)", "replacement": "\\1 AT \\2", "string": "user@domain"}}},
            source={},
            dest={},
        )

        assert result == {"result": "user AT domain"}

    def test_regex_replace_with_count(self):
        """$regex_replace with count parameter."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_replace": {"pattern": "\\d+", "replacement": "X", "string": "a1b2c3", "count": 2}}},
            source={},
            dest={},
        )

        assert result == {"result": "aXbXc3"}

    def test_regex_groups(self):
        """$regex_groups returns capture groups."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_groups": {"pattern": "(\\w+)@(\\w+)", "string": "user@domain"}}},
            source={},
            dest={},
        )

        assert result == {"result": ["user", "domain"]}

    def test_regex_groups_no_match(self):
        """$regex_groups returns empty list if no match."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_groups": {"pattern": "\\d+", "string": "abc"}}},
            source={},
            dest={},
        )

        assert result == {"result": []}