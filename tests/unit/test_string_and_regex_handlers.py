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

class TestStringValidationErrors:
    """Test validation errors in string operation handlers."""

    def test_str_split_string_arg_raises(self):
        """$str_split raises when given a string instead of dict (line 757)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a dict"):
            engine.apply({"/r": {"$str_split": "bad"}}, source={}, dest={})

    def test_str_split_non_string_value_raises(self):
        """$str_split raises when 'string' is not a string (line 764)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_split": {"string": 42, "delimiter": ","}}},
                source={},
                dest={},
            )

    def test_str_join_string_arg_raises(self):
        """$str_join raises when given a string instead of dict (line 820)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a dict"):
            engine.apply({"/r": {"$str_join": "bad"}}, source={}, dest={})

    def test_str_join_non_list_array_raises(self):
        """$str_join raises when 'array' is not a list (line 826)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a list"):
            engine.apply(
                {"/r": {"$str_join": {"array": "not_a_list", "separator": ","}}},
                source={},
                dest={},
            )

    def test_str_join_empty_array(self):
        """$str_join returns empty string for empty array (line 843)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/r": {"$str_join": {"array": [], "separator": ","}}},
            source={},
            dest={},
        )

        assert result == {"r": ""}

    def test_str_slice_string_arg_raises(self):
        """$str_slice raises when given a string instead of dict (line 876)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a dict"):
            engine.apply({"/r": {"$str_slice": "bad"}}, source={}, dest={})

    def test_str_slice_non_string_raises(self):
        """$str_slice raises when 'string' is not a string (line 883)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_slice": {"string": 42, "start": 0}}},
                source={},
                dest={},
            )

    def test_str_upper_non_string_raises(self):
        """$str_upper raises when given non-string (line 903)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a string"):
            engine.apply({"/r": {"$str_upper": 42}}, source={}, dest={})

    def test_str_lower_non_string_raises(self):
        """$str_lower raises when given non-string (line 923)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a string"):
            engine.apply({"/r": {"$str_lower": 42}}, source={}, dest={})

    def test_str_strip_simple_non_string_raises(self):
        """$str_strip (simple form) raises when template resolves to non-string (line 952)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a string"):
            engine.apply(
                {"/r": {"$str_strip": "${/num}"}},
                source={"num": 42},
                dest={},
            )

    def test_str_strip_dict_non_string_raises(self):
        """$str_strip (dict form) raises when 'string' is not a string (line 960)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_strip": {"string": 42, "chars": " "}}},
                source={},
                dest={},
            )

    def test_str_lstrip_non_string_raises(self):
        """$str_lstrip (simple form) raises when template resolves to non-string (line 983)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a string"):
            engine.apply(
                {"/r": {"$str_lstrip": "${/num}"}},
                source={"num": 42},
                dest={},
            )

    def test_str_lstrip_dict_non_string_raises(self):
        """$str_lstrip (dict form) raises when 'string' is not a string (line 990)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_lstrip": {"string": 42, "chars": " "}}},
                source={},
                dest={},
            )

    def test_str_rstrip_non_string_raises(self):
        """$str_rstrip (simple form) raises when template resolves to non-string (line 1013)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a string"):
            engine.apply(
                {"/r": {"$str_rstrip": "${/num}"}},
                source={"num": 42},
                dest={},
            )

    def test_str_rstrip_dict_non_string_raises(self):
        """$str_rstrip (dict form) raises when 'string' is not a string (line 1020)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_rstrip": {"string": 42, "chars": " "}}},
                source={},
                dest={},
            )

    def test_str_replace_non_string_raises(self):
        """$str_replace raises when 'string' is not a string (line 1063)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_replace": {"string": 42, "old": "a", "new": "b"}}},
                source={},
                dest={},
            )

    def test_str_contains_non_string_raises(self):
        """$str_contains raises when 'string' is not a string (line 1108)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_contains": {"string": 42, "substring": "x"}}},
                source={},
                dest={},
            )

    def test_str_startswith_non_string_raises(self):
        """$str_startswith raises when 'string' is not a string (line 1131)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_startswith": {"string": 42, "prefix": "x"}}},
                source={},
                dest={},
            )

    def test_str_endswith_non_string_raises(self):
        """$str_endswith raises when 'string' is not a string (line 1154)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$str_endswith": {"string": 42, "suffix": "x"}}},
                source={},
                dest={},
            )


class TestRegexValidationErrors:
    """Test validation errors in regex handlers."""

    def test_regex_match_non_string_raises(self):
        """$regex_match raises when 'string' is not a string (line 1212)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$regex_match": {"pattern": r"\d+", "string": 42}}},
                source={},
                dest={},
            )

    def test_regex_match_disallowed_flags_raises(self):
        """$regex_match raises when flags contain disallowed bits (lines 1223-1224)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=re_mod.IGNORECASE)

        with pytest.raises(ValueError, match="disallowed flags"):
            engine.apply(
                {"/r": {"$regex_match": {"pattern": r"\d+", "string": "123", "flags": re_mod.MULTILINE}}},
                source={},
                dest={},
            )

    def test_regex_search_non_string_raises(self):
        """$regex_search raises when 'string' is not a string (line 1273)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$regex_search": {"pattern": r"\d+", "string": 42}}},
                source={},
                dest={},
            )

    def test_regex_search_disallowed_flags_raises(self):
        """$regex_search raises when flags disallowed (lines 1276, 1284-1285)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=re_mod.IGNORECASE)

        with pytest.raises(ValueError, match="disallowed flags"):
            engine.apply(
                {"/r": {"$regex_search": {"pattern": r"\d+", "string": "123", "flags": re_mod.MULTILINE}}},
                source={},
                dest={},
            )

    def test_regex_findall_non_string_raises(self):
        """$regex_findall raises when 'string' is not a string (line 1333)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$regex_findall": {"pattern": r"\d+", "string": 42}}},
                source={},
                dest={},
            )

    def test_regex_findall_disallowed_flags_raises(self):
        """$regex_findall raises when flags disallowed (lines 1336, 1343-1344)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=re_mod.IGNORECASE)

        with pytest.raises(ValueError, match="disallowed flags"):
            engine.apply(
                {"/r": {"$regex_findall": {"pattern": r"\d+", "string": "123", "flags": re_mod.MULTILINE}}},
                source={},
                dest={},
            )

    def test_regex_replace_non_string_raises(self):
        """$regex_replace raises when 'string' is not a string (line 1397)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$regex_replace": {"pattern": r"\d+", "replacement": "X", "string": 42}}},
                source={},
                dest={},
            )

    def test_regex_replace_disallowed_flags_raises(self):
        """$regex_replace raises when flags disallowed (lines 1400, 1407-1408)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=re_mod.IGNORECASE)

        with pytest.raises(ValueError, match="disallowed flags"):
            engine.apply(
                {"/r": {"$regex_replace": {"pattern": r"\d+", "replacement": "X", "string": "123", "flags": re_mod.MULTILINE}}},
                source={},
                dest={},
            )

    def test_regex_groups_non_string_raises(self):
        """$regex_groups raises when 'string' is not a string (line 1458)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="must be a string"):
            engine.apply(
                {"/r": {"$regex_groups": {"pattern": r"(\d+)", "string": 42}}},
                source={},
                dest={},
            )

    def test_regex_groups_disallowed_flags_raises(self):
        """$regex_groups raises when flags disallowed (lines 1461, 1469-1470)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=re_mod.IGNORECASE)

        with pytest.raises(ValueError, match="disallowed flags"):
            engine.apply(
                {"/r": {"$regex_groups": {"pattern": r"(\d+)", "string": "123", "flags": re_mod.MULTILINE}}},
                source={},
                dest={},
            )


class TestRegexTimeoutAndAllowAllFlags:
    """Test regex timeout errors and allowed_flags=-1 (allow all)."""

    def test_regex_match_allow_all_flags(self):
        """make_regex_match_handler with allowed_flags=-1 allows all flags (line 1184)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=-1)

        result = engine.apply(
            {"/r": {"$regex_match": {"pattern": r"hello", "string": "HELLO", "flags": re_mod.IGNORECASE}}},
            source={},
            dest={},
        )
        assert result == {"r": True}

    def test_regex_match_timeout(self):
        """$regex_match raises TimeoutError on catastrophic backtracking (lines 1223-1224)."""
        engine = build_default_engine(regex_timeout=0.0001)

        with pytest.raises(TimeoutError):
            engine.apply(
                {"/r": {"$regex_match": {"pattern": r"(a|aa)+$", "string": "a" * 20 + "!"}}},
                source={},
                dest={},
            )

    def test_regex_search_allow_all_flags(self):
        """make_regex_search_handler with allowed_flags=-1 allows all flags (line 1247)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=-1)

        result = engine.apply(
            {"/r": {"$regex_search": {"pattern": r"\d+", "string": "abc123", "flags": re_mod.UNICODE}}},
            source={},
            dest={},
        )
        assert result == {"r": "123"}

    def test_regex_search_timeout(self):
        """$regex_search raises TimeoutError on catastrophic backtracking (lines 1284-1285)."""
        engine = build_default_engine(regex_timeout=0.0001)

        with pytest.raises(TimeoutError):
            engine.apply(
                {"/r": {"$regex_search": {"pattern": r"(a+)+b", "string": "a" * 30 + "c"}}},
                source={},
                dest={},
            )

    def test_regex_findall_allow_all_flags(self):
        """make_regex_findall_handler with allowed_flags=-1 (line 1307)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=-1)

        result = engine.apply(
            {"/r": {"$regex_findall": {"pattern": r"\d+", "string": "a1b2c3", "flags": re_mod.UNICODE}}},
            source={},
            dest={},
        )
        assert result == {"r": ["1", "2", "3"]}

    def test_regex_findall_timeout(self):
        """$regex_findall raises TimeoutError (lines 1343-1344)."""
        engine = build_default_engine(regex_timeout=0.0001)

        with pytest.raises(TimeoutError):
            engine.apply(
                {"/r": {"$regex_findall": {"pattern": r"(a+)+b", "string": "a" * 30 + "c"}}},
                source={},
                dest={},
            )

    def test_regex_replace_allow_all_flags(self):
        """make_regex_replace_handler with allowed_flags=-1 (line 1366)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=-1)

        result = engine.apply(
            {"/r": {"$regex_replace": {"pattern": r"\d+", "replacement": "X", "string": "a1b2c3", "flags": re_mod.UNICODE}}},
            source={},
            dest={},
        )
        assert result == {"r": "aXbXcX"}

    def test_regex_replace_timeout(self):
        """$regex_replace raises TimeoutError (lines 1407-1408)."""
        engine = build_default_engine(regex_timeout=0.0001)

        with pytest.raises(TimeoutError):
            engine.apply(
                {"/r": {"$regex_replace": {"pattern": r"(a+)+b", "replacement": "X", "string": "a" * 30 + "c"}}},
                source={},
                dest={},
            )

    def test_regex_groups_allow_all_flags(self):
        """make_regex_groups_handler with allowed_flags=-1 (line 1430)."""
        import re as re_mod
        engine = build_default_engine(regex_allowed_flags=-1)

        result = engine.apply(
            {"/r": {"$regex_groups": {"pattern": r"(\w+)@(\w+)", "string": "user@host", "flags": re_mod.UNICODE}}},
            source={},
            dest={},
        )
        assert result == {"r": ["user", "host"]}

    def test_regex_groups_timeout(self):
        """$regex_groups raises TimeoutError (lines 1469-1470)."""
        engine = build_default_engine(regex_timeout=0.0001)

        with pytest.raises(TimeoutError):
            engine.apply(
                {"/r": {"$regex_groups": {"pattern": r"(a+)+b", "string": "a" * 30 + "c"}}},
                source={},
                dest={},
            )
