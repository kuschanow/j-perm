"""Tests for template substitution handlers."""

import pytest
from j_perm import (
    TemplMatcher,
    TemplSubstHandler,
    template_unescape,
    build_default_engine,
)


class TestTemplMatcher:
    """Test TemplMatcher."""

    def test_matches_strings_with_placeholders(self):
        """TemplMatcher matches strings with ${...}."""
        matcher = TemplMatcher()

        assert matcher.matches("${/path}") is True
        assert matcher.matches("prefix ${/x} suffix") is True
        assert matcher.matches("${int:/age}") is True

    def test_rejects_strings_without_placeholders(self):
        """TemplMatcher rejects plain strings."""
        matcher = TemplMatcher()

        assert matcher.matches("plain text") is False
        assert matcher.matches("") is False
        assert matcher.matches("/path") is False

    def test_rejects_escaped_placeholders(self):
        """TemplMatcher rejects escaped $${...}."""
        matcher = TemplMatcher()

        assert matcher.matches("$${escaped}") is False
        assert matcher.matches("$$not a placeholder") is False

    def test_rejects_non_strings(self):
        """TemplMatcher rejects non-string values."""
        matcher = TemplMatcher()

        assert matcher.matches(123) is False
        assert matcher.matches(None) is False
        assert matcher.matches({"key": "value"}) is False
        assert matcher.matches([1, 2, 3]) is False


class TestTemplSubstHandler:
    """Test TemplSubstHandler."""

    def test_simple_pointer_substitution(self):
        """Substitute simple JSON pointer."""
        engine = build_default_engine()
        handler = TemplSubstHandler()

        from j_perm import ExecutionContext, PointerResolver

        ctx = ExecutionContext(
            source={"user": {"name": "Alice"}},
            dest={},
            engine=engine,
        )

        result = handler.execute("${/user/name}", ctx)
        assert result == "Alice"

    def test_caster_int(self):
        """Caster prefix int:."""
        engine = build_default_engine()
        handler = TemplSubstHandler()

        from j_perm import ExecutionContext, PointerResolver

        ctx = ExecutionContext(
            source={"age": "30"},
            dest={},
            engine=engine,
        )

        # Note: single expression returns native type
        result = handler.execute("${int:/age}", ctx)
        assert result == 30  # Native type (int)

    def test_multiple_placeholders(self):
        """Multiple ${...} in one string."""
        engine = build_default_engine()
        handler = TemplSubstHandler()

        from j_perm import ExecutionContext, PointerResolver

        ctx = ExecutionContext(
            source={"first": "Alice", "last": "Smith"},
            dest={},
            engine=engine,
        )

        result = handler.execute("${/first} ${/last}", ctx)
        assert result == "Alice Smith"

    def test_escaped_placeholders_preserved(self):
        """Escaped $${...} should be preserved."""
        engine = build_default_engine()
        handler = TemplSubstHandler()

        from j_perm import ExecutionContext, PointerResolver

        ctx = ExecutionContext(source={}, dest={}, engine=engine)

        result = handler.execute("$${literal}", ctx)
        assert result == "$${literal}"


class TestTemplateUnescape:
    """Test template_unescape function."""

    def test_unescape_double_dollar_brace(self):
        """$${ → ${."""
        assert template_unescape("$${test}") == "${test}"
        assert template_unescape("prefix $${x} suffix") == "prefix ${x} suffix"

    def test_unescape_double_dollar(self):
        """$$ → $."""
        assert template_unescape("$$test") == "$test"
        assert template_unescape("price: $$100") == "price: $100"

    def test_unescape_in_lists(self):
        """Recursively unescape lists."""
        result = template_unescape(["$${a}", "$$b"])
        assert result == ["${a}", "$b"]

    def test_unescape_in_dicts(self):
        """Recursively unescape dicts."""
        result = template_unescape({"key": "$${val}", "k2": "$$v2"})
        assert result == {"key": "${val}", "k2": "$v2"}

    def test_unescape_dict_keys(self):
        """Unescape dict keys too."""
        result = template_unescape({"$${key}": "value"})
        assert result == {"${key}": "value"}

    def test_unescape_non_strings_unchanged(self):
        """Non-strings pass through."""
        assert template_unescape(123) == 123
        assert template_unescape(None) is None
        assert template_unescape(True) is True


class TestTemplateHandlerEdgeCases:
    """Additional edge case tests for template handler."""

    def test_non_string_value_passthrough_direct(self):
        """TemplSubstHandler.execute returns non-string values unchanged (line 148)."""
        from j_perm import TemplSubstHandler

        handler = TemplSubstHandler()
        result = handler.execute(42, None)
        assert result == 42

    def test_non_string_value_passthrough_dict(self):
        """TemplSubstHandler.execute returns dict values unchanged (line 148)."""
        from j_perm import TemplSubstHandler

        handler = TemplSubstHandler()
        result = handler.execute({"key": "val"}, None)
        assert result == {"key": "val"}

    def test_unclosed_brace_treated_as_literal(self):
        """Unclosed '${' is treated as literal text (lines 222-223)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": "hello ${unclosed"},
            source={},
            dest={},
        )

        assert result["result"] == "hello ${unclosed"

    def test_single_expression_loop_exhausted_returns_false(self):
        """_is_single_expression returns False when outer brace consumed by inner (line 177)."""
        engine = build_default_engine()

        # "${a${/x}" starts+ends with "${"/"}": inner ${/x} consumes the only "}"
        # _is_single_expression loops through and returns False at line 177
        result = engine.apply(
            {"/result": "${a${/x}"},
            source={"x": "!"},
            dest={},
        )

        # The template is processed via _flat_substitute since _is_single_expression=False
        assert isinstance(result["result"], str)

    def test_dollar_dollar_escape_in_flat_substitute(self):
        """'$$' in flat template is kept as '$$' before unescape (lines 193-195)."""
        engine = build_default_engine()

        # "${/v}$$rest" — after substitution gives "hello$$rest",
        # then unescape converts $$ → $
        result = engine.apply(
            {"/result": "${/v}$$rest"},
            source={"v": "hello"},
            dest={},
        )

        assert result["result"] == "hello$rest"

    def test_single_expression_not_matched_when_extra_text_after(self):
        """_is_single_expression falls through when extra text follows (via flat_substitute)."""
        engine = build_default_engine()

        # "${/val}extra" is NOT a single expression (ends with "a" not "}")
        result = engine.apply(
            {"/result": "${/val}extra"},
            source={"val": "hello"},
            dest={},
        )

        assert result["result"] == "helloextra"

    def test_template_with_dict_value_renders_as_json(self):
        """Dict/list value in flat template is JSON-serialized (line 211)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": "data: ${/obj}"},
            source={"obj": {"key": "value"}},
            dest={},
        )

        import json
        assert json.loads(result["result"][len("data: "):]) == {"key": "value"}

    def test_nested_template_depth_tracking(self):
        """Nested '${' inside flat template increments depth correctly (lines 204, 218)."""
        engine = build_default_engine()

        # "prefix ${foo${/sep}bar} suffix" - the inner ${/sep} triggers depth++ (204),
        # its closing } triggers depth-- (218), then outer } terminates the outer expression
        result = engine.apply(
            {"/result": "prefix ${foo${/sep}bar} suffix"},
            source={"sep": "_"},
            dest={},
        )

        assert result["result"] == "prefix foo_bar suffix"


class TestTemplateUnescapeEdgeCases:
    """Additional edge case tests for template_unescape."""

    def test_tuple_unescape(self):
        """template_unescape processes tuples (line 285)."""
        from j_perm import template_unescape

        result = template_unescape(("$$hello", "$$world"))
        assert result == ("$hello", "$world")
        assert isinstance(result, tuple)
