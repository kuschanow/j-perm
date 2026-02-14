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
