"""Tests for shorthand expansion stages."""

import pytest
from j_perm import build_default_engine


class TestAssertShorthand:
    """Test ~assert / ~assertD shorthand."""

    def test_assert_shorthand_dict(self):
        """~assert with dict value."""
        engine = build_default_engine()

        result = engine.apply(
            {"~assert": {"/x": 10, "/y": 20}},
            source={"x": 10, "y": 20},
            dest={},
        )

        assert result == {}  # no changes, just assertions

    def test_assert_shorthand_list(self):
        """~assert with list value."""
        engine = build_default_engine()

        result = engine.apply(
            {"~assert": ["/x", "/y"]},
            source={"x": 1, "y": 2},
            dest={},
        )

        assert result == {}

    def test_assert_shorthand_scalar(self):
        """~assert with single path."""
        engine = build_default_engine()

        result = engine.apply(
            {"~assert": "/required"},
            source={"required": "value"},
            dest={},
        )

        assert result == {}

    def test_assertd_shorthand(self):
        """~assertD checks destination."""
        engine = build_default_engine()

        result = engine.apply(
            {"~assertD": "/check"},
            source={},
            dest={"check": "value"},
        )

        assert result == {"check": "value"}


class TestDeleteShorthand:
    """Test ~delete shorthand."""

    def test_delete_shorthand_single(self):
        """~delete with single path."""
        engine = build_default_engine()

        result = engine.apply(
            {"~delete": "/remove"},
            source={},
            dest={"keep": 1, "remove": 2},
        )

        assert result == {"keep": 1}

    def test_delete_shorthand_list(self):
        """~delete with multiple paths."""
        engine = build_default_engine()

        result = engine.apply(
            {"~delete": ["/a", "/b"]},
            source={},
            dest={"a": 1, "b": 2, "c": 3},
        )

        assert result == {"c": 3}


class TestAppendShorthand:
    """Test /path[] append shorthand."""

    def test_append_shorthand(self):
        """field[] appends to list."""
        engine = build_default_engine()

        result = engine.apply(
            {"/items[]": "new"},
            source={},
            dest={"items": [1, 2]},
        )

        assert result == {"items": [1, 2, "new"]}

    def test_append_creates_list(self):
        """field[] creates list if doesn't exist."""
        engine = build_default_engine()

        result = engine.apply(
            {"/items[]": "first"},
            source={},
            dest={},
        )

        assert result == {"items": ["first"]}


class TestPointerAssignmentShorthand:
    """Test pointer assignment shorthand."""

    def test_pointer_assignment(self):
        """'/dest': '/source' copies from source."""
        engine = build_default_engine()

        result = engine.apply(
            {"/name": "/user/name"},
            source={"user": {"name": "Alice"}},
            dest={},
        )

        assert result == {"name": "Alice"}

    def test_pointer_assignment_ignores_missing(self):
        """Pointer assignment has ignore_missing=True."""
        engine = build_default_engine()

        result = engine.apply(
            {"/name": "/missing"},
            source={},
            dest={},
        )

        # Should not raise, just skip
        assert result == {}


class TestLiteralAssignmentShorthand:
    """Test literal value assignment shorthand."""

    def test_literal_assignment(self):
        """'/path': value sets literal value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/status": "active", "/count": 42},
            source={},
            dest={},
        )

        assert result == {"status": "active", "count": 42}


class TestMixedShorthands:
    """Test multiple shorthands in one dict."""

    def test_mixed_shorthands_priority_order(self):
        """Shorthands execute in priority order: assert > delete > assign."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "~assert": "/check",  # First (priority 100)
                "~delete": "/tmp",  # Second (priority 50)
                "/result": "value",  # Third (priority 0)
            },
            source={"check": "ok"},
            dest={"tmp": "junk"},
        )

        assert result == {"result": "value"}

    def test_all_shorthands_together(self):
        """All shorthand types in one spec."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "~assertD": "/existing",
                "~delete": "/remove",
                "/copy_this": "/source_value",
                "/items[]": "appended",
                "/literal": 123,
            },
            source={"source_value": "copied"},
            dest={"existing": "yes", "remove": "gone", "items": [1, 2]},
        )

        assert result == {
            "existing": "yes",
            "copy_this": "copied",
            "items": [1, 2, "appended"],
            "literal": 123,
        }
