"""Tests for all operation handlers."""

import pytest
from j_perm import build_default_engine


class TestSetOperation:
    """Test 'set' operation."""

    def test_set_simple_value(self):
        """Set a simple value."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/name", "value": "Alice"},
            source={},
            dest={},
        )

        assert result == {"name": "Alice"}

    def test_set_nested_with_autocreate(self):
        """Auto-create intermediate nodes."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/a/b/c", "value": 42},
            source={},
            dest={},
        )

        assert result == {"a": {"b": {"c": 42}}}

    def test_set_append_with_dash(self):
        """Append to list using '/-'."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/items/-", "value": "new"},
            source={},
            dest={"items": [1, 2]},
        )

        assert result == {"items": [1, 2, "new"]}

    def test_set_append_creates_list(self):
        """Append creates list if doesn't exist."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/items/-", "value": "first"},
            source={},
            dest={},
        )

        assert result == {"items": ["first"]}


class TestCopyOperation:
    """Test 'copy' operation."""

    def test_copy_from_source(self):
        """Copy value from source."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "copy", "from": "/user/name", "path": "/name"},
            source={"user": {"name": "Alice"}},
            dest={},
        )

        assert result == {"name": "Alice"}

    def test_copy_with_default(self):
        """Use default if source missing."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "copy", "from": "/missing", "path": "/name", "default": "Unknown"},
            source={},
            dest={},
        )

        assert result == {"name": "Unknown"}

    def test_copy_ignore_missing(self):
        """Ignore missing source path."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "copy", "from": "/missing", "path": "/name", "ignore_missing": True},
            source={},
            dest={"existing": "data"},
        )

        assert result == {"existing": "data"}  # unchanged

    def test_copy_raises_on_missing_without_default(self):
        """Raise if source missing and no default."""
        engine = build_default_engine()

        with pytest.raises(Exception):  # KeyError or similar
            engine.apply(
                {
                    "op": "copy",
                    "from": "/missing",
                    "path": "/name",
                    "ignore_missing": False,
                },
                source={},
                dest={},
            )


class TestCopyDOperation:
    """Test 'copyD' operation."""

    def test_copyd_within_dest(self):
        """Copy within destination."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "copyD", "from": "/a", "path": "/b"},
            source={},
            dest={"a": "value"},
        )

        assert result == {"a": "value", "b": "value"}


class TestDeleteOperation:
    """Test 'delete' operation."""

    def test_delete_key(self):
        """Delete a key."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "delete", "path": "/remove"},
            source={},
            dest={"keep": 1, "remove": 2},
        )

        assert result == {"keep": 1}

    def test_delete_ignore_missing(self):
        """Ignore missing path (default behavior)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "delete", "path": "/missing"},
            source={},
            dest={"keep": 1},
        )

        assert result == {"keep": 1}  # no error


class TestForeachOperation:
    """Test 'foreach' operation."""

    def test_foreach_basic(self):
        """Iterate over array."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in": "/items", "do": {"/out[]": "/item"}},
            source={"items": [1, 2, 3]},
            dest={},
        )

        assert result == {"out": [1, 2, 3]}

    def test_foreach_with_custom_var(self):
        """Custom variable name."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in": "/items", "as": "x", "do": {"/out[]": "/x"}},
            source={"items": ["a", "b"]},
            dest={},
        )

        assert result == {"out": ["a", "b"]}

    def test_foreach_skip_empty(self):
        """Skip if array is empty."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in": "/items", "skip_empty": True, "do": {"/out[]": "/item"}},
            source={"items": []},
            dest={"pre": "existing"},
        )

        assert result == {"pre": "existing"}  # unchanged


class TestIfOperation:
    """Test 'if' operation."""

    def test_if_path_exists(self):
        """Execute 'then' if path exists."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "if", "path": "/check", "exists": True, "then": {"/result": "yes"}},
            source={},
            dest={"check": True},
        )

        assert result == {"check": True, "result": "yes"}

    def test_if_else_branch(self):
        """Execute 'else' if condition fails."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "if",
                "path": "/missing",
                "exists": True,
                "then": {"/result": "yes"},
                "else": {"/result": "no"},
            },
            source={},
            dest={},
        )

        assert result == {"result": "no"}


class TestExecOperation:
    """Test 'exec' operation."""

    def test_exec_inline_actions(self):
        """Execute inline actions."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "exec", "actions": {"/x": 1, "/y": 2}},
            source={},
            dest={},
        )

        assert result == {"x": 1, "y": 2}

    def test_exec_merge_mode(self):
        """Merge mode preserves dest."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "exec", "actions": {"/new": "value"}, "merge": True},
            source={},
            dest={"existing": "data"},
        )

        assert result == {"existing": "data", "new": "value"}


class TestUpdateOperation:
    """Test 'update' operation."""

    def test_update_mapping(self):
        """Update a dict with new keys."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "update", "path": "/obj", "value": {"b": 2}},
            source={},
            dest={"obj": {"a": 1}},
        )

        assert result == {"obj": {"a": 1, "b": 2}}

    def test_update_deep_merge(self):
        """Deep merge mode."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "update",
                "path": "/obj",
                "value": {"nested": {"b": 2}},
                "deep": True,
            },
            source={},
            dest={"obj": {"nested": {"a": 1}}},
        )

        assert result == {"obj": {"nested": {"a": 1, "b": 2}}}


class TestDistinctOperation:
    """Test 'distinct' operation."""

    def test_distinct_removes_duplicates(self):
        """Remove duplicate values."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "distinct", "path": "/arr"},
            source={},
            dest={"arr": [1, 2, 1, 3, 2]},
        )

        assert result == {"arr": [1, 2, 3]}

    def test_distinct_preserves_order(self):
        """Preserve first occurrence order."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "distinct", "path": "/arr"},
            source={},
            dest={"arr": [3, 1, 2, 1, 3]},
        )

        assert result == {"arr": [3, 1, 2]}


class TestReplaceRootOperation:
    """Test 'replace_root' operation."""

    def test_replace_root(self):
        """Replace entire destination."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "replace_root", "value": {"new": "root"}},
            source={},
            dest={"old": "data"},
        )

        assert result == {"new": "root"}


class TestAssertOperation:
    """Test 'assert' and 'assertD' operations."""

    def test_assert_exists_passes(self):
        """Assert passes if path exists in source."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "path": "/check"},
            source={"check": "value"},
            dest={},
        )

        assert result == {}  # no change

    def test_assert_fails_on_missing(self):
        """Assert fails if path missing."""
        engine = build_default_engine()

        with pytest.raises(AssertionError, match="does not exist"):
            engine.apply(
                {"op": "assert", "path": "/missing"},
                source={},
                dest={},
            )

    def test_assert_equals(self):
        """Assert with equals check."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "path": "/x", "equals": 10},
            source={"x": 10},
            dest={},
        )

        assert result == {}

    def test_assertd_checks_dest(self):
        """assertD checks destination not source."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assertD", "path": "/check"},
            source={},
            dest={"check": "value"},
        )

        assert result == {"check": "value"}
