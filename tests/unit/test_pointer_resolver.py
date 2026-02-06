"""Tests for PointerResolver."""

import pytest
from j_perm import PointerResolver


class TestPointerResolverGet:
    """Test get() method."""

    def test_root_references_on_scalars(self):
        """Root references should work on scalars."""
        resolver = PointerResolver()

        assert resolver.get(".", 42) == 42
        assert resolver.get("/", 42) == 42
        assert resolver.get("", 42) == 42

        assert resolver.get(".", "text") == "text"
        assert resolver.get("/", None) is None
        assert resolver.get(".", [1, 2, 3]) == [1, 2, 3]

    def test_root_references_on_dicts(self):
        """Root references should return whole dict."""
        resolver = PointerResolver()
        data = {"a": 1}

        assert resolver.get(".", data) == {"a": 1}
        assert resolver.get("/", data) == {"a": 1}
        assert resolver.get("", data) == {"a": 1}

    def test_simple_dict_access(self):
        """Basic dictionary key access."""
        resolver = PointerResolver()
        data = {"a": {"b": {"c": 42}}}

        assert resolver.get("/a", data) == {"b": {"c": 42}}
        assert resolver.get("/a/b", data) == {"c": 42}
        assert resolver.get("/a/b/c", data) == 42

    def test_list_access(self):
        """Array index access."""
        resolver = PointerResolver()
        data = {"arr": [10, 20, 30]}

        assert resolver.get("/arr/0", data) == 10
        assert resolver.get("/arr/1", data) == 20
        assert resolver.get("/arr/2", data) == 30

    def test_slices(self):
        """Array slicing."""
        resolver = PointerResolver()
        data = {"arr": [1, 2, 3, 4, 5]}

        assert resolver.get("/arr[1:3]", data) == [2, 3]
        assert resolver.get("/arr[2:]", data) == [3, 4, 5]
        assert resolver.get("/arr[:2]", data) == [1, 2]
        assert resolver.get("/arr[:-1]", data) == [1, 2, 3, 4]

    def test_parent_navigation(self):
        """.. parent reference."""
        resolver = PointerResolver()
        data = {"a": {"b": {"c": 1}, "d": 2}}

        # /a/b/c/../d should resolve to /a/b/../d -> /a/d
        assert resolver.get("/a/b/../d", data) == 2

    def test_escape_sequences(self):
        """RFC 6901 escape sequences."""
        resolver = PointerResolver()
        data = {"a/b": 1, "c~d": 2, "e$f": 3, "g.h": 4}

        assert resolver.get("/a~1b", data) == 1
        assert resolver.get("/c~0d", data) == 2
        assert resolver.get("/e~2f", data) == 3
        assert resolver.get("/g~3h", data) == 4

    def test_missing_key_raises(self):
        """Missing keys should raise KeyError."""
        resolver = PointerResolver()
        data = {"a": 1}

        with pytest.raises(KeyError):
            resolver.get("/missing", data)

    def test_missing_index_raises(self):
        """Out of bounds index should raise IndexError."""
        resolver = PointerResolver()
        data = {"arr": [1, 2]}

        with pytest.raises(IndexError):
            resolver.get("/arr/10", data)


class TestPointerResolverSet:
    """Test set() method."""

    def test_root_replacement(self):
        """Root references should replace entire value."""
        resolver = PointerResolver()

        assert resolver.set(".", 123, "new") == "new"
        assert resolver.set("/", {"old": 1}, "new") == "new"
        assert resolver.set("", [1, 2], "new") == "new"

    def test_simple_set(self):
        """Basic key assignment."""
        resolver = PointerResolver()
        data = {}

        result = resolver.set("/key", data, "value")
        assert result == {"key": "value"}
        assert data == {"key": "value"}  # mutates in place

    def test_nested_set_with_create(self):
        """Auto-create intermediate nodes."""
        resolver = PointerResolver()
        data = {}

        # Should create {"a": {"b": {"c": 42}}}
        result = resolver.set("/a/b/c", data, 42)
        assert result["a"]["b"]["c"] == 42

    def test_list_append_with_dash(self):
        """Append using '/-' notation."""
        resolver = PointerResolver()
        data = {"items": [1, 2]}

        resolver.set("/items/-", data, 3)
        assert data == {"items": [1, 2, 3]}

    def test_list_append_raises_on_non_list(self):
        """'/-' on non-list should raise."""
        resolver = PointerResolver()
        data = {"items": "not a list"}

        with pytest.raises(TypeError, match="not a list"):
            resolver.set("/items/-", data, 3)

    def test_list_auto_grow(self):
        """Setting past end of list should extend with None."""
        resolver = PointerResolver()
        data = {"arr": [1, 2]}

        resolver.set("/arr/5", data, "x")
        assert data["arr"] == [1, 2, None, None, None, "x"]


class TestPointerResolverDelete:
    """Test delete() method."""

    def test_delete_dict_key(self):
        """Delete dictionary key."""
        resolver = PointerResolver()
        data = {"a": 1, "b": 2, "c": 3}

        resolver.delete("/b", data)
        assert data == {"a": 1, "c": 3}

    def test_delete_list_item(self):
        """Delete list item by index."""
        resolver = PointerResolver()
        data = {"arr": [10, 20, 30]}

        resolver.delete("/arr/1", data)
        assert data == {"arr": [10, 30]}

    def test_delete_nested(self):
        """Delete nested key."""
        resolver = PointerResolver()
        data = {"a": {"b": {"c": 1}}}

        resolver.delete("/a/b/c", data)
        assert data == {"a": {"b": {}}}

    def test_delete_missing_raises(self):
        """Deleting missing key should raise."""
        resolver = PointerResolver()
        data = {"a": 1}

        with pytest.raises(KeyError):
            resolver.delete("/missing", data)


class TestPointerResolverExists:
    """Test exists() method."""

    def test_exists_true(self):
        """exists() returns True for existing paths."""
        resolver = PointerResolver()
        data = {"a": {"b": 1}}

        assert resolver.exists("/a", data) is True
        assert resolver.exists("/a/b", data) is True

    def test_exists_false(self):
        """exists() returns False for missing paths."""
        resolver = PointerResolver()
        data = {"a": 1}

        assert resolver.exists("/missing", data) is False
        assert resolver.exists("/a/missing", data) is False
