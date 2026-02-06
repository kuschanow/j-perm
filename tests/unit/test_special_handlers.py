"""Tests for special construct handlers ($ref, $eval)."""

import pytest
from j_perm import build_default_engine


class TestRefHandler:
    """Test $ref special construct."""

    def test_ref_simple(self):
        """$ref resolves pointer from source."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/data"}},
            source={"data": "value"},
            dest={},
        )

        assert result == {"result": "value"}

    def test_ref_nested(self):
        """$ref with nested path."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/user/name"}},
            source={"user": {"name": "Alice"}},
            dest={},
        )

        assert result == {"result": "Alice"}

    def test_ref_with_default(self):
        """$ref with $default fallback."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "/missing", "$default": "fallback"}},
            source={},
            dest={},
        )

        assert result == {"result": "fallback"}

    def test_ref_raises_without_default(self):
        """$ref raises if missing and no default."""
        engine = build_default_engine()

        with pytest.raises(Exception):  # KeyError or similar
            engine.apply(
                {"/result": {"$ref": "/missing"}},
                source={},
                dest={},
            )

    def test_ref_deep_copy(self):
        """$ref returns deep copy (no aliasing)."""
        engine = build_default_engine()

        source = {"data": {"mutable": "value"}}
        result = engine.apply(
            {"/result": {"$ref": "/data"}},
            source=source,
            dest={},
        )

        # Mutate result
        result["result"]["mutable"] = "changed"

        # Source should be unchanged
        assert source["data"]["mutable"] == "value"


class TestEvalHandler:
    """Test $eval special construct."""

    def test_eval_inline_actions(self):
        """$eval executes nested actions."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$eval": {"/x": 1, "/y": 2}}},
            source={},
            dest={},
        )

        assert result == {"result": {"x": 1, "y": 2}}

    def test_eval_with_select(self):
        """$eval with $select extracts sub-path."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$eval": {"/x": 1, "/y": 2}, "$select": "/x"}},
            source={},
            dest={},
        )

        assert result == {"result": 1}

    def test_eval_accesses_source(self):
        """$eval nested actions can access source."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$eval": {"/copied": "/data"}}},
            source={"data": "value"},
            dest={},
        )

        assert result == {"result": {"copied": "value"}}

    def test_eval_starts_with_empty_dest(self):
        """$eval starts with empty dest={}."""
        engine = build_default_engine()

        # $eval executes in isolated context with dest={}
        # The outer dest should not be visible inside $eval
        result = engine.apply(
            {"/result": {"$eval": [
                {"op": "set", "path": "/x", "value": 1},
                # If $eval had access to outer dest, /pre would exist
                # This is a negative test: we expect it NOT to exist
            ]}},
            source={},
            dest={"pre": "existing"},
        )

        # $eval creates {x: 1} in its local dest, returns it, assigned to /result
        # The outer "pre" field remains untouched
        assert result == {"pre": "existing", "result": {"x": 1}}
