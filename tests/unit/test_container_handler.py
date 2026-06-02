"""Tests for container handler (recursive descent)."""

import pytest
from j_perm import build_default_engine


class TestRecursiveDescentHandler:
    """Test RecursiveDescentHandler (containers)."""

    def test_processes_list_elements(self):
        """Recursively process list elements."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": ["${/a}", "${/b}", "${/c}"]},
            source={"a": 1, "b": 2, "c": 3},
            dest={},
        )

        assert result == {"result": [1, 2, 3]}

    def test_processes_dict_values(self):
        """Recursively process dict values."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"x": "${/a}", "y": "${/b}"}},
            source={"a": "A", "b": "B"},
            dest={},
        )

        assert result == {"result": {"x": "A", "y": "B"}}

    def test_processes_nested_structures(self):
        """Recursively process deeply nested structures."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"nested": {"list": ["${/val}"]}}},
            source={"val": "deep"},
            dest={},
        )

        assert result == {"result": {"nested": {"list": ["deep"]}}}

    def test_preserves_non_template_values(self):
        """Non-template values pass through."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": [1, "text", None, True, {"key": 123}]},
            source={},
            dest={},
        )

        assert result == {"result": [1, "text", None, True, {"key": 123}]}

    def test_duplicate_key_after_substitution_raises(self):
        """Duplicate dict key after template substitution raises KeyError (line 74)."""
        engine = build_default_engine()

        # Both "${/k1}" and "${/k2}" resolve to the same key "x",
        # causing a duplicate key collision in the output dict.
        with pytest.raises(KeyError, match="duplicate key after substitution"):
            engine.apply(
                {"/result": {"${/k1}": 1, "${/k2}": 2}},
                source={"k1": "x", "k2": "x"},
                dest={},
            )

    def test_non_container_passthrough(self):
        """Non-container (scalar) values pass through unchanged (line 78)."""
        from j_perm import RecursiveDescentHandler, ExecutionContext

        handler = RecursiveDescentHandler()
        ctx = object()

        # Scalars are not list/tuple/Mapping, fall through to `return step`
        result = handler.execute(42, None)
        assert result == 42
