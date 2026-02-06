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
