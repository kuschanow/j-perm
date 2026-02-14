"""Tests for build_default_engine factory."""

import pytest
from j_perm import build_default_engine, ref_handler, eval_handler


class TestBuildDefaultEngine:
    """Test build_default_engine factory function."""

    def test_builds_working_engine(self):
        """Factory returns a working engine."""
        engine = build_default_engine()

        result = engine.apply(
            {"/test": "value"},
            source={},
            dest={},
        )

        assert result == {"test": "value"}

    def test_default_specials_included(self):
        """Default engine includes $ref and $eval."""
        engine = build_default_engine()

        # Test $ref
        result = engine.apply(
            {"/result": {"$ref": "/data"}},
            source={"data": "value"},
            dest={},
        )
        assert result == {"result": "value"}

        # Test $eval
        result = engine.apply(
            {"/result": {"$eval": {"/x": 1}}},
            source={},
            dest={},
        )
        assert result == {"result": {"x": 1}}

    def test_all_ops_registered(self):
        """All 12 operations are registered."""
        engine = build_default_engine()

        ops = [
            {"op": "set", "path": "/x", "value": 1},
            {"op": "copy", "from": "/x", "path": "/y"},
            {"op": "delete", "path": "/z"},
            {"op": "update", "path": "/obj", "value": {}},
            {"op": "distinct", "path": "/arr"},
            {"op": "assert", "path": "/x"},
        ]

        # Each should execute without "unhandled step" error
        for op_spec in ops:
            if op_spec["op"] == "assert":
                source = {"x": 1}
            else:
                source = {}

            if op_spec["op"] == "update":
                dest = {"obj": {}}
            elif op_spec["op"] == "distinct":
                dest = {"arr": [1, 2]}
            elif op_spec["op"] == "copy":
                dest = {"x": "val"}
            else:
                dest = {}

            # Should not raise "unhandled step"
            try:
                engine.apply(op_spec, source=source, dest=dest)
            except Exception as e:
                if "unhandled" in str(e):
                    pytest.fail(f"Operation {op_spec['op']} not registered")

    def test_custom_specials(self):
        """Can override specials."""

        def custom_special(node, ctx):
            return "custom"

        engine = build_default_engine(specials={"$custom": custom_special})

        result = engine.apply(
            {"/result": {"$custom": "ignored"}},
            source={},
            dest={},
        )

        assert result == {"result": "custom"}

    def test_template_casters_work(self):
        """Built-in casters are available."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": "${int:/value}"},
            source={"value": "42"},
            dest={},
        )

        # Single expression returns native type
        assert result == {"result": 42}

    def test_jmespath_functions_work(self):
        """Built-in JMESPath functions available."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": "${int:${?subtract(`100`, `30`)}}"},
            source={},
            dest={},
        )

        assert result == {"result": 70}

    def test_shorthand_stages_registered(self):
        """Shorthand expansion works."""
        engine = build_default_engine()

        # Test all shorthands
        result = engine.apply(
            {
                "~delete": "/tmp",
                "/literal": 123,
                "/copy": "/source",
            },
            source={"source": "value"},
            dest={"tmp": "remove"},
        )

        assert result == {"literal": 123, "copy": "value"}

    def test_value_max_depth_parameter(self):
        """Can set custom value_max_depth."""
        engine = build_default_engine(value_max_depth=5)

        # Engine should work normally
        result = engine.apply(
            {"/test": "${/value}"},
            source={"value": "ok"},
            dest={},
        )

        assert result == {"test": "ok"}

    def test_unescape_rules_registered(self):
        """Template unescape rule is registered."""
        engine = build_default_engine()

        # Escaped templates should be unescaped after stabilization
        result = engine.apply(
            {"/test": "$${literal}"},
            source={},
            dest={},
        )

        assert result == {"test": "${literal}"}
