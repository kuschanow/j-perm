"""Tests for mathematical and logical operator handlers."""

import pytest
from j_perm import build_default_engine


class TestComparisonOperators:
    """Test comparison operator constructs ($gt, $gte, $lt, $lte, $eq, $ne)."""

    def test_gt_true(self):
        """$gt returns True when left > right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gt": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_gt_false(self):
        """$gt returns False when left <= right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gt": [5, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_gt_with_templates(self):
        """$gt works with template substitution."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gt": ["${/age}", 18]}},
            source={"age": 25},
            dest={},
        )

        assert result == {"result": True}

    def test_gte_equal(self):
        """$gte returns True when left == right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gte": [10, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_gte_greater(self):
        """$gte returns True when left > right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gte": [15, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_lt_true(self):
        """$lt returns True when left < right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$lt": [5, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_lt_false(self):
        """$lt returns False when left >= right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$lt": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_lte_equal(self):
        """$lte returns True when left == right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$lte": [10, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_lte_less(self):
        """$lte returns True when left < right."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$lte": [5, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_eq_true(self):
        """$eq returns True when values are equal."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$eq": [10, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_eq_false(self):
        """$eq returns False when values are not equal."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$eq": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_eq_strings(self):
        """$eq works with strings."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$eq": ["${/status}", "active"]}},
            source={"status": "active"},
            dest={},
        )

        assert result == {"result": True}

    def test_ne_true(self):
        """$ne returns True when values are not equal."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ne": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_ne_false(self):
        """$ne returns False when values are equal."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ne": [10, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_comparison_with_ref(self):
        """Comparison operators work with $ref."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gt": [{"$ref": "/count"}, 100]}},
            source={"count": 150},
            dest={},
        )

        assert result == {"result": True}

    def test_comparison_with_cast(self):
        """Comparison operators work with $cast."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$gte": [{"$cast": {"value": "25", "type": "int"}}, 18]}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_gt_invalid_args_raises(self):
        """$gt raises ValueError if not given exactly 2 values."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a list of exactly 2 values"):
            engine.apply(
                {"/result": {"$gt": [10]}},
                source={},
                dest={},
            )

    def test_eq_invalid_args_raises(self):
        """$eq raises ValueError if not given a list."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a list of exactly 2 values"):
            engine.apply(
                {"/result": {"$eq": "invalid"}},
                source={},
                dest={},
            )

    def test_nested_comparisons_in_if(self):
        """Comparison operators can be used in if conditions."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/age": 25},
                {
                    "op": "if",
                    "cond": {"$gte": [{"$ref": "@:/age"}, 18]},
                    "then": [{"/is_adult": True}],
                    "else": [{"/is_adult": False}],
                },
            ],
            source={},
            dest={},
        )

        assert result == {"age": 25, "is_adult": True}


class TestMathOperators:
    """Test mathematical operator constructs ($add, $sub, $mul, $div, $pow, $mod)."""

    # --- $add tests ---
    def test_add_single_operand(self):
        """$add with single operand returns the value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$add": [42]}},
            source={},
            dest={},
        )

        assert result == {"result": 42}

    def test_add_two_operands(self):
        """$add with two operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$add": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 15}

    def test_add_multiple_operands(self):
        """$add with multiple operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$add": [1, 2, 3, 4]}},
            source={},
            dest={},
        )

        assert result == {"result": 10}

    def test_add_with_templates(self):
        """$add works with template substitution."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$add": ["${/a}", {"$ref": "/b"}, 5]}},
            source={"a": 10, "b": 20},
            dest={},
        )

        assert result == {"result": 35}

    # --- $sub tests ---
    def test_sub_single_operand(self):
        """$sub with single operand returns the value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$sub": [42]}},
            source={},
            dest={},
        )

        assert result == {"result": 42}

    def test_sub_two_operands(self):
        """$sub with two operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$sub": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 5}

    def test_sub_multiple_operands(self):
        """$sub with multiple operands (left-to-right)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$sub": [100, 20, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": 70}  # (100 - 20) - 10

    # --- $mul tests ---
    def test_mul_single_operand(self):
        """$mul with single operand returns the value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$mul": [7]}},
            source={},
            dest={},
        )

        assert result == {"result": 7}

    def test_mul_two_operands(self):
        """$mul with two operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$mul": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 50}

    def test_mul_multiple_operands(self):
        """$mul with multiple operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$mul": [2, 3, 4]}},
            source={},
            dest={},
        )

        assert result == {"result": 24}  # ((2 * 3) * 4)

    # --- $div tests ---
    def test_div_single_operand(self):
        """$div with single operand returns the value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$div": [10]}},
            source={},
            dest={},
        )

        assert result == {"result": 10}

    def test_div_two_operands(self):
        """$div with two operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$div": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 2.0}

    def test_div_multiple_operands(self):
        """$div with multiple operands (left-to-right)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$div": [100, 2, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 10.0}  # ((100 / 2) / 5)

    # --- $pow tests ---
    def test_pow_single_operand(self):
        """$pow with single operand returns the value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$pow": [3]}},
            source={},
            dest={},
        )

        assert result == {"result": 3}

    def test_pow_two_operands(self):
        """$pow with two operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$pow": [2, 3]}},
            source={},
            dest={},
        )

        assert result == {"result": 8}

    def test_pow_multiple_operands(self):
        """$pow with multiple operands (left-to-right)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$pow": [2, 3, 2]}},
            source={},
            dest={},
        )

        assert result == {"result": 64}  # ((2 ** 3) ** 2)

    # --- $mod tests ---
    def test_mod_single_operand(self):
        """$mod with single operand returns the value."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$mod": [7]}},
            source={},
            dest={},
        )

        assert result == {"result": 7}

    def test_mod_two_operands(self):
        """$mod with two operands."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$mod": [10, 3]}},
            source={},
            dest={},
        )

        assert result == {"result": 1}

    def test_mod_multiple_operands(self):
        """$mod with multiple operands (left-to-right)."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$mod": [100, 7, 3]}},
            source={},
            dest={},
        )

        assert result == {"result": 2}  # ((100 % 7) % 3) = (2 % 3) = 2

    # --- Integration tests ---
    def test_nested_math_expressions(self):
        """Math operators can be nested."""
        engine = build_default_engine()

        # (2 * 3) + 4 = 10
        result = engine.apply(
            {"/result": {"$add": [{"$mul": [2, 3]}, 4]}},
            source={},
            dest={},
        )

        assert result == {"result": 10}

    def test_complex_expression(self):
        """Complex nested expression."""
        engine = build_default_engine()

        # ((10 + 5) * 2) - 3 = 27
        result = engine.apply(
            {"/result": {"$sub": [{"$mul": [{"$add": [10, 5]}, 2]}, 3]}},
            source={},
            dest={},
        )

        assert result == {"result": 27}

    def test_math_with_cast(self):
        """Math operators work with $cast."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$add": [{"$cast": {"value": "10", "type": "int"}}, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 15}

    def test_math_in_condition(self):
        """Math operators can be used in conditions."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "if",
                "cond": {"$gt": [{"$add": [10, 5]}, 12]},
                "then": [{"/success": True}],
                "else": [{"/success": False}],
            },
            source={},
            dest={},
        )

        assert result == {"success": True}  # 15 > 12

    def test_add_invalid_args_raises(self):
        """$add raises ValueError if not given a list."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a list of at least 1 value"):
            engine.apply(
                {"/result": {"$add": "invalid"}},
                source={},
                dest={},
            )

    def test_add_empty_list_raises(self):
        """$add raises ValueError if given an empty list."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires a list of at least 1 value"):
            engine.apply(
                {"/result": {"$add": []}},
                source={},
                dest={},
            )


class TestExistsConstruct:
    """Test $exists construct — checks if a path resolves without error."""

    def test_exists_present_source(self):
        """$exists returns True for a path that exists in source."""
        engine = build_default_engine()

        result = engine.apply(
            {"/ok": {"$exists": "/user/name"}},
            source={"user": {"name": "Alice"}},
            dest={},
        )

        assert result == {"ok": True}

    def test_exists_missing_source(self):
        """$exists returns False for a path that is absent in source."""
        engine = build_default_engine()

        result = engine.apply(
            {"/ok": {"$exists": "/user/age"}},
            source={"user": {"name": "Alice"}},
            dest={},
        )

        assert result == {"ok": False}

    def test_exists_dest_prefix(self):
        """$exists with @: prefix checks dest."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/counter": 0},
                {"/has_counter": {"$exists": "@:/counter"}},
                {"/no_field": {"$exists": "@:/missing"}},
            ],
            source={},
            dest={},
        )

        assert result["has_counter"] is True
        assert result["no_field"] is False

    def test_exists_temp_read_only_prefix(self):
        """$exists with &: prefix checks temp_read_only (function args)."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "check",
                    "params": ["x"],
                    "body": [{"/found": {"$exists": "&:/x"}}],
                    "return": "/found",
                },
                {"/result": {"$func": "check", "args": [42]}},
            ],
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_exists_nested_path(self):
        """$exists works for deeply nested paths."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/deep": {"$exists": "/a/b/c"}},
                {"/shallow": {"$exists": "/a/b"}},
                {"/top": {"$exists": "/a"}},
                {"/none": {"$exists": "/z"}},
            ],
            source={"a": {"b": {"c": 1}}},
            dest={},
        )

        assert result["deep"] is True
        assert result["shallow"] is True
        assert result["top"] is True
        assert result["none"] is False

    def test_exists_in_condition(self):
        """$exists can be used as a condition in if."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "if",
                "cond": {"$exists": "/optional"},
                "then": [{"/status": "has_optional"}],
                "else": [{"/status": "no_optional"}],
            },
            source={"optional": "present"},
            dest={},
        )

        assert result == {"status": "has_optional"}

    def test_exists_in_condition_false(self):
        """$exists condition evaluates False when path is missing."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "if",
                "cond": {"$exists": "/optional"},
                "then": [{"/status": "has_optional"}],
                "else": [{"/status": "no_optional"}],
            },
            source={},
            dest={},
        )

        assert result == {"status": "no_optional"}

    def test_exists_with_template_path(self):
        """$exists path can be a template expression."""
        engine = build_default_engine()

        result = engine.apply(
            {"/ok": {"$exists": "/user/${/field}"}},
            source={"user": {"name": "Alice"}, "field": "name"},
            dest={},
        )

        assert result == {"ok": True}

    def test_exists_invalid_arg_raises(self):
        """$exists raises error when argument is not a string."""
        engine = build_default_engine()

        with pytest.raises(Exception):
            engine.apply(
                {"/ok": {"$exists": 42}},
                source={},
                dest={},
            )