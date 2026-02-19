"""Tests for security limits in math and regex handlers.

Tests protection against:
- ReDoS (Regular Expression Denial of Service)
- DoS via large numbers in $pow
- DoS via large string multiplication in $mul
- DoS via large numeric operands in $mul
"""

import pytest
import re
from j_perm import build_default_engine


class TestPowSecurityLimits:
    """Test $pow security limits to prevent DoS via large numbers."""

    def test_pow_base_limit_exceeded(self):
        """$pow raises ValueError when base exceeds limit."""
        engine = build_default_engine(pow_max_base=1000)

        with pytest.raises(ValueError, match="Base value .* exceeds limit of 1000"):
            engine.apply(
                {"/result": {"$pow": [10000, 2]}},
                source={},
                dest={},
            )

    def test_pow_exponent_limit_exceeded(self):
        """$pow raises ValueError when exponent exceeds limit."""
        engine = build_default_engine(pow_max_exponent=10)

        with pytest.raises(ValueError, match="Exponent value .* exceeds limit of 10"):
            engine.apply(
                {"/result": {"$pow": [2, 100]}},
                source={},
                dest={},
            )

    def test_pow_intermediate_result_limit_exceeded(self):
        """$pow raises ValueError when intermediate result exceeds base limit."""
        engine = build_default_engine(pow_max_base=10000)

        # 10^3 = 1000, then 1000^3 = 1_000_000_000 > 10000
        with pytest.raises(ValueError, match="exceeds base limit"):
            engine.apply(
                {"/result": {"$pow": [10, 3, 3]}},
                source={},
                dest={},
            )

    def test_pow_within_limits(self):
        """$pow works normally when within limits."""
        engine = build_default_engine(pow_max_base=1e6, pow_max_exponent=1000)

        result = engine.apply(
            {"/result": {"$pow": [2, 10]}},
            source={},
            dest={},
        )

        assert result == {"result": 1024}

    def test_pow_negative_values_checked(self):
        """$pow checks absolute values for negative numbers."""
        engine = build_default_engine(pow_max_base=1000)

        with pytest.raises(ValueError, match="Base value .* exceeds limit"):
            engine.apply(
                {"/result": {"$pow": [-10000, 2]}},
                source={},
                dest={},
            )

    def test_pow_custom_limits_via_factory(self):
        """Custom pow limits can be set via build_default_engine."""
        engine = build_default_engine(
            pow_max_base=100,
            pow_max_exponent=5,
        )

        # Should fail with custom limits
        with pytest.raises(ValueError, match="Exponent value .* exceeds limit of 5"):
            engine.apply(
                {"/result": {"$pow": [2, 10]}},
                source={},
                dest={},
            )

    def test_pow_with_templates_checks_limits(self):
        """$pow checks limits even with template substitution."""
        engine = build_default_engine(pow_max_exponent=10)

        with pytest.raises(ValueError, match="Exponent value .* exceeds limit"):
            engine.apply(
                {"/result": {"$pow": [2, "${/exp}"]}},
                source={"exp": 100},
                dest={},
            )

    def test_pow_single_operand_no_limit_check(self):
        """$pow with single operand doesn't trigger exponent check."""
        engine = build_default_engine(pow_max_exponent=10)

        # Single operand just returns the value
        result = engine.apply(
            {"/result": {"$pow": [1000]}},
            source={},
            dest={},
        )

        assert result == {"result": 1000}


class TestMulSecurityLimits:
    """Test $mul security limits to prevent DoS via large strings/numbers."""

    def test_mul_string_length_limit_exceeded(self):
        """$mul raises ValueError when string multiplication exceeds limit."""
        engine = build_default_engine(mul_max_string_result=100)

        with pytest.raises(ValueError, match="String multiplication would create string of length 1000"):
            engine.apply(
                {"/result": {"$mul": ["A", 1000]}},
                source={},
                dest={},
            )

    def test_mul_string_reverse_order_limit_exceeded(self):
        """$mul checks string limit regardless of operand order."""
        engine = build_default_engine(mul_max_string_result=100)

        with pytest.raises(ValueError, match="String multiplication would create string of length 1000"):
            engine.apply(
                {"/result": {"$mul": [1000, "A"]}},
                source={},
                dest={},
            )

    def test_mul_numeric_operand_limit_exceeded(self):
        """$mul raises ValueError when numeric operand exceeds limit."""
        engine = build_default_engine(mul_max_operand=1e6)

        with pytest.raises(ValueError, match="Numeric operand .* exceeds limit"):
            engine.apply(
                {"/result": {"$mul": [10, 1e9]}},
                source={},
                dest={},
            )

    def test_mul_string_within_limits(self):
        """$mul works normally for strings within limits."""
        engine = build_default_engine(mul_max_string_result=1000)

        result = engine.apply(
            {"/result": {"$mul": ["AB", 3]}},
            source={},
            dest={},
        )

        assert result == {"result": "ABABAB"}

    def test_mul_numbers_within_limits(self):
        """$mul works normally for numbers within limits."""
        engine = build_default_engine(mul_max_operand=1e9)

        result = engine.apply(
            {"/result": {"$mul": [10, 5]}},
            source={},
            dest={},
        )

        assert result == {"result": 50}

    def test_mul_multiple_operands_checks_all(self):
        """$mul checks limits for each operand in chain."""
        engine = build_default_engine(mul_max_operand=1e6)

        # First multiplication is OK, but second operand exceeds limit
        with pytest.raises(ValueError, match="Numeric operand .* exceeds limit"):
            engine.apply(
                {"/result": {"$mul": [10, 100, 1e9]}},
                source={},
                dest={},
            )

    def test_mul_custom_limits_via_factory(self):
        """Custom mul limits can be set via build_default_engine."""
        engine = build_default_engine(
            mul_max_string_result=50,
            mul_max_operand=1000,
        )

        # Should fail with custom limits
        with pytest.raises(ValueError, match="String multiplication would create string of length 200"):
            engine.apply(
                {"/result": {"$mul": ["X", 200]}},
                source={},
                dest={},
            )

    def test_mul_with_templates_checks_limits(self):
        """$mul checks limits even with template substitution."""
        engine = build_default_engine(mul_max_string_result=100)

        with pytest.raises(ValueError, match="String multiplication would create string of length"):
            engine.apply(
                {"/result": {"$mul": ["${/str}", "${/count}"]}},
                source={"str": "A", "count": 1000},
                dest={},
            )

    def test_mul_negative_multiplier_checks_absolute_value(self):
        """$mul checks absolute value for negative multipliers."""
        engine = build_default_engine(mul_max_string_result=100)

        # Even with negative multiplier, should check absolute value
        # Note: Python doesn't support negative string multiplication, but check abs value
        with pytest.raises(ValueError, match="String multiplication would create string of length"):
            engine.apply(
                {"/result": {"$mul": ["A", -1000]}},
                source={},
                dest={},
            )

    def test_mul_single_operand_no_limit_check(self):
        """$mul with single operand doesn't trigger limit checks."""
        engine = build_default_engine(mul_max_operand=100)

        # Single operand just returns the value
        result = engine.apply(
            {"/result": {"$mul": [10000]}},
            source={},
            dest={},
        )

        assert result == {"result": 10000}


class TestRegexSecurityLimits:
    """Test regex security limits to prevent ReDoS attacks."""

    def test_regex_match_with_timeout(self):
        """$regex_match uses timeout parameter."""
        engine = build_default_engine(regex_timeout=2.0)

        # Normal pattern should work fine
        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": r"^\d+$", "string": "12345"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_regex_search_with_timeout(self):
        """$regex_search uses timeout parameter."""
        engine = build_default_engine(regex_timeout=2.0)

        result = engine.apply(
            {"/result": {"$regex_search": {"pattern": r"\d+", "string": "abc123"}}},
            source={},
            dest={},
        )

        assert result == {"result": "123"}

    def test_regex_findall_with_timeout(self):
        """$regex_findall uses timeout parameter."""
        engine = build_default_engine(regex_timeout=2.0)

        result = engine.apply(
            {"/result": {"$regex_findall": {"pattern": r"\d+", "string": "a1b2c3"}}},
            source={},
            dest={},
        )

        assert result == {"result": ["1", "2", "3"]}

    def test_regex_replace_with_timeout(self):
        """$regex_replace uses timeout parameter."""
        engine = build_default_engine(regex_timeout=2.0)

        result = engine.apply(
            {"/result": {"$regex_replace": {"pattern": r"\d+", "replacement": "X", "string": "a1b2"}}},
            source={},
            dest={},
        )

        assert result == {"result": "aXbX"}

    def test_regex_groups_with_timeout(self):
        """$regex_groups uses timeout parameter."""
        engine = build_default_engine(regex_timeout=2.0)

        result = engine.apply(
            {"/result": {"$regex_groups": {"pattern": r"(\w+)@(\w+)", "string": "user@domain"}}},
            source={},
            dest={},
        )

        assert result == {"result": ["user", "domain"]}

    def test_regex_flags_within_allowed(self):
        """Regex operations work with allowed flags."""
        # Default allows IGNORECASE, MULTILINE, DOTALL, VERBOSE, ASCII
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": "^hello$", "string": "HELLO", "flags": re.IGNORECASE}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_regex_flags_outside_allowed_raises(self):
        """Regex operations raise ValueError for disallowed flags."""
        # Create engine with very restrictive flags (only IGNORECASE allowed)
        engine = build_default_engine(regex_allowed_flags=re.IGNORECASE)

        # Try to use MULTILINE which is not allowed
        with pytest.raises(ValueError, match="contain disallowed flags"):
            engine.apply(
                {"/result": {"$regex_match": {"pattern": "test", "string": "test", "flags": re.MULTILINE}}},
                source={},
                dest={},
            )

    def test_regex_custom_timeout_via_factory(self):
        """Custom regex timeout can be set via build_default_engine."""
        engine = build_default_engine(regex_timeout=5.0)

        # Should work fine with longer timeout
        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": r"^\d+$", "string": "12345"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_regex_custom_allowed_flags(self):
        """Custom allowed flags can be set via build_default_engine."""
        # Only allow IGNORECASE and MULTILINE
        engine = build_default_engine(
            regex_allowed_flags=re.IGNORECASE | re.MULTILINE
        )

        # IGNORECASE should work
        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": "test", "string": "TEST", "flags": re.IGNORECASE}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

        # DOTALL should fail
        with pytest.raises(ValueError, match="contain disallowed flags"):
            engine.apply(
                {"/result": {"$regex_match": {"pattern": "test", "string": "test", "flags": re.DOTALL}}},
                source={},
                dest={},
            )

    def test_regex_no_flags_always_allowed(self):
        """Regex operations with no flags always work."""
        engine = build_default_engine(regex_allowed_flags=0)  # No flags allowed

        # No flags should work even with restrictive setting
        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": "test", "string": "test"}}},
            source={},
            dest={},
        )

        assert result == {"result": True}

    def test_regex_with_templates_uses_timeout(self):
        """Regex with template substitution still uses timeout."""
        engine = build_default_engine(regex_timeout=2.0)

        result = engine.apply(
            {"/result": {"$regex_match": {"pattern": "${/pattern}", "string": "${/text}"}}},
            source={"pattern": r"^\d+$", "text": "123"},
            dest={},
        )

        assert result == {"result": True}


class TestSecurityLimitsIntegration:
    """Integration tests for security limits across multiple handlers."""

    def test_all_limits_can_be_customized(self):
        """All security limits can be customized together."""
        engine = build_default_engine(
            regex_timeout=1.0,
            regex_allowed_flags=re.IGNORECASE,
            pow_max_base=1000,
            pow_max_exponent=100,
            mul_max_string_result=500,
            mul_max_operand=1e6,
        )

        # Should fail pow check
        with pytest.raises(ValueError, match="Base value .* exceeds limit of 1000"):
            engine.apply(
                {"/result": {"$pow": [10000, 2]}},
                source={},
                dest={},
            )

        # Should fail mul check
        with pytest.raises(ValueError, match="String multiplication would create string of length"):
            engine.apply(
                {"/result": {"$mul": ["X", 1000]}},
                source={},
                dest={},
            )

    def test_limits_work_in_nested_expressions(self):
        """Security limits work in nested expressions."""
        engine = build_default_engine(pow_max_exponent=10)

        # Nested $pow should still check limits
        with pytest.raises(ValueError, match="Exponent value .* exceeds limit"):
            engine.apply(
                {"/result": {"$add": [{"$pow": [2, 100]}, 5]}},
                source={},
                dest={},
            )

    def test_limits_work_with_ref_and_cast(self):
        """Security limits work with $ref and $cast."""
        engine = build_default_engine(mul_max_string_result=100)

        with pytest.raises(ValueError, match="String multiplication would create string of length"):
            engine.apply(
                {"/result": {"$mul": [{"$ref": "/str"}, {"$cast": {"value": "1000", "type": "int"}}]}},
                source={"str": "A"},
                dest={},
            )

    def test_limits_work_in_conditions(self):
        """Security limits work in if conditions."""
        engine = build_default_engine(pow_max_exponent=10)

        # Should fail even inside condition
        with pytest.raises(ValueError, match="Exponent value .* exceeds limit"):
            engine.apply(
                {
                    "op": "if",
                    "cond": {"$gt": [{"$pow": [2, 100]}, 100]},
                    "then": [{"/result": True}],
                },
                source={},
                dest={},
            )

    def test_default_limits_are_reasonable(self):
        """Default limits allow normal operations but prevent extreme cases."""
        engine = build_default_engine()

        # Normal operations should work
        result = engine.apply(
            {
                "/pow_result": {"$pow": [10, 5]},
                "/mul_result": {"$mul": ["test", 100]},
                "/regex_result": {"$regex_match": {"pattern": r"^\w+$", "string": "hello"}},
            },
            source={},
            dest={},
        )

        assert result["pow_result"] == 100_000
        assert result["mul_result"] == "test" * 100
        assert result["regex_result"] is True

        # Extreme operations should fail
        with pytest.raises(ValueError):
            engine.apply(
                {"/result": {"$pow": [10, 10000]}},
                source={},
                dest={},
            )

        with pytest.raises(ValueError):
            engine.apply(
                {"/result": {"$mul": ["A", 10_000_000]}},
                source={},
                dest={},
            )


class TestLoopSecurityLimits:
    """Test loop security limits (while, foreach) to prevent infinite loops and DoS."""

    def test_while_loop_exceeds_max_iterations(self):
        """While loop raises RuntimeError when exceeding max iterations."""
        engine = build_default_engine(max_loop_iterations=10)

        # This will loop forever without the limit
        with pytest.raises(RuntimeError, match="exceeded maximum iterations"):
            engine.apply(
                {"op": "while", "cond": True, "do": []},
                source={},
                dest={},
            )

    def test_while_loop_within_limit(self):
        """While loop works when within iteration limit."""
        engine = build_default_engine(max_loop_iterations=100)

        result = engine.apply(
            [
                {"/counter": 0},
                {
                    "op": "while",
                    "path": "@:/counter",
                    "cond": {"$lt": [{"$ref": "@:/counter"}, 10]},
                    "do": [{"op": "set", "path": "/counter", "value": {"$add": [{"$ref": "@:/counter"}, 1]}}],
                },
            ],
            source={},
            dest={},
        )

        assert result["counter"] == 10

    def test_while_loop_custom_limit(self):
        """Custom max_loop_iterations can be set."""
        engine = build_default_engine(max_loop_iterations=5)

        with pytest.raises(RuntimeError, match="exceeded maximum iterations \\(5\\)"):
            engine.apply(
                [
                    {"/counter": 0},
                    {
                        "op": "while",
                        "path": "@:/counter",
                        "cond": {"$lt": [{"$ref": "@:/counter"}, 100]},
                        "do": [{"op": "set", "path": "/counter", "value": {"$add": [{"$ref": "@:/counter"}, 1]}}],
                    },
                ],
                source={},
                dest={},
            )

    def test_foreach_exceeds_max_items(self):
        """Foreach raises ValueError when array size exceeds max."""
        engine = build_default_engine(max_foreach_items=100)

        with pytest.raises(ValueError, match="exceeds maximum \\(100\\)"):
            engine.apply(
                {"op": "foreach", "in": "/items", "do": [{"/result/-": "${item}"}]},
                source={"items": list(range(1000))},
                dest={},
            )

    def test_foreach_within_limit(self):
        """Foreach works when array size is within limit."""
        engine = build_default_engine(max_foreach_items=100)

        result = engine.apply(
            {"op": "foreach", "in": "/items", "as": "num", "do": [{"/result/-": "${&:num}"}]},
            source={"items": [1, 2, 3]},
            dest={},
        )

        assert result == {"result": [1, 2, 3]}

    def test_foreach_custom_limit(self):
        """Custom max_foreach_items can be set."""
        engine = build_default_engine(max_foreach_items=5)

        with pytest.raises(ValueError, match="exceeds maximum \\(5\\)"):
            engine.apply(
                {"op": "foreach", "in": "/items", "do": []},
                source={"items": list(range(10))},
                dest={},
            )


class TestStringOperationLimits:
    """Test string operation security limits to prevent DoS."""

    def test_str_split_exceeds_max_results(self):
        """$str_split raises ValueError when result would exceed limit."""
        engine = build_default_engine(str_max_split_results=10)

        with pytest.raises(ValueError, match="exceeding limit of 10"):
            engine.apply(
                {"/result": {"$str_split": {"string": "a b c d e f g h i j k l", "delimiter": " "}}},
                source={},
                dest={},
            )

    def test_str_split_within_limit(self):
        """$str_split works when result is within limit."""
        engine = build_default_engine(str_max_split_results=100)

        result = engine.apply(
            {"/result": {"$str_split": {"string": "a,b,c", "delimiter": ","}}},
            source={},
            dest={},
        )

        assert result == {"result": ["a", "b", "c"]}

    def test_str_split_custom_limit(self):
        """Custom str_max_split_results can be set."""
        engine = build_default_engine(str_max_split_results=2)

        with pytest.raises(ValueError, match="exceeding limit of 2"):
            engine.apply(
                {"/result": {"$str_split": {"string": "a,b,c", "delimiter": ","}}},
                source={},
                dest={},
            )

    def test_str_join_exceeds_max_length(self):
        """$str_join raises ValueError when result would exceed limit."""
        engine = build_default_engine(str_max_join_result=50)

        with pytest.raises(ValueError, match="exceeding limit of 50"):
            engine.apply(
                {"/result": {"$str_join": {"array": ["a" * 30, "b" * 30], "separator": ""}}},
                source={},
                dest={},
            )

    def test_str_join_within_limit(self):
        """$str_join works when result is within limit."""
        engine = build_default_engine(str_max_join_result=1000)

        result = engine.apply(
            {"/result": {"$str_join": {"array": ["a", "b", "c"], "separator": "-"}}},
            source={},
            dest={},
        )

        assert result == {"result": "a-b-c"}

    def test_str_join_custom_limit(self):
        """Custom str_max_join_result can be set."""
        engine = build_default_engine(str_max_join_result=5)

        with pytest.raises(ValueError, match="exceeding limit of 5"):
            engine.apply(
                {"/result": {"$str_join": {"array": ["abc", "def"], "separator": ""}}},
                source={},
                dest={},
            )

    def test_str_replace_exceeds_max_length(self):
        """$str_replace raises ValueError when result would exceed limit."""
        engine = build_default_engine(str_max_replace_result=100)

        with pytest.raises(ValueError, match="exceeding limit of 100"):
            engine.apply(
                {"/result": {"$str_replace": {"string": "x" * 50, "old": "x", "new": "xxx"}}},
                source={},
                dest={},
            )

    def test_str_replace_within_limit(self):
        """$str_replace works when result is within limit."""
        engine = build_default_engine(str_max_replace_result=1000)

        result = engine.apply(
            {"/result": {"$str_replace": {"string": "hello", "old": "ll", "new": "rr"}}},
            source={},
            dest={},
        )

        assert result == {"result": "herro"}

    def test_str_replace_custom_limit(self):
        """Custom str_max_replace_result can be set."""
        engine = build_default_engine(str_max_replace_result=10)

        with pytest.raises(ValueError, match="exceeding limit of 10"):
            engine.apply(
                {"/result": {"$str_replace": {"string": "aaaaa", "old": "a", "new": "bbb"}}},
                source={},
                dest={},
            )

class TestOperationCounterLimits:
    """Test operation counter to prevent runaway execution."""

    def test_operation_count_exceeds_limit(self):
        """Operation count raises RuntimeError when exceeding max."""
        engine = build_default_engine(max_operations=100)

        # Use exec with many set operations to hit operation count
        with pytest.raises(RuntimeError, match="Operation limit exceeded.*maximum allowed is 100"):
            engine.apply(
                {"op": "exec", "actions": [{f"/dummy{i}": i} for i in range(150)]},
                source={},
                dest={},
            )

    def test_operation_count_within_limit(self):
        """Operations work when within limit."""
        engine = build_default_engine(max_operations=100)

        result = engine.apply(
            {"op": "exec", "actions": [{f"/item{i}": i} for i in range(50)]},
            source={},
            dest={},
        )

        assert len(result) == 50
        assert result["item0"] == 0
        assert result["item49"] == 49

    def test_operation_count_custom_limit(self):
        """Custom max_operations can be set."""
        engine = build_default_engine(max_operations=5)

        with pytest.raises(RuntimeError, match="Operation limit exceeded.*maximum allowed is 5"):
            engine.apply(
                {"op": "exec", "actions": [{f"/item{i}": i} for i in range(10)]},
                source={},
                dest={},
            )


class TestFunctionRecursionLimits:
    """Test function recursion depth limits to prevent stack overflow."""

    def test_function_recursion_exceeds_limit(self):
        """Function recursion raises RecursionError when exceeding max depth."""
        engine = build_default_engine(max_function_recursion_depth=10)

        with pytest.raises(RecursionError, match="recursion depth.*exceeded maximum"):
            engine.apply(
                [
                    {
                        "$def": "loop",
                        "body": [{"$func": "loop"}],
                    },
                    {"$func": "loop"},
                ],
                source={},
                dest={},
            )

    def test_function_recursion_within_limit(self):
        """Function recursion works when within limit."""
        engine = build_default_engine(max_function_recursion_depth=100)

        result = engine.apply(
            [
                {"/counter": 0},
                {
                    "$def": "countdown",
                    "params": ["n"],
                    "body": [
                        {
                            "op": "if",
                            "cond": {"$gt": [{"$ref": "&:/n"}, 0]},
                            "then": [
                                {"op": "set", "path": "/counter", "value": {"$add": [{"$ref": "@:/counter"}, 1]}},
                                {"$func": "countdown", "args": [{"$sub": [{"$ref": "&:/n"}, 1]}]},
                            ],
                        },
                    ],
                },
                {"$func": "countdown", "args": [10]},
            ],
            source={},
            dest={},
        )

        assert result["counter"] == 10

    def test_function_recursion_custom_limit(self):
        """Custom max_function_recursion_depth can be set."""
        engine = build_default_engine(max_function_recursion_depth=3)

        with pytest.raises(RecursionError, match="exceeded maximum \\(3\\)"):
            engine.apply(
                [
                    {"/counter": 0},
                    {
                        "$def": "countdown",
                        "params": ["n"],
                        "body": [
                            {
                                "op": "if",
                                "cond": {"$gt": [{"$ref": "&:/n"}, 0]},
                                "then": [
                                    {"op": "set", "path": "/counter", "value": {"$add": [{"$ref": "@:/counter"}, 1]}},
                                    {"$func": "countdown", "args": [{"$sub": [{"$ref": "&:/n"}, 1]}]},
                                ],
                            },
                        ],
                    },
                    {"$func": "countdown", "args": [10]},
                ],
                source={},
                dest={},
            )


class TestMathAccumulativeLimits:
    """Test $add and $sub limits to prevent accumulative DoS."""

    def test_add_number_exceeds_limit(self):
        """$add raises ValueError when numeric result exceeds limit."""
        engine = build_default_engine(add_max_number_result=1000)

        with pytest.raises(ValueError, match="exceeds numeric limit"):
            engine.apply(
                {"/result": {"$add": [500, 600]}},
                source={},
                dest={},
            )

    def test_add_string_exceeds_limit(self):
        """$add raises ValueError when string result exceeds limit."""
        engine = build_default_engine(add_max_string_result=100)

        with pytest.raises(ValueError, match="exceeds limit of 100"):
            engine.apply(
                {"/result": {"$add": ["a" * 60, "b" * 60]}},
                source={},
                dest={},
            )

    def test_add_within_limits(self):
        """$add works when within limits."""
        engine = build_default_engine(
            add_max_number_result=1e15,
            add_max_string_result=100_000_000,
        )

        result = engine.apply(
            {
                "/num": {"$add": [100, 200]},
                "/str": {"$add": ["hello", " ", "world"]},
            },
            source={},
            dest={},
        )

        assert result == {"num": 300, "str": "hello world"}

    def test_add_accumulative_in_loop(self):
        """$add prevents accumulative DoS in loops."""
        engine = build_default_engine(add_max_string_result=1000)

        # Each iteration adds 100 chars, limit is 1000
        with pytest.raises(ValueError, match="exceeds limit"):
            engine.apply(
                [
                    {"/data": ""},
                    {
                        "op": "foreach",
                        "in": "/items",
                        "do": [
                            {"op": "set", "path": "/data", "value": {"$add": [{"$ref": "@:/data"}, "x" * 100]}},
                        ],
                    },
                ],
                source={"items": list(range(20))},  # 20 * 100 = 2000 > 1000
                dest={},
            )

    def test_sub_number_exceeds_limit(self):
        """$sub raises ValueError when numeric result exceeds limit."""
        engine = build_default_engine(sub_max_number_result=1000)

        with pytest.raises(ValueError, match="exceeds numeric limit"):
            engine.apply(
                {"/result": {"$sub": [-500, 600]}},  # -500 - 600 = -1100
                source={},
                dest={},
            )

    def test_sub_within_limits(self):
        """$sub works when within limits."""
        engine = build_default_engine(sub_max_number_result=1e15)

        result = engine.apply(
            {"/result": {"$sub": [100, 50]}},
            source={},
            dest={},
        )

        assert result == {"result": 50}
