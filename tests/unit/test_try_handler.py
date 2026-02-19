"""Tests for TryHandler (try-except error handling)."""

import pytest
from j_perm import build_default_engine, JPermError


class TestTryHandler:
    """Test try-except error handling construct."""

    def test_try_no_error(self):
        """Try executes normally when no error occurs."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/value": 42},
                    {"/status": "success"},
                ],
            },
            source={},
            dest={},
        )

        assert result == {"value": 42, "status": "success"}

    def test_try_with_error_caught(self):
        """Try catches errors and executes except block."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/value": 42},
                    {"$raise": "Something went wrong"},
                    {"/not_reached": True},
                ],
                "except": [
                    {"/error_handled": True},
                ],
            },
            source={},
            dest={},
        )

        # Error was caught, except block executed
        assert result == {"value": 42, "error_handled": True}
        assert "not_reached" not in result

    def test_try_without_except_reraises(self):
        """Try without except block re-raises the error."""
        engine = build_default_engine()

        with pytest.raises(JPermError, match="Test error"):
            engine.apply(
                {
                    "op": "try",
                    "do": [
                        {"$raise": "Test error"},
                    ],
                },
                source={},
                dest={},
            )

    def test_try_except_has_access_to_error(self):
        """Except block has access to error message via metadata."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"$raise": "Custom error message"},
                ],
                "except": [
                    {"/error_message": "${&:/_error_message}"},
                ],
            },
            source={},
            dest={},
        )

        assert result["error_message"] == "Custom error message"

    def test_try_finally_always_executes(self):
        """Finally block executes whether error occurs or not."""
        engine = build_default_engine()

        # No error - finally still runs
        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/value": 42},
                ],
                "finally": [
                    {"/cleanup": True},
                ],
            },
            source={},
            dest={},
        )

        assert result == {"value": 42, "cleanup": True}

    def test_try_finally_executes_after_error(self):
        """Finally executes after error and except."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/value": 42},
                    {"$raise": "Error"},
                ],
                "except": [
                    {"/error_caught": True},
                ],
                "finally": [
                    {"/cleanup": True},
                ],
            },
            source={},
            dest={},
        )

        assert result == {"value": 42, "error_caught": True, "cleanup": True}

    def test_try_finally_without_except_reraises(self):
        """Finally runs but error is re-raised if no except."""
        engine = build_default_engine()

        with pytest.raises(JPermError, match="Test error"):
            engine.apply(
                {
                    "op": "try",
                    "do": [
                        {"/before": 1},
                        {"$raise": "Test error"},
                    ],
                    "finally": [
                        {"/cleanup": True},
                    ],
                },
                source={},
                dest={},
            )

    def test_try_nested(self):
        """Try blocks can be nested."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/outer": 1},
                    {
                        "op": "try",
                        "do": [
                            {"/inner": 2},
                            {"$raise": "Inner error"},
                        ],
                        "except": [
                            {"/inner_error_caught": True},
                        ],
                    },
                    {"/after_inner": 3},
                ],
            },
            source={},
            dest={},
        )

        assert result == {
            "outer": 1,
            "inner": 2,
            "inner_error_caught": True,
            "after_inner": 3,
        }

    def test_try_with_missing_path_error(self):
        """Try can catch errors from missing paths."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"op": "copy", "from": "/nonexistent", "path": "/result"},
                ],
                "except": [
                    {"/error": "Path not found"},
                ],
            },
            source={},
            dest={},
        )

        assert result == {"error": "Path not found"}

    def test_try_except_can_reference_dest(self):
        """Except block can reference values set before error."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/counter": 10},
                    {"/name": "test"},
                    {"$raise": "Error after setup"},
                ],
                "except": [
                    {"/error_context": "Counter was ${@:/counter}, name was ${@:/name}"},
                ],
            },
            source={},
            dest={},
        )

        assert result["error_context"] == "Counter was 10, name was test"

    def test_try_with_function_error(self):
        """Try can catch errors from function calls."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "failingFunc",
                    "body": [
                        {"$raise": "Function failed"},
                    ],
                },
                {
                    "op": "try",
                    "do": [
                        {"/result": {"$func": "failingFunc"}},
                    ],
                    "except": [
                        {"/func_error_caught": True},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result["func_error_caught"] is True

    def test_try_empty_do_block(self):
        """Try with empty do block."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [],
            },
            source={},
            dest={},
        )

        assert result == {}

    def test_try_except_preserves_dest_on_error(self):
        """Except sees the dest as it was when error occurred."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/step1": "done"},
                    {"/step2": "done"},
                    {"$raise": "Error at step 3"},
                    {"/step3": "not reached"},
                ],
                "except": [
                    {"/steps_completed": ["${@:/step1}", "${@:/step2}"]},
                ],
            },
            source={},
            dest={},
        )

        assert result == {
            "step1": "done",
            "step2": "done",
            "steps_completed": ["done", "done"],
        }

    def test_try_error_in_except_reraises(self):
        """Error in except block is re-raised."""
        engine = build_default_engine()

        with pytest.raises(JPermError, match="Error in except"):
            engine.apply(
                {
                    "op": "try",
                    "do": [
                        {"$raise": "Original error"},
                    ],
                    "except": [
                        {"/handled": True},
                        {"$raise": "Error in except"},
                    ],
                },
                source={},
                dest={},
            )

    def test_try_error_in_finally_propagates(self):
        """Error in finally block propagates."""
        engine = build_default_engine()

        with pytest.raises(JPermError, match="Error in finally"):
            engine.apply(
                {
                    "op": "try",
                    "do": [
                        {"/value": 42},
                    ],
                    "finally": [
                        {"$raise": "Error in finally"},
                    ],
                },
                source={},
                dest={},
            )

    def test_try_finally_error_suppresses_original(self):
        """Error in finally suppresses original error (not applicable with current implementation)."""
        engine = build_default_engine()

        # With current implementation, if except handles the error,
        # finally error will take precedence
        with pytest.raises(JPermError, match="Finally error"):
            engine.apply(
                {
                    "op": "try",
                    "do": [
                        {"$raise": "Original error"},
                    ],
                    "except": [
                        {"/caught": True},
                    ],
                    "finally": [
                        {"$raise": "Finally error"},
                    ],
                },
                source={},
                dest={},
            )

    def test_try_with_validation_pattern(self):
        """Try-except for validation with fallback."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [
                    {"/age": {"$cast": {"value": "${/user_input}", "type": "int"}}},
                    {
                        "op": "if",
                        "cond": {"$lt": [{"$ref": "@:/age"}, 0]},
                        "then": [{"$raise": "Age cannot be negative"}],
                    },
                    {"/valid": True},
                ],
                "except": [
                    {"/valid": False},
                    {"/error_msg": "${&:/_error_message}"},
                ],
            },
            source={"user_input": "-5"},
            dest={},
        )

        assert result["valid"] is False
        assert result["error_msg"] == "Age cannot be negative"
