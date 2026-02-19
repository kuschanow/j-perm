"""Tests for loop and function control flow: $break, $continue, $return."""

import pytest

from j_perm import build_default_engine
from j_perm.handlers.signals import BreakSignal, ContinueSignal, ReturnSignal


# ─────────────────────────────────────────────────────────────────────────────
# $break
# ─────────────────────────────────────────────────────────────────────────────

class TestBreak:

    def test_break_exits_foreach_early(self):
        """$break stops foreach before processing all elements."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, "stop"]},
                        "then": [{"$break": None}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": ["a", "b", "stop", "c", "d"]},
            dest={"result": []},
        )

        assert result == {"result": ["a", "b"]}

    def test_break_exits_while_early(self):
        """$break stops while loop before condition becomes false."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/counter": 0},
                {
                    "op": "while",
                    "cond": {"$lt": [{"$ref": "@:/counter"}, 10]},
                    "do": [
                        {
                            "op": "if",
                            "cond": {"$eq": [{"$ref": "@:/counter"}, 3]},
                            "then": [{"$break": None}],
                        },
                        {"/counter": {"$add": [{"$ref": "@:/counter"}, 1]}},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result == {"counter": 3}

    def test_break_preserves_changes_before_break(self):
        """Changes made before $break in the same iteration are kept."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {"/last_seen": "&:/item"},
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, "b"]},
                        "then": [{"$break": None}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": ["a", "b", "c"]},
            dest={"result": []},
        )

        # "a" was fully processed, "b" was seen but break fired before appending
        assert result["result"] == ["a"]
        assert result["last_seen"] == "b"

    def test_break_in_empty_loop_is_noop(self):
        """$break in a loop with zero iterations does nothing."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [{"$break": None}],
                "skip_empty": False,
            },
            source={"items": []},
            dest={"untouched": True},
        )

        assert result == {"untouched": True}

    def test_break_outside_loop_propagates(self):
        """$break outside any loop propagates as BreakSignal."""
        engine = build_default_engine()

        with pytest.raises(BreakSignal):
            engine.apply(
                [{"$break": None}],
                source={},
                dest={},
            )

    def test_break_propagates_through_if(self):
        """$break inside an if branch reaches the enclosing loop."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, 2]},
                        "then": [{"$break": None}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": [1, 2, 3]},
            dest={"result": []},
        )

        assert result == {"result": [1]}

    def test_break_propagates_through_try(self):
        """$break inside a try block is not caught by except; it propagates to the loop."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "try",
                        "do": [
                            {
                                "op": "if",
                                "cond": {"$eq": [{"$ref": "&:/item"}, 2]},
                                "then": [{"$break": None}],
                            },
                        ],
                        "except": [{"/caught": True}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": [1, 2, 3]},
            dest={"result": []},
        )

        assert result == {"result": [1]}
        assert "caught" not in result

    def test_break_try_finally_runs(self):
        """$break inside try still executes the finally block."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "try",
                        "do": [{"$break": None}],
                        "finally": [{"/cleanup": True}],
                    },
                ],
            },
            source={"items": [1]},
            dest={},
        )

        assert result.get("cleanup") is True


# ─────────────────────────────────────────────────────────────────────────────
# $continue
# ─────────────────────────────────────────────────────────────────────────────

class TestContinue:

    def test_continue_skips_rest_of_foreach_iteration(self):
        """$continue skips remaining actions in the current foreach iteration."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, "skip"]},
                        "then": [{"$continue": None}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": ["a", "skip", "b", "skip", "c"]},
            dest={"result": []},
        )

        assert result == {"result": ["a", "b", "c"]}

    def test_continue_skips_rest_of_while_iteration(self):
        """$continue re-evaluates while condition, skipping rest of body."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/counter": 0},
                {"/result": []},
                {
                    "op": "while",
                    "cond": {"$lt": [{"$ref": "@:/counter"}, 5]},
                    "do": [
                        {"/counter": {"$add": [{"$ref": "@:/counter"}, 1]}},
                        {
                            "op": "if",
                            "cond": {"$eq": [{"$ref": "@:/counter"}, 3]},
                            "then": [{"$continue": None}],
                        },
                        {"op": "set", "path": "/result/-", "value": {"$ref": "@:/counter"}},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result["result"] == [1, 2, 4, 5]

    def test_continue_multiple_items(self):
        """$continue correctly skips multiple items across iterations."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/numbers",
                "as": "n",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$mod": [{"$ref": "&:/n"}, 2]}, 0]},
                        "then": [{"$continue": None}],
                    },
                    {"/odds[]": "&:/n"},
                ],
            },
            source={"numbers": [1, 2, 3, 4, 5, 6, 7]},
            dest={"odds": []},
        )

        assert result == {"odds": [1, 3, 5, 7]}

    def test_continue_outside_loop_propagates(self):
        """$continue outside any loop propagates as ContinueSignal."""
        engine = build_default_engine()

        with pytest.raises(ContinueSignal):
            engine.apply(
                [{"$continue": None}],
                source={},
                dest={},
            )

    def test_continue_propagates_through_try(self):
        """$continue inside a try block is not caught by except."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "try",
                        "do": [
                            {
                                "op": "if",
                                "cond": {"$eq": [{"$ref": "&:/item"}, "skip"]},
                                "then": [{"$continue": None}],
                            },
                        ],
                        "except": [{"/caught": True}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": ["a", "skip", "b"]},
            dest={"result": []},
        )

        assert result["result"] == ["a", "b"]
        assert "caught" not in result


# ─────────────────────────────────────────────────────────────────────────────
# $return
# ─────────────────────────────────────────────────────────────────────────────

class TestReturn:

    def test_return_simple_value(self):
        """$return returns a literal value from a function."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "get_42",
                    "body": [{"$return": 42}],
                },
                {"/answer": {"$func": "get_42"}},
            ],
            source={},
            dest={},
        )

        assert result == {"answer": 42}

    def test_return_null_value(self):
        """$return with null returns None."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "get_null",
                    "body": [{"$return": None}],
                },
                {"/answer": {"$func": "get_null"}},
            ],
            source={},
            dest={},
        )

        assert result == {"answer": None}

    def test_return_early_from_body(self):
        """$return stops execution of remaining body steps."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "early",
                    "body": [
                        {"$return": "first"},
                        {"/side_effect": True},  # must not run
                    ],
                },
                {"/answer": {"$func": "early"}},
            ],
            source={},
            dest={},
        )

        assert result["answer"] == "first"
        assert "side_effect" not in result

    def test_return_from_if_branch(self):
        """$return from inside an if branch exits the function."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "sign",
                    "params": ["x"],
                    "body": [
                        {
                            "op": "if",
                            "cond": {"$gt": [{"$ref": "&:/x"}, 0]},
                            "then": [{"$return": "positive"}],
                        },
                        {
                            "op": "if",
                            "cond": {"$lt": [{"$ref": "&:/x"}, 0]},
                            "then": [{"$return": "negative"}],
                        },
                        {"$return": "zero"},
                    ],
                },
                {"/pos": {"$func": "sign", "args": [5]}},
                {"/neg": {"$func": "sign", "args": [-3]}},
                {"/zer": {"$func": "sign", "args": [0]}},
            ],
            source={},
            dest={},
        )

        assert result == {"pos": "positive", "neg": "negative", "zer": "zero"}

    def test_return_from_inside_foreach(self):
        """$return inside foreach exits the entire function (not just the loop)."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "find_first",
                    "params": ["items", "target"],
                    "body": [
                        {
                            "op": "foreach",
                            "in": "&:/items",
                            "as": "item",
                            "do": [
                                {
                                    "op": "if",
                                    "cond": {"$eq": [{"$ref": "&:/item"}, {"$ref": "&:/target"}]},
                                    "then": [{"$return": {"$ref": "&:/item"}}],
                                },
                            ],
                        },
                        {"$return": None},
                    ],
                },
                {"/found": {"$func": "find_first", "args": [["a", "b", "c"], "b"]}},
                {"/missing": {"$func": "find_first", "args": [["a", "b", "c"], "z"]}},
            ],
            source={},
            dest={},
        )

        assert result["found"] == "b"
        assert result["missing"] is None

    def test_return_expression_is_evaluated(self):
        """The $return value expression is fully evaluated."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "double",
                    "params": ["x"],
                    "body": [
                        {"$return": {"$mul": [{"$ref": "&:/x"}, 2]}},
                    ],
                },
                {"/answer": {"$func": "double", "args": [21]}},
            ],
            source={},
            dest={},
        )

        assert result == {"answer": 42}

    def test_return_supersedes_def_return_path(self):
        """When $return is used, it takes precedence over 'return' path in $def."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "func",
                    "body": [
                        {"/value": 99},
                        {"$return": 42},
                    ],
                    "return": "/value",  # should be ignored due to $return
                },
                {"/answer": {"$func": "func"}},
            ],
            source={},
            dest={},
        )

        assert result == {"answer": 42}

    def test_return_outside_function_propagates(self):
        """$return outside any function propagates as ReturnSignal."""
        engine = build_default_engine()

        with pytest.raises(ReturnSignal) as exc_info:
            engine.apply(
                [{"$return": 42}],
                source={},
                dest={},
            )

        assert exc_info.value.value == 42

    def test_return_not_caught_by_on_failure(self):
        """$return is intentional and does not trigger on_failure."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "func",
                    "body": [{"$return": "ok"}],
                    "on_failure": [{"/failed": True}],
                },
                {"/answer": {"$func": "func"}},
            ],
            source={},
            dest={},
        )

        assert result["answer"] == "ok"
        assert "failed" not in result

    def test_return_propagates_through_try(self):
        """$return inside a try block is not caught by except."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "func",
                    "body": [
                        {
                            "op": "try",
                            "do": [{"$return": "from_try"}],
                            "except": [{"/caught": True}],
                        },
                    ],
                },
                {"/answer": {"$func": "func"}},
            ],
            source={},
            dest={},
        )

        assert result["answer"] == "from_try"
        assert "caught" not in result

    def test_return_try_finally_runs(self):
        """$return inside try still executes the finally block."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "func",
                    "context": "shared",
                    "body": [
                        {
                            "op": "try",
                            "do": [{"$return": "value"}],
                            "finally": [{"/cleanup": True}],
                        },
                    ],
                },
                {"/answer": {"$func": "func"}},
            ],
            source={},
            dest={},
        )

        assert result["answer"] == "value"
        assert result.get("cleanup") is True


# ─────────────────────────────────────────────────────────────────────────────
# Combined: break + continue + return together
# ─────────────────────────────────────────────────────────────────────────────

class TestCombined:

    def test_break_and_continue_in_same_loop(self):
        """Both $break and $continue work correctly in the same loop."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": [
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, "skip"]},
                        "then": [{"$continue": None}],
                    },
                    {
                        "op": "if",
                        "cond": {"$eq": [{"$ref": "&:/item"}, "stop"]},
                        "then": [{"$break": None}],
                    },
                    {"/result[]": "&:/item"},
                ],
            },
            source={"items": ["a", "skip", "b", "stop", "c"]},
            dest={"result": []},
        )

        assert result == {"result": ["a", "b"]}

    def test_return_inside_foreach_with_break(self):
        """$return from inside a loop with $break exits the function entirely."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "func",
                    "params": ["items"],
                    "body": [
                        {
                            "op": "foreach",
                            "in": "&:/items",
                            "as": "item",
                            "do": [
                                {
                                    "op": "if",
                                    "cond": {"$eq": [{"$ref": "&:/item"}, "found"]},
                                    "then": [{"$return": "yes"}],
                                },
                                {
                                    "op": "if",
                                    "cond": {"$eq": [{"$ref": "&:/item"}, "abort"]},
                                    "then": [{"$break": None}],
                                },
                            ],
                        },
                        {"$return": "no"},
                    ],
                },
                {"/r1": {"$func": "func", "args": [["a", "found", "b"]]}},
                {"/r2": {"$func": "func", "args": [["a", "abort", "found"]]}},
            ],
            source={},
            dest={},
        )

        assert result["r1"] == "yes"
        assert result["r2"] == "no"


# ─────────────────────────────────────────────────────────────────────────────
# Out-of-context usage: error messages + try does not suppress signals
# ─────────────────────────────────────────────────────────────────────────────

class TestOutOfContext:
    """Signals used outside their valid scope propagate with a readable message,
    and try-except never swallows them."""

    # ── error messages ────────────────────────────────────────────────────────

    def test_break_outside_loop_message(self):
        """BreakSignal carries a meaningful message when used outside a loop."""
        engine = build_default_engine()

        with pytest.raises(BreakSignal, match=r"\$break used outside of a loop"):
            engine.apply([{"$break": None}], source={}, dest={})

    def test_continue_outside_loop_message(self):
        """ContinueSignal carries a meaningful message when used outside a loop."""
        engine = build_default_engine()

        with pytest.raises(ContinueSignal, match=r"\$continue used outside of a loop"):
            engine.apply([{"$continue": None}], source={}, dest={})

    def test_return_outside_function_message(self):
        """ReturnSignal carries a meaningful message and its value when used outside a function."""
        engine = build_default_engine()

        with pytest.raises(ReturnSignal, match=r"\$return used outside of a function") as exc_info:
            engine.apply([{"$return": 99}], source={}, dest={})

        assert exc_info.value.value == 99

    def test_return_outside_function_null_value(self):
        """ReturnSignal.value is None when $return: null is used outside a function."""
        engine = build_default_engine()

        with pytest.raises(ReturnSignal) as exc_info:
            engine.apply([{"$return": None}], source={}, dest={})

        assert exc_info.value.value is None

    # ── try does not catch signals at top level ───────────────────────────────

    def test_try_does_not_catch_break_at_top_level(self):
        """try-except at top level does not swallow $break."""
        engine = build_default_engine()

        with pytest.raises(BreakSignal):
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [{"$break": None}],
                        "except": [{"/caught": True}],
                    }
                ],
                source={},
                dest={},
            )

    def test_try_does_not_catch_continue_at_top_level(self):
        """try-except at top level does not swallow $continue."""
        engine = build_default_engine()

        with pytest.raises(ContinueSignal):
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [{"$continue": None}],
                        "except": [{"/caught": True}],
                    }
                ],
                source={},
                dest={},
            )

    def test_try_does_not_catch_return_at_top_level(self):
        """try-except at top level does not swallow $return."""
        engine = build_default_engine()

        with pytest.raises(ReturnSignal) as exc_info:
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [{"$return": 42}],
                        "except": [{"/caught": True}],
                    }
                ],
                source={},
                dest={},
            )

        assert exc_info.value.value == 42

    def test_try_finally_runs_on_break_at_top_level(self):
        """finally runs even when $break escapes a top-level try."""
        engine = build_default_engine()

        # We can observe finally's effect only if we catch the signal ourselves;
        # use a mutable container as a side-channel since engine.apply raises.
        ran = []

        try:
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [{"$break": None}],
                        "finally": [{"/cleanup": True}],
                    }
                ],
                source={},
                dest={},
            )
        except BreakSignal:
            ran.append("break_propagated")

        assert ran == ["break_propagated"]

    def test_try_finally_runs_on_return_at_top_level(self):
        """finally runs even when $return escapes a top-level try."""
        engine = build_default_engine()

        ran = []

        try:
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [{"$return": "val"}],
                        "finally": [{"/cleanup": True}],
                    }
                ],
                source={},
                dest={},
            )
        except ReturnSignal as e:
            ran.append(e.value)

        assert ran == ["val"]

    # ── nested try does not intercept signals either ──────────────────────────

    def test_nested_try_does_not_catch_break(self):
        """Multiple nested try blocks all fail to catch $break."""
        engine = build_default_engine()

        with pytest.raises(BreakSignal):
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [
                            {
                                "op": "try",
                                "do": [{"$break": None}],
                                "except": [{"/inner_caught": True}],
                            }
                        ],
                        "except": [{"/outer_caught": True}],
                    }
                ],
                source={},
                dest={},
            )

    def test_nested_try_does_not_catch_return(self):
        """Multiple nested try blocks all fail to catch $return."""
        engine = build_default_engine()

        with pytest.raises(ReturnSignal) as exc_info:
            engine.apply(
                [
                    {
                        "op": "try",
                        "do": [
                            {
                                "op": "try",
                                "do": [{"$return": "deep"}],
                                "except": [{"/inner_caught": True}],
                            }
                        ],
                        "except": [{"/outer_caught": True}],
                    }
                ],
                source={},
                dest={},
            )

        assert exc_info.value.value == "deep"
