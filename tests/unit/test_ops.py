"""Tests for all operation handlers."""

import base64
import hashlib

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

    def test_set_append_raises_when_create_false_and_parent_missing(self):
        """set append with create=False raises if parent missing (line 69)."""
        engine = build_default_engine()

        with pytest.raises(Exception):
            engine.apply(
                {"op": "set", "path": "/items/-", "value": "x", "create": False},
                source={},
                dest={},
            )

    def test_set_append_converts_non_list_parent_to_list(self):
        """set converts non-list parent to list when create=True (lines 73-79)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/data/-", "value": "new"},
            source={},
            dest={"data": "existing_string"},
        )

        assert result == {"data": ["existing_string", "new"]}

    def test_set_append_empty_dict_parent_becomes_list(self):
        """Empty dict parent becomes empty list when appending (line 76)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/data/-", "value": "item"},
            source={},
            dest={"data": {}},
        )

        assert result == {"data": ["item"]}

    def test_set_append_raises_non_list_parent_with_create_false(self):
        """set append raises TypeError when parent is not list and create=False (line 81)."""
        engine = build_default_engine()

        with pytest.raises(TypeError):
            engine.apply(
                {"op": "set", "path": "/data/-", "value": "x", "create": False},
                source={},
                dest={"data": "string"},
            )

    def test_set_append_extends_when_value_is_list(self):
        """set extends parent list when value is list and extend=True (line 85)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "set", "path": "/items/-", "value": [4, 5], "extend": True},
            source={},
            dest={"items": [1, 2, 3]},
        )

        assert result == {"items": [1, 2, 3, 4, 5]}


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

    def test_delete_path_with_dash_raises(self):
        """Delete with '/-' suffix raises ValueError (line 167)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="'-' not allowed in delete"):
            engine.apply(
                {"op": "delete", "path": "/items/-"},
                source={},
                dest={"items": [1, 2]},
            )

    def test_delete_raises_on_missing_with_ignore_false(self):
        """Delete raises when key missing and ignore_missing=False (lines 171-173)."""
        engine = build_default_engine()

        with pytest.raises(Exception):
            engine.apply(
                {"op": "delete", "path": "/missing", "ignore_missing": False},
                source={},
                dest={},
            )


class TestForeachOperation:
    """Test 'foreach' operation."""

    def test_foreach_basic(self):
        """Iterate over array."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in": "/items", "do": {"/out[]": "&:/item"}},
            source={"items": [1, 2, 3]},
            dest={},
        )

        assert result == {"out": [1, 2, 3]}

    def test_foreach_with_custom_var(self):
        """Custom variable name."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in": "/items", "as": "x", "do": {"/out[]": "&:/x"}},
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

    def test_foreach_in_value_basic(self):
        """in_value passes array directly without pointer resolution."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in_value": [10, 20, 30], "do": {"/out[]": "&:/item"}},
            source={},
            dest={},
        )

        assert result == {"out": [10, 20, 30]}

    def test_foreach_in_value_template(self):
        """in_value supports template expressions."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in_value": "${/tags}", "as": "tag", "do": {"/out[]": "&:/tag"}},
            source={"tags": ["a", "b"]},
            dest={},
        )

        assert result == {"out": ["a", "b"]}

    def test_foreach_in_value_skip_empty(self):
        """in_value respects skip_empty."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "foreach", "in_value": [], "do": {"/out[]": "&:/item"}},
            source={},
            dest={"pre": "existing"},
        )

        assert result == {"pre": "existing"}

    def test_foreach_both_in_params_raises(self):
        """Providing both 'in' and 'in_value' raises ValueError."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="cannot have both"):
            engine.apply(
                {"op": "foreach", "in": "/items", "in_value": [1, 2], "do": {"/out[]": "&:/item"}},
                source={"items": [1, 2]},
                dest={},
            )

    def test_foreach_no_in_params_raises(self):
        """Providing neither 'in' nor 'in_value' raises ValueError."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires either"):
            engine.apply(
                {"op": "foreach", "do": {"/out[]": "&:/item"}},
                source={},
                dest={},
            )

    def test_foreach_dict_source_iterates_items(self):
        """Dict source is converted to (key, value) tuples (line 232)."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/data",
                "do": {"/keys[]": "&:/item/0"},
            },
            source={"data": {"a": 1, "b": 2}},
            dest={},
        )

        assert set(result["keys"]) == {"a", "b"}

    def test_foreach_uses_default_when_pointer_fails(self):
        """foreach falls back to default when 'in' pointer fails (lines 225-226)."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/missing",
                "default": [10, 20],
                "do": {"/items[]": "&:/item"},
            },
            source={},
            dest={},
        )

        assert result == {"items": [10, 20]}


class TestIfOperation:
    """Test 'if' operation."""

    def test_if_path_exists(self):
        """Execute 'then' if path exists."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "if", "path": "@:/check", "exists": True, "then": {"/result": "yes"}},
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

    def test_if_path_truthy_check(self):
        """if with path alone checks truthiness (line 399 branch)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "if", "path": "/flag", "then": {"/ok": True}},
            source={"flag": True},
            dest={},
        )

        assert result == {"ok": True}

    def test_if_with_no_matching_branch_returns_dest_unchanged(self):
        """if with no matching branch key returns dest unchanged (line 409)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "if", "cond": False},
            source={},
            dest={"keep": "me"},
        )

        assert result == {"keep": "me"}


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

    def test_exec_cannot_have_both_from_and_actions(self):
        """exec raises when both 'from' and 'actions' provided (line 451)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="cannot have both"):
            engine.apply(
                {"op": "exec", "from": "/acts", "actions": {"/x": 1}},
                source={"acts": {"/x": 1}},
                dest={},
            )

    def test_exec_requires_from_or_actions(self):
        """exec raises when neither 'from' nor 'actions' provided (line 453)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires either"):
            engine.apply(
                {"op": "exec"},
                source={},
                dest={},
            )

    def test_exec_from_missing_with_default(self):
        """exec uses default when 'from' pointer fails and default provided (lines 460-461)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "exec", "from": "/missing", "default": {"/fallback": True}},
            source={},
            dest={},
        )

        assert result == {"fallback": True}

    def test_exec_from_missing_without_default_raises(self):
        """exec raises when 'from' pointer fails and no default (line 463)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="Cannot find actions"):
            engine.apply(
                {"op": "exec", "from": "/missing"},
                source={},
                dest={},
            )

    def test_exec_actions_processed(self):
        """exec with 'actions' key processes value through engine (line 465)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "exec", "actions": {"/x": "${/val}"}},
            source={"val": 42},
            dest={},
        )

        assert result == {"x": 42}


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

    def test_update_from_pointer_with_default(self):
        """update uses 'default' when 'from' pointer fails (lines 508-513)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "update", "path": "/obj", "from": "/missing", "default": {"fallback": True}},
            source={},
            dest={"obj": {"a": 1}},
        )

        assert result == {"obj": {"a": 1, "fallback": True}}

    def test_update_from_pointer_raises_on_missing(self):
        """update raises when 'from' pointer fails and no default (lines 514-515)."""
        engine = build_default_engine()

        with pytest.raises(Exception):
            engine.apply(
                {"op": "update", "path": "/obj", "from": "/missing"},
                source={},
                dest={"obj": {}},
            )

    def test_update_requires_from_or_value(self):
        """update raises when neither 'from' nor 'value' is provided (line 519)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires either 'from' or 'value'"):
            engine.apply(
                {"op": "update", "path": "/obj"},
                source={},
                dest={"obj": {}},
            )

    def test_update_creates_target_when_missing(self):
        """update creates target dict when missing with create=True (lines 527-530)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "update", "path": "/new_obj", "value": {"x": 1}},
            source={},
            dest={},
        )

        assert result == {"new_obj": {"x": 1}}

    def test_update_raises_when_target_not_dict(self):
        """update raises TypeError when target is not a dict (line 535)."""
        engine = build_default_engine()

        with pytest.raises(TypeError):
            engine.apply(
                {"op": "update", "path": "/arr", "value": {"x": 1}},
                source={},
                dest={"arr": [1, 2, 3]},
            )

    def test_update_raises_when_create_false_and_missing(self):
        """update raises KeyError when target missing and create=False (line 532)."""
        engine = build_default_engine()

        with pytest.raises((KeyError, Exception)):
            engine.apply(
                {"op": "update", "path": "/missing", "value": {"x": 1}, "create": False},
                source={},
                dest={},
            )


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

    def test_distinct_with_key(self):
        """distinct with 'key' deduplicates by field (line 574)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "distinct", "path": "/arr", "key": "/id"},
            source={},
            dest={"arr": [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}]},
        )

        assert len(result["arr"]) == 2
        assert result["arr"][0]["id"] == 1
        assert result["arr"][1]["id"] == 2

    def test_distinct_with_unhashable_items(self):
        """distinct handles unhashable items by always including them (line 578)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "distinct", "path": "/arr"},
            source={},
            dest={"arr": [{"a": 1}, {"b": 2}, {"a": 1}]},
        )

        assert len(result["arr"]) == 3


class TestWhileOperation:
    """Test 'while' operation."""

    def test_while_with_cond(self):
        """While loop with path-based condition checking dest."""
        engine = build_default_engine()

        # Use path-based condition to check dest
        result = engine.apply(
            [
                {"op": "set", "path": "/run", "value": True},
                {"op": "set", "path": "/counter", "value": 0},
                {
                    "op": "while",
                    "path": "@:/run",
                    "equals": True,
                    "do": [
                        {"op": "set", "path": "/counter", "value": 1},
                        {"op": "set", "path": "/run", "value": False},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result == {"run": False, "counter": 1}

    def test_while_with_path_equals(self):
        """While loop checking path equality."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"op": "set", "path": "/status", "value": "running"},
                {"op": "set", "path": "/count", "value": 0},
                {
                    "op": "while",
                    "path": "@:/status",
                    "equals": "running",
                    "do": [
                        {"op": "set", "path": "/count", "value": 1},
                        {"op": "set", "path": "/status", "value": "done"},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result == {"status": "done", "count": 1}

    def test_while_do_while(self):
        """While with do_while executes at least once."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "while",
                "path": "/never_true",
                "equals": True,
                "do_while": True,
                "do": {"op": "set", "path": "/executed", "value": True},
            },
            source={},
            dest={},
        )

        assert result == {"executed": True}

    def test_while_with_path_exists_condition(self):
        """while with path+exists checks existence (line 325-326)."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"op": "set", "path": "/flag", "value": True},
                {
                    "op": "while",
                    "path": "@:/flag",
                    "exists": True,
                    "do": [
                        {"op": "set", "path": "/ran", "value": True},
                        {"op": "delete", "path": "/flag"},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result.get("ran") is True
        assert "flag" not in result

    def test_while_requires_cond_or_path(self):
        """while raises when neither 'cond' nor 'path' provided (line 330)."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires 'cond' or 'path'"):
            engine.apply(
                {"op": "while", "do": {"/x": 1}},
                source={},
                dest={},
            )

    def test_while_propagates_return_signal(self):
        """while propagates ReturnSignal without rollback (line 347)."""
        from j_perm import ReturnSignal

        engine = build_default_engine()

        with pytest.raises(ReturnSignal):
            engine.apply(
                [
                    {"op": "set", "path": "/counter", "value": 0},
                    {
                        "op": "while",
                        "cond": True,
                        "do": [{"$return": None}],
                    },
                ],
                source={},
                dest={},
            )


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

    def test_assert_with_return(self):
        """Assert with return mode returns value instead of raising."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "path": "/missing", "return": True},
            source={},
            dest={"existing": "data"},
        )

        assert result == False

    def test_assert_with_return_and_to_path(self):
        """Assert with return and to_path sets value at destination."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "path": "/x", "equals": 10, "return": True, "to_path": "/result"},
            source={"x": 10},
            dest={},
        )

        assert result == {"result": 10}

    def test_assert_with_return_on_missing(self):
        """Assert with return on missing path returns False."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "path": "/missing", "return": True, "to_path": "/result"},
            source={},
            dest={},
        )

        assert result == {"result": False}

    def test_assert_with_value(self):
        """Assert with direct value instead of path."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "value": "test", "equals": "test"},
            source={},
            dest={},
        )

        assert result == {}

    def test_assert_value_fails_on_mismatch(self):
        """Assert with value fails on mismatch."""
        engine = build_default_engine()

        with pytest.raises(AssertionError, match="Value !="):
            engine.apply(
                {"op": "assert", "value": "test", "equals": "other"},
                source={},
                dest={},
            )

    def test_assert_value_with_return(self):
        """Assert with value and return mode."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "value": 42, "equals": 42, "return": True, "to_path": "/result"},
            source={},
            dest={},
        )

        assert result == {"result": 42}

    def test_assert_requires_path_or_value(self):
        """Assert requires either path or value."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires either 'path' or 'value'"):
            engine.apply(
                {"op": "assert", "equals": "test"},
                source={},
                dest={},
            )

    def test_assert_cannot_have_both_path_and_value(self):
        """Assert cannot have both path and value."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="cannot have both 'path' and 'value'"):
            engine.apply(
                {"op": "assert", "path": "/x", "value": "test"},
                source={},
                dest={},
            )

    def test_assert_equals_mismatch_with_return(self):
        """Assert with equals mismatch and return=True returns False (line 664)."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "assert", "path": "/x", "equals": 100, "return": True},
            source={"x": 42},
            dest={"existing": "data"},
        )

        assert result is False


class TestWhileAdditional:
    """Additional while tests for uncovered branches."""

    def test_while_path_truthiness_check(self):
        """while with path alone checks bool(current) (line 328)."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"op": "set", "path": "/flag", "value": "yes"},
                {
                    "op": "while",
                    "path": "@:/flag",
                    "do": [
                        {"op": "set", "path": "/ran", "value": True},
                        {"op": "set", "path": "/flag", "value": ""},
                    ],
                },
            ],
            source={},
            dest={},
        )

        assert result.get("ran") is True
        assert result.get("flag") == ""


class TestIfAdditional:
    """Additional if tests for uncovered branches."""

    def test_if_path_equals_check(self):
        """if with path+equals checks value equality (lines 394-395)."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "if",
                "path": "/status",
                "equals": "active",
                "then": {"/result": "yes"},
                "else": {"/result": "no"},
            },
            source={"status": "active"},
            dest={},
        )

        assert result == {"result": "yes"}


class TestUpdateAdditional:
    """Additional update tests for uncovered branches."""

    def test_update_with_non_mapping_value_raises(self):
        """update raises TypeError when value is not a mapping (line 522)."""
        engine = build_default_engine()

        with pytest.raises(TypeError, match="must be a dict"):
            engine.apply(
                {"op": "update", "path": "/obj", "value": [1, 2, 3]},
                source={},
                dest={"obj": {}},
            )


class TestDistinctAdditional:
    """Additional distinct tests for uncovered branches."""

    def test_distinct_raises_for_non_list(self):
        """distinct raises TypeError when path is not a list (line 574)."""
        engine = build_default_engine()

        with pytest.raises(TypeError, match="is not a list"):
            engine.apply(
                {"op": "distinct", "path": "/data"},
                source={},
                dest={"data": "not_a_list"},
            )


class TestDeserializeOperation:
    """Test 'deserialize' operation."""

    def test_deserialize_json_from_pointer(self):
        """Parse JSON string from source pointer."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "from": "/data", "format": "json", "path": "/parsed"},
            source={"data": '{"name": "Alice", "age": 30}'},
            dest={},
        )

        assert result == {"parsed": {"name": "Alice", "age": 30}}

    def test_deserialize_json_inline_value(self):
        """Parse JSON string passed as inline value."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "value": '[1, 2, 3]', "format": "json", "path": "/items"},
            source={},
            dest={},
        )

        assert result == {"items": [1, 2, 3]}

    def test_deserialize_pretty_json(self):
        """'pretty_json' format is an alias for 'json'."""
        engine = build_default_engine()

        pretty = '{\n    "key": "value"\n}'
        result = engine.apply(
            {"op": "deserialize", "value": pretty, "format": "pretty_json", "path": "/obj"},
            source={},
            dest={},
        )

        assert result == {"obj": {"key": "value"}}

    def test_deserialize_yaml_from_pointer(self):
        """Parse YAML string from source pointer."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "from": "/raw", "format": "yaml", "path": "/doc"},
            source={"raw": "name: Bob\nage: 25\n"},
            dest={},
        )

        assert result == {"doc": {"name": "Bob", "age": 25}}

    def test_deserialize_yaml_inline_value(self):
        """Parse YAML string passed as inline value."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "value": "- a\n- b\n- c\n", "format": "yaml", "path": "/list"},
            source={},
            dest={},
        )

        assert result == {"list": ["a", "b", "c"]}

    def test_deserialize_default_format_is_json(self):
        """When 'format' is omitted, JSON is used."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "value": '{"x": 1}', "path": "/obj"},
            source={},
            dest={},
        )

        assert result == {"obj": {"x": 1}}

    def test_deserialize_default_on_missing_pointer(self):
        """Use 'default' when source pointer does not exist."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "from": "/missing", "format": "json",
             "path": "/obj", "default": {"fallback": True}},
            source={},
            dest={},
        )

        assert result == {"obj": {"fallback": True}}

    def test_deserialize_default_on_parse_error(self):
        """Use 'default' when the string cannot be parsed."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "value": "not valid json", "format": "json",
             "path": "/obj", "default": None},
            source={},
            dest={},
        )

        assert result == {"obj": None}

    def test_deserialize_raises_on_missing_pointer_without_default(self):
        """Raise when source pointer is missing and no default provided."""
        engine = build_default_engine()

        with pytest.raises(Exception):
            engine.apply(
                {"op": "deserialize", "from": "/missing", "format": "json", "path": "/obj"},
                source={},
                dest={},
            )

    def test_deserialize_raises_on_invalid_json_without_default(self):
        """Raise ValueError when JSON is invalid and no default provided."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="failed to parse as 'json'"):
            engine.apply(
                {"op": "deserialize", "value": "{bad json}", "format": "json", "path": "/obj"},
                source={},
                dest={},
            )

    def test_deserialize_raises_on_unknown_format(self):
        """Raise ValueError for unsupported format name."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="unknown format"):
            engine.apply(
                {"op": "deserialize", "value": "{}", "format": "toml", "path": "/obj"},
                source={},
                dest={},
            )

    def test_deserialize_requires_from_or_value(self):
        """Raise when neither 'from' nor 'value' is provided."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="requires either 'from' or 'value'"):
            engine.apply(
                {"op": "deserialize", "format": "json", "path": "/obj"},
                source={},
                dest={},
            )

    def test_deserialize_cannot_have_both_from_and_value(self):
        """Raise when both 'from' and 'value' are provided."""
        engine = build_default_engine()

        with pytest.raises(ValueError, match="cannot have both 'from' and 'value'"):
            engine.apply(
                {"op": "deserialize", "from": "/data", "value": "{}", "format": "json", "path": "/obj"},
                source={"data": "{}"},
                dest={},
            )

    def test_deserialize_with_context_prefix_in_from(self):
        """'from' supports context prefixes like '@:' (dest)."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"op": "set", "path": "/raw", "value": '{"ok": true}'},
                {"op": "deserialize", "from": "@:/raw", "format": "json", "path": "/parsed"},
            ],
            source={},
            dest={},
        )

        assert result == {"raw": '{"ok": true}', "parsed": {"ok": True}}

    def test_deserialize_writes_to_nested_path_with_autocreate(self):
        """Auto-create intermediate nodes when writing to nested path."""
        engine = build_default_engine()

        result = engine.apply(
            {"op": "deserialize", "value": '{"v": 1}', "format": "json", "path": "/a/b/parsed"},
            source={},
            dest={},
        )

        assert result == {"a": {"b": {"parsed": {"v": 1}}}}


class TestSerializeOperation:
    """Test 'serialize' operation."""

    def test_serialize_json_compact(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "value": {"b": 1, "a": 2}, "format": "json", "path": "/s"},
            source={}, dest={},
        )
        assert result == {"s": '{"b":1,"a":2}'}

    def test_serialize_from_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "from": "/data", "format": "json", "path": "/s"},
            source={"data": [1, 2, 3]}, dest={},
        )
        assert result == {"s": "[1,2,3]"}

    def test_serialize_pretty_json(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "value": {"k": "v"}, "format": "pretty_json", "path": "/s"},
            source={}, dest={},
        )
        assert result == {"s": '{\n  "k": "v"\n}'}

    def test_serialize_yaml(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "value": {"a": 1}, "format": "yaml", "path": "/s"},
            source={}, dest={},
        )
        assert result == {"s": "a: 1\n"}

    def test_serialize_default_format_is_json(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "value": {"x": 1}, "path": "/s"},
            source={}, dest={},
        )
        assert result == {"s": '{"x":1}'}

    def test_serialize_unicode_not_escaped(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "value": "мир", "format": "json", "path": "/s"},
            source={}, dest={},
        )
        assert result == {"s": '"мир"'}

    def test_serialize_default_on_missing_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "from": "/missing", "path": "/s", "default": "fallback"},
            source={}, dest={},
        )
        assert result == {"s": "fallback"}

    def test_serialize_default_on_render_error(self):
        """Non-JSON-serializable value falls back to default."""
        engine = build_default_engine()
        result = engine.apply(
            {"op": "serialize", "value": {"1", "2"}, "format": "json",
             "path": "/s", "default": "err"},
            source={}, dest={},
        )
        assert result == {"s": "err"}

    def test_serialize_raises_on_missing_pointer_without_default(self):
        engine = build_default_engine()
        with pytest.raises(Exception):
            engine.apply(
                {"op": "serialize", "from": "/missing", "path": "/s"},
                source={}, dest={},
            )

    def test_serialize_raises_on_render_error_without_default(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="failed to render as 'json'"):
            engine.apply(
                {"op": "serialize", "value": {"1", "2"}, "format": "json", "path": "/s"},
                source={}, dest={},
            )

    def test_serialize_raises_on_unknown_format(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="unknown format"):
            engine.apply(
                {"op": "serialize", "value": {}, "format": "toml", "path": "/s"},
                source={}, dest={},
            )

    def test_serialize_requires_from_or_value(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="requires either 'from' or 'value'"):
            engine.apply({"op": "serialize", "path": "/s"}, source={}, dest={})

    def test_serialize_cannot_have_both_from_and_value(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="cannot have both 'from' and 'value'"):
            engine.apply(
                {"op": "serialize", "from": "/d", "value": 1, "path": "/s"},
                source={"d": 1}, dest={},
            )

    def test_serialize_roundtrip_with_deserialize(self):
        engine = build_default_engine()
        result = engine.apply(
            [
                {"op": "serialize", "from": "/obj", "format": "yaml", "path": "/text"},
                {"op": "deserialize", "from": "@:/text", "format": "yaml", "path": "/back"},
            ],
            source={"obj": {"a": 1, "b": [2, 3]}}, dest={},
        )
        assert result["back"] == {"a": 1, "b": [2, 3]}


class TestEncodeDecodeOperation:
    """Test 'encode' and 'decode' operations."""

    ALL_CODECS = ["base64", "base64url", "base32", "base16", "hex",
                  "base85", "ascii85", "url"]

    @pytest.mark.parametrize("codec", ALL_CODECS)
    def test_encode_decode_roundtrip(self, codec):
        engine = build_default_engine()
        text = "Привет, мир! /?+="
        enc = engine.apply(
            {"op": "encode", "value": text, "codec": codec, "path": "/o"},
            source={}, dest={},
        )["o"]
        assert isinstance(enc, str)
        dec = engine.apply(
            {"op": "decode", "value": enc, "codec": codec, "path": "/o"},
            source={}, dest={},
        )["o"]
        assert dec == text

    def test_encode_base64_known_value(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "value": "hello", "codec": "base64", "path": "/o"},
            source={}, dest={},
        )
        assert result == {"o": "aGVsbG8="}

    def test_encode_default_codec_is_base64(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "value": "hello", "path": "/o"},
            source={}, dest={},
        )
        assert result == {"o": "aGVsbG8="}

    def test_encode_hex_is_lowercase(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "value": "AZ", "codec": "hex", "path": "/o"},
            source={}, dest={},
        )
        assert result == {"o": "415a"}

    def test_encode_base16_is_uppercase(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "value": "AZ", "codec": "base16", "path": "/o"},
            source={}, dest={},
        )
        assert result == {"o": "415A"}

    def test_encode_url_percent(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "value": "a b/c", "codec": "url", "path": "/o"},
            source={}, dest={},
        )
        assert result == {"o": "a%20b%2Fc"}

    def test_encode_custom_encoding(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "value": "é", "codec": "hex", "encoding": "latin-1", "path": "/o"},
            source={}, dest={},
        )
        assert result == {"o": "e9"}

    def test_encode_from_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "from": "/text", "codec": "base64", "path": "/o"},
            source={"text": "hello"}, dest={},
        )
        assert result == {"o": "aGVsbG8="}

    def test_decode_from_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "decode", "from": "/enc", "codec": "base64", "path": "/o"},
            source={"enc": "aGVsbG8="}, dest={},
        )
        assert result == {"o": "hello"}

    def test_encode_default_on_missing_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "encode", "from": "/missing", "path": "/o", "default": "FB"},
            source={}, dest={},
        )
        assert result == {"o": "FB"}

    def test_decode_default_on_decode_error(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "decode", "value": "!!!not base64!!!", "codec": "base64",
             "path": "/o", "default": "FB"},
            source={}, dest={},
        )
        assert result == {"o": "FB"}

    def test_decode_raises_on_decode_error_without_default(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="failed to decode as 'base64'"):
            engine.apply(
                {"op": "decode", "value": "!!!not base64!!!", "codec": "base64", "path": "/o"},
                source={}, dest={},
            )

    def test_encode_raises_on_encode_error_without_default(self):
        """A non-ascii-encodable char under a byte-limited encoding fails."""
        engine = build_default_engine()
        with pytest.raises(ValueError, match="failed to encode as 'base64'"):
            engine.apply(
                {"op": "encode", "value": "мир", "codec": "base64",
                 "encoding": "ascii", "path": "/o"},
                source={}, dest={},
            )

    def test_encode_raises_on_missing_pointer_without_default(self):
        engine = build_default_engine()
        with pytest.raises(Exception):
            engine.apply(
                {"op": "encode", "from": "/missing", "path": "/o"},
                source={}, dest={},
            )

    def test_encode_raises_on_unknown_codec(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="unknown codec"):
            engine.apply(
                {"op": "encode", "value": "x", "codec": "base9999", "path": "/o"},
                source={}, dest={},
            )

    def test_decode_raises_on_unknown_codec(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="unknown codec"):
            engine.apply(
                {"op": "decode", "value": "x", "codec": "base9999", "path": "/o"},
                source={}, dest={},
            )

    def test_encode_requires_from_or_value(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="requires either 'from' or 'value'"):
            engine.apply({"op": "encode", "path": "/o"}, source={}, dest={})

    def test_encode_cannot_have_both_from_and_value(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="cannot have both 'from' and 'value'"):
            engine.apply(
                {"op": "encode", "from": "/t", "value": "x", "path": "/o"},
                source={"t": "x"}, dest={},
            )


class TestHashOperation:
    """Test 'hash' operation."""

    def test_hash_string_sha256(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "value": "abc", "algo": "sha256", "path": "/h"},
            source={}, dest={},
        )
        assert result == {"h": hashlib.sha256(b"abc").hexdigest()}

    def test_hash_default_algo_is_sha256(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "value": "abc", "path": "/h"},
            source={}, dest={},
        )
        assert result == {"h": hashlib.sha256(b"abc").hexdigest()}

    @pytest.mark.parametrize("algo", [
        "sha256", "sha512", "sha1", "md5", "sha3_256", "sha3_512", "blake2b", "blake2s",
    ])
    def test_hash_all_algos(self, algo):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "value": "data", "algo": algo, "path": "/h"},
            source={}, dest={},
        )
        assert result == {"h": hashlib.new(algo, b"data").hexdigest()}

    def test_hash_object_is_canonical(self):
        """Equal objects hash equally regardless of key order."""
        engine = build_default_engine()
        h1 = engine.apply(
            {"op": "hash", "value": {"a": 1, "b": 2}, "path": "/h"}, source={}, dest={},
        )["h"]
        h2 = engine.apply(
            {"op": "hash", "value": {"b": 2, "a": 1}, "path": "/h"}, source={}, dest={},
        )["h"]
        assert h1 == h2
        import json
        expected = hashlib.sha256(
            json.dumps({"a": 1, "b": 2}, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        assert h1 == expected

    def test_hash_output_base64(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "value": "abc", "output": "base64", "path": "/h"},
            source={}, dest={},
        )
        assert result == {"h": base64.b64encode(hashlib.sha256(b"abc").digest()).decode()}

    def test_hash_output_base64url(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "value": "abc", "output": "base64url", "path": "/h"},
            source={}, dest={},
        )
        assert result == {"h": base64.urlsafe_b64encode(hashlib.sha256(b"abc").digest()).decode()}

    def test_hash_custom_encoding(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "value": "é", "encoding": "latin-1", "path": "/h"},
            source={}, dest={},
        )
        assert result == {"h": hashlib.sha256("é".encode("latin-1")).hexdigest()}

    def test_hash_from_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "from": "/data", "path": "/h"},
            source={"data": "abc"}, dest={},
        )
        assert result == {"h": hashlib.sha256(b"abc").hexdigest()}

    def test_hash_default_on_missing_pointer(self):
        engine = build_default_engine()
        result = engine.apply(
            {"op": "hash", "from": "/missing", "path": "/h", "default": "none"},
            source={}, dest={},
        )
        assert result == {"h": "none"}

    def test_hash_raises_on_missing_pointer_without_default(self):
        engine = build_default_engine()
        with pytest.raises(Exception):
            engine.apply({"op": "hash", "from": "/missing", "path": "/h"}, source={}, dest={})

    def test_hash_raises_on_unknown_algo(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="unknown algo"):
            engine.apply(
                {"op": "hash", "value": "x", "algo": "sha999", "path": "/h"},
                source={}, dest={},
            )

    def test_hash_raises_on_unknown_output(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="unknown output"):
            engine.apply(
                {"op": "hash", "value": "x", "output": "octal", "path": "/h"},
                source={}, dest={},
            )

    def test_hash_requires_from_or_value(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="requires either 'from' or 'value'"):
            engine.apply({"op": "hash", "path": "/h"}, source={}, dest={})

    def test_hash_cannot_have_both_from_and_value(self):
        engine = build_default_engine()
        with pytest.raises(ValueError, match="cannot have both 'from' and 'value'"):
            engine.apply(
                {"op": "hash", "from": "/d", "value": "x", "path": "/h"},
                source={"d": "x"}, dest={},
            )

    def test_hash_then_assert_checksum(self):
        engine = build_default_engine()
        expected = hashlib.sha256(b"data").hexdigest()
        result = engine.apply(
            [
                {"op": "hash", "from": "/payload", "algo": "sha256", "path": "/actual"},
                {"op": "assert", "path": "@:/actual", "equals": "${/expected}"},
            ],
            source={"payload": "data", "expected": expected}, dest={},
        )
        assert result["actual"] == expected

    def test_hash_then_assert_checksum_mismatch_raises(self):
        engine = build_default_engine()
        with pytest.raises(AssertionError):
            engine.apply(
                [
                    {"op": "hash", "from": "/payload", "algo": "sha256", "path": "/actual"},
                    {"op": "assert", "path": "@:/actual", "equals": "${/expected}"},
                ],
                source={"payload": "data", "expected": "wrong"}, dest={},
            )

