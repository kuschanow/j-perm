"""Tests for construct_groups module."""

import pytest
from j_perm import build_default_engine
from j_perm.construct_groups import get_all_handlers, get_all_handlers_with_limits


class TestGetAllHandlers:
    """Test get_all_handlers() function."""

    def test_returns_dict_with_cast(self):
        """get_all_handlers() returns dict including $cast."""
        handlers = get_all_handlers()
        assert "$cast" in handlers
        assert "$ref" in handlers
        assert "$eval" in handlers
        assert "$add" in handlers
        assert "$str_split" in handlers
        assert "$regex_match" in handlers

    def test_cast_with_default_casters(self):
        """$cast handler works with default casters."""
        handlers = get_all_handlers()
        engine = build_default_engine(specials=handlers)

        result = engine.apply(
            {"/n": {"$cast": {"value": "42", "type": "int"}}},
            source={},
            dest={},
        )
        assert result == {"n": 42}

    def test_cast_with_custom_casters(self):
        """$cast handler uses provided custom casters."""
        handlers = get_all_handlers(casters={"upper": str.upper})
        engine = build_default_engine(specials=handlers)

        result = engine.apply(
            {"/s": {"$cast": {"value": "hello", "type": "upper"}}},
            source={},
            dest={},
        )
        assert result == {"s": "HELLO"}


class TestGetAllHandlersWithLimits:
    """Test get_all_handlers_with_limits() function."""

    def test_returns_dict_with_all_handlers(self):
        """get_all_handlers_with_limits() returns dict with all constructs."""
        handlers = get_all_handlers_with_limits()
        assert "$cast" in handlers
        assert "$add" in handlers
        assert "$mul" in handlers
        assert "$pow" in handlers
        assert "$str_split" in handlers
        assert "$regex_match" in handlers

    def test_custom_limits_are_applied(self):
        """Custom limits passed to get_all_handlers_with_limits are enforced."""
        handlers = get_all_handlers_with_limits(pow_max_exponent=5)
        engine = build_default_engine(specials=handlers)

        with pytest.raises(ValueError, match="Exponent value"):
            engine.apply(
                {"/r": {"$pow": [2, 100]}},
                source={},
                dest={},
            )

    def test_with_custom_casters(self):
        """$cast handler in get_all_handlers_with_limits uses provided casters."""
        handlers = get_all_handlers_with_limits(casters={"double": lambda x: x * 2})
        engine = build_default_engine(specials=handlers)

        result = engine.apply(
            {"/r": {"$cast": {"value": 5, "type": "double"}}},
            source={},
            dest={},
        )
        assert result == {"r": 10}