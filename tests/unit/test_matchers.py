"""Tests for matcher classes."""

import pytest
from j_perm import OpMatcher, AlwaysMatcher


class TestOpMatcher:
    """Test OpMatcher."""

    def test_matches_correct_op(self):
        """OpMatcher matches step with correct op."""
        matcher = OpMatcher("set")

        assert matcher.matches({"op": "set"}) is True
        assert matcher.matches({"op": "copy"}) is False
        assert matcher.matches({"op": "set", "path": "/x"}) is True

    def test_rejects_non_dict(self):
        """OpMatcher rejects non-dict steps."""
        matcher = OpMatcher("set")

        assert matcher.matches("not a dict") is False
        assert matcher.matches(123) is False
        assert matcher.matches(None) is False

    def test_rejects_dict_without_op(self):
        """OpMatcher rejects dict without 'op' key."""
        matcher = OpMatcher("set")

        assert matcher.matches({"path": "/x"}) is False
        assert matcher.matches({}) is False


class TestAlwaysMatcher:
    """Test AlwaysMatcher."""

    def test_always_matches(self):
        """AlwaysMatcher always returns True."""
        matcher = AlwaysMatcher()

        assert matcher.matches({}) is True
        assert matcher.matches({"op": "anything"}) is True
        assert matcher.matches("string") is True
        assert matcher.matches(123) is True
        assert matcher.matches(None) is True
        assert matcher.matches([1, 2, 3]) is True
