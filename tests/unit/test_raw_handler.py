"""Tests for $raw construct and $raw: True flag."""

import pytest
from j_perm import build_default_engine, RawValueSignal, PipelineSignal


@pytest.fixture
def engine():
    return build_default_engine()


# ---------------------------------------------------------------------------
# $raw as wrapper construct
# ---------------------------------------------------------------------------

class TestRawWrapperConstruct:
    """{"$raw": <literal>} returns the literal without any processing."""

    def test_raw_returns_dict_that_looks_like_construct(self, engine):
        """$raw returns a dict verbatim — the $ref inside is not evaluated."""
        result = engine.apply(
            {"/result": {"$raw": {"$ref": "/data"}}},
            source={"data": "value"},
            dest={},
        )
        assert result == {"result": {"$ref": "/data"}}

    def test_raw_returns_string_with_template(self, engine):
        """$raw returns a string with ${...} — template not substituted."""
        result = engine.apply(
            {"/result": {"$raw": "hello ${name}"}},
            source={"name": "Alice"},
            dest={},
        )
        assert result == {"result": "hello ${name}"}

    def test_raw_returns_list_with_constructs(self, engine):
        """$raw returns a list whose elements are not processed."""
        result = engine.apply(
            {"/result": {"$raw": [{"$ref": "/a"}, {"$ref": "/b"}]}},
            source={"a": 1, "b": 2},
            dest={},
        )
        assert result == {"result": [{"$ref": "/a"}, {"$ref": "/b"}]}

    def test_raw_returns_scalar(self, engine):
        """$raw with a plain scalar just returns it."""
        result = engine.apply(
            {"/result": {"$raw": 42}},
            source={},
            dest={},
        )
        assert result == {"result": 42}

    def test_raw_returns_none(self, engine):
        """$raw with None returns None."""
        result = engine.apply(
            {"/result": {"$raw": None}},
            source={},
            dest={},
        )
        assert result == {"result": None}

    def test_raw_outer_stops_inner_from_being_evaluated(self, engine):
        """Outer $raw shields inner $raw dict from evaluation."""
        result = engine.apply(
            {"/result": {"$raw": {"$raw": {"$ref": "/x"}}}},
            source={"x": 99},
            dest={},
        )
        # Outer $raw returns inner dict as literal: {"$raw": {"$ref": "/x"}}
        assert result == {"result": {"$raw": {"$ref": "/x"}}}


# ---------------------------------------------------------------------------
# $raw: True as flag on special constructs
# ---------------------------------------------------------------------------

class TestRawFlag:
    """$raw: True on a construct stops further value-pipeline iterations."""

    def test_flag_on_ref_stops_chain_resolution(self, engine):
        """$ref resolves one level; $raw: True stops further resolution."""
        # source["/a"] is a literal construct — $ref will return it as a dict,
        # then $raw: True prevents the stabilisation loop from resolving it further.
        result = engine.apply(
            {"/result": {"$ref": "/a", "$raw": True}},
            source={"a": {"$ref": "/b"}, "b": "final"},
            dest={},
        )
        assert result == {"result": {"$ref": "/b"}}

    def test_without_raw_flag_chain_is_fully_resolved(self, engine):
        """Without $raw: True the stabilisation loop resolves the full chain."""
        result = engine.apply(
            {"/result": {"$ref": "/a"}},
            source={"a": {"$ref": "/b"}, "b": "final"},
            dest={},
        )
        assert result == {"result": "final"}

    def test_flag_on_ref_three_level_chain_stops_at_first(self, engine):
        """$raw: True stops at the first resolved value regardless of chain depth."""
        result = engine.apply(
            {"/result": {"$ref": "/a", "$raw": True}},
            source={"a": {"$ref": "/b"}, "b": {"$ref": "/c"}, "c": "deep"},
            dest={},
        )
        # Only first hop resolved: /a → {"$ref": "/b"}
        assert result == {"result": {"$ref": "/b"}}

    def test_flag_on_add(self, engine):
        """$add + $raw: True — arithmetic result not further processed."""
        result = engine.apply(
            {"/result": {"$add": [1, 2], "$raw": True}},
            source={},
            dest={},
        )
        assert result == {"result": 3}

    def test_flag_on_eq(self, engine):
        """$eq + $raw: True — boolean result returned as-is."""
        result = engine.apply(
            {"/result": {"$eq": [1, 1], "$raw": True}},
            source={},
            dest={},
        )
        assert result == {"result": True}

    def test_flag_on_eval(self, engine):
        """$eval + $raw: True — eval result not further processed."""
        result = engine.apply(
            {"/result": {"$eval": [{"/x": {"$raw": {"$ref": "/y"}}}], "$raw": True}},
            source={"y": "hello"},
            dest={},
        )
        # $eval runs the inner spec and returns dest = {"x": {"$ref": "/y"}}
        # $raw: True then stops — {"$ref": "/y"} is not resolved
        assert result == {"result": {"x": {"$ref": "/y"}}}


# ---------------------------------------------------------------------------
# $raw: True as flag on $func
# ---------------------------------------------------------------------------

class TestRawFlagOnFunc:
    """$raw: True on $func call stops further processing of the return value."""

    def test_func_result_not_processed_further_with_raw(self, engine):
        """Function returns a construct literal; $raw: True prevents its evaluation."""
        result = engine.apply(
            [
                # Function body stores a literal construct and returns it
                {"$def": "get_template",
                 "body": [{"/val": {"$raw": {"$ref": "/data"}}}],
                 "return": "/val"},
                {"/result": {"$func": "get_template", "$raw": True}},
            ],
            source={"data": "real_value"},
            dest={},
        )
        # get_template returns {"$ref": "/data"}; $raw: True stops evaluation
        assert result["result"] == {"$ref": "/data"}

    def test_func_result_fully_resolved_without_raw(self, engine):
        """Without $raw: True the function's return value is fully resolved."""
        result = engine.apply(
            [
                {"$def": "get_template",
                 "body": [{"/val": {"$raw": {"$ref": "/data"}}}],
                 "return": "/val"},
                {"/result": {"$func": "get_template"}},
            ],
            source={"data": "real_value"},
            dest={},
        )
        # {"$ref": "/data"} is resolved in the stabilisation loop → "real_value"
        assert result["result"] == "real_value"


# ---------------------------------------------------------------------------
# RawValueSignal mechanics
# ---------------------------------------------------------------------------

class TestRawValueSignal:
    """RawValueSignal is a PipelineSignal subclass with correct handle() behaviour."""

    def test_is_pipeline_signal(self):
        sig = RawValueSignal("x")
        assert isinstance(sig, PipelineSignal)

    def test_carries_value(self):
        sig = RawValueSignal({"key": "val"})
        assert sig.value == {"key": "val"}

    def test_default_value_is_none(self):
        sig = RawValueSignal()
        assert sig.value is None

    def test_handle_updates_ctx_dest(self):
        """handle() sets ctx.dest to signal's value before re-raising."""
        sig = RawValueSignal("raw!")

        class _FakeCtx:
            dest = None

        ctx = _FakeCtx()
        with pytest.raises(RawValueSignal):
            sig.handle(ctx)

        assert ctx.dest == "raw!"

    def test_handle_reraises_same_signal(self):
        """handle() re-raises the same signal instance."""
        sig = RawValueSignal("raw!")

        class _FakeCtx:
            dest = None

        with pytest.raises(RawValueSignal) as exc_info:
            sig.handle(_FakeCtx())

        assert exc_info.value is sig