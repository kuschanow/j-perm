"""Tests for the rendering primitives in render.py."""
import pytest

from j_perm_sql.render import (
    bind_value,
    fragment,
    is_fragment,
    is_query,
    join_fragments,
    render,
    render_construct,
    render_operand,
    render_operands,
    render_subquery,
)


class TestActivePipeline:
    def test_defaults_to_read_pipeline(self, ctx):
        # ctx fixture registers only the "sql" pipeline; no metadata set
        assert render({"$col": "id"}, ctx) == {"sql": '"id"', "params": []}

    def test_respects_active_pipeline_metadata(self, ctx):
        ctx.metadata["_sql_pipeline"] = "does_not_exist"
        with pytest.raises(KeyError, match="not registered"):
            render({"$col": "id"}, ctx)


class TestFragment:
    def test_fragment_no_params(self):
        assert fragment("x") == {"sql": "x", "params": []}

    def test_fragment_with_params(self):
        assert fragment("?", [5]) == {"sql": "?", "params": [5]}

    def test_is_fragment(self):
        assert is_fragment({"sql": "x", "params": []})
        assert not is_fragment({"sql": "x"})
        assert not is_fragment("x")

    def test_bind_value(self):
        assert bind_value(7) == {"sql": "?", "params": [7]}


class TestIsQuery:
    def test_select_is_query(self):
        assert is_query({"$select": {}})

    def test_values_is_query(self):
        assert is_query({"$values": []})

    def test_non_query_dict(self):
        assert not is_query({"$col": "id"})

    def test_non_dict(self):
        assert not is_query("x")


class TestJoinHelpers:
    def test_join_fragments(self):
        frags = [fragment("a", [1]), fragment("b", [2])]
        assert join_fragments(frags, ", ") == {"sql": "a, b", "params": [1, 2]}

    def test_render_operands(self, ctx, opts):
        result = render_operands(["a", "b"], ctx, opts)
        assert result == {"sql": '"a", "b"', "params": []}


class TestRenderOperand:
    def test_bare_string(self, ctx, opts):
        assert render_operand("id", ctx, opts) == {"sql": '"id"', "params": []}

    def test_construct(self, ctx, opts):
        assert render_operand({"$val": 5}, ctx, opts) == {"sql": "?", "params": [5]}

    def test_query_wrapped_in_parens(self, ctx, opts):
        frag = render_operand({"$select": {"columns": ["id"], "from": {"table": "t"}}}, ctx, opts)
        assert frag["sql"] == '(SELECT "id" FROM "t")'

    def test_invalid_operand_type(self, ctx, opts):
        with pytest.raises(TypeError, match="invalid SQL operand"):
            render_operand(123, ctx, opts)


class TestRenderConstruct:
    def test_non_construct_raises(self, ctx):
        # a bare string flows through identity and is not a fragment
        with pytest.raises(ValueError, match="expected a SQL construct"):
            render_construct("not_a_construct", ctx)

    def test_subquery_parens(self, ctx):
        frag = render_subquery({"$select": {"columns": ["id"], "from": {"table": "t"}}}, ctx)
        assert frag["sql"].startswith("(SELECT")
        assert frag["sql"].endswith(")")
