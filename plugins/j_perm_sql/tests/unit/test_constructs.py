"""Tests for the SQL construct handlers (the full SELECT surface)."""
import pytest

from j_perm_sql import RenderOptions
from j_perm_sql.constructs import _frame_bound, _int_literal, _render_frame


# ─────────────────────────────────────────────────────────────────────────────
# Leaves
# ─────────────────────────────────────────────────────────────────────────────

class TestCol:
    def test_string_form(self, render_sql):
        sql, params = render_sql({"$col": "id"})
        assert sql == '"id"' and params == []

    def test_qualified(self, render_sql):
        sql, _ = render_sql({"$col": {"table": "u", "name": "id"}})
        assert sql == '"u"."id"'

    def test_alias(self, render_sql):
        sql, _ = render_sql({"$col": {"name": "id", "as": "x"}})
        assert sql == '"id" AS "x"'

    def test_star(self, render_sql):
        assert render_sql({"$col": "*"})[0] == "*"

    def test_table_star(self, render_sql):
        assert render_sql({"$col": {"table": "u", "name": "*"}})[0] == '"u".*'


class TestVal:
    def test_literal(self, render_sql):
        assert render_sql({"$val": 42}) == ("?", [42])

    def test_from_source(self, render_sql):
        sql, params = render_sql({"$val": {"$ref": "/age"}}, source={"age": 30})
        assert sql == "?" and params == [30]


# ─────────────────────────────────────────────────────────────────────────────
# Expressions
# ─────────────────────────────────────────────────────────────────────────────

class TestFunc:
    def test_basic(self, render_sql):
        sql, _ = render_sql({"$func": {"name": "COUNT", "args": [{"$col": "id"}]}})
        assert sql == 'COUNT("id")'

    def test_star_arg(self, render_sql):
        assert render_sql({"$func": {"name": "COUNT", "args": ["*"]}})[0] == "COUNT(*)"

    def test_distinct(self, render_sql):
        assert render_sql({"$func": {"name": "COUNT", "args": [{"$col": "id"}], "distinct": True}})[0] == 'COUNT(DISTINCT "id")'

    def test_call_alias(self, render_sql):
        assert render_sql({"$call": {"name": "NOW", "args": []}})[0] == "NOW()"

    def test_alias(self, render_sql):
        assert render_sql({"$func": {"name": "SUM", "args": [{"$col": "x"}], "as": "total"}})[0] == 'SUM("x") AS "total"'

    def test_window_partition_and_order(self, render_sql):
        sql, _ = render_sql({"$func": {
            "name": "ROW_NUMBER", "args": [],
            "over": {"partition_by": [{"$col": "dept"}], "order_by": [{"expr": {"$col": "salary"}, "dir": "desc"}]},
        }})
        assert sql == 'ROW_NUMBER() OVER (PARTITION BY "dept" ORDER BY "salary" DESC)'

    def test_window_frame_between(self, render_sql):
        sql, _ = render_sql({"$func": {
            "name": "SUM", "args": [{"$col": "x"}],
            "over": {"order_by": [{"expr": {"$col": "x"}}],
                     "frame": {"type": "rows", "start": "unbounded preceding", "end": "current row"}},
        }})
        assert sql == 'SUM("x") OVER (ORDER BY "x" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'


class TestFrameHelpers:
    def test_frame_single_bound(self):
        assert _render_frame({"type": "range", "start": "current row"}) == "RANGE CURRENT ROW"

    def test_frame_bad_type(self):
        with pytest.raises(ValueError, match="frame type"):
            _render_frame({"type": "blocks", "start": "current row"})

    def test_bound_preceding(self):
        assert _frame_bound({"preceding": 3}) == "3 PRECEDING"

    def test_bound_following(self):
        assert _frame_bound({"following": 2}) == "2 FOLLOWING"

    def test_bound_unbounded_following(self):
        assert _frame_bound("unbounded following") == "UNBOUNDED FOLLOWING"

    def test_bound_bad_string(self):
        with pytest.raises(ValueError, match="invalid frame bound"):
            _frame_bound("sideways")

    def test_bound_bad_type(self):
        with pytest.raises(ValueError, match="invalid frame bound"):
            _frame_bound(5)

    def test_bound_bad_dict(self):
        with pytest.raises(ValueError, match="invalid frame bound"):
            _frame_bound({"weird": 1})


class TestIntLiteral:
    def test_ok(self):
        assert _int_literal(5, "limit") == 5

    def test_float_rejected(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _int_literal(1.5, "limit")

    def test_bool_rejected(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _int_literal(True, "limit")


class TestCast:
    def test_basic(self, render_sql):
        assert render_sql({"$cast": {"expr": {"$col": "x"}, "type": "INTEGER"}})[0] == 'CAST("x" AS INTEGER)'

    def test_alias(self, render_sql):
        assert render_sql({"$cast": {"expr": {"$val": 1}, "type": "VARCHAR(10)", "as": "s"}}) == (
            'CAST(? AS VARCHAR(10)) AS "s"', [1])


class TestCase:
    def test_with_else(self, render_sql):
        sql, params = render_sql({"$case": {
            "whens": [{"when": {"$gt": [{"$col": "x"}, {"$val": 0}]}, "then": {"$val": "pos"}}],
            "else": {"$val": "neg"}, "as": "sign",
        }})
        assert sql == 'CASE WHEN "x" > ? THEN ? ELSE ? END AS "sign"'
        assert params == [0, "pos", "neg"]

    def test_without_else(self, render_sql):
        sql, _ = render_sql({"$case": {"whens": [{"when": {"$is_null": {"$col": "x"}}, "then": {"$val": 1}}]}})
        assert sql == 'CASE WHEN "x" IS NULL THEN ? END'


class TestConcatAndArithmetic:
    def test_concat_default(self, render_sql):
        assert render_sql({"$concat": [{"$col": "a"}, {"$col": "b"}]})[0] == '("a" || "b")'

    def test_concat_custom_operator(self, render_sql):
        opts = RenderOptions(concat_operator="+")
        assert render_sql({"$concat": [{"$col": "a"}, {"$col": "b"}]}, opts=opts)[0] == '("a" + "b")'

    def test_arithmetic(self, render_sql):
        assert render_sql({"$add": [{"$col": "a"}, {"$val": 1}]}) == ('("a" + ?)', [1])

    def test_mod(self, render_sql):
        assert render_sql({"$mod": [{"$col": "a"}, {"$val": 2}]})[0] == '("a" % ?)'


# ─────────────────────────────────────────────────────────────────────────────
# Predicates
# ─────────────────────────────────────────────────────────────────────────────

class TestBoolean:
    def test_and(self, render_sql):
        sql, params = render_sql({"$and": [
            {"$eq": [{"$col": "a"}, {"$val": 1}]},
            {"$eq": [{"$col": "b"}, {"$val": 2}]},
        ]})
        assert sql == '("a" = ? AND "b" = ?)' and params == [1, 2]

    def test_or(self, render_sql):
        sql, _ = render_sql({"$or": [{"$is_null": {"$col": "a"}}, {"$is_null": {"$col": "b"}}]})
        assert sql == '("a" IS NULL OR "b" IS NULL)'

    def test_empty_raises(self, render_sql):
        with pytest.raises(ValueError, match="at least one predicate"):
            render_sql({"$and": []})

    def test_not(self, render_sql):
        assert render_sql({"$not": {"$eq": [{"$col": "a"}, {"$val": 1}]}}) == ('NOT ("a" = ?)', [1])


class TestComparisons:
    @pytest.mark.parametrize("key,sym", [
        ("$eq", "="), ("$ne", "<>"), ("$gt", ">"), ("$gte", ">="), ("$lt", "<"), ("$lte", "<="),
    ])
    def test_each(self, render_sql, key, sym):
        sql, params = render_sql({key: [{"$col": "a"}, {"$val": 1}]})
        assert sql == f'"a" {sym} ?' and params == [1]


class TestIn:
    def test_list(self, render_sql):
        sql, params = render_sql({"$in": [{"$col": "c"}, ["US", "GB"]]})
        assert sql == '"c" IN (?, ?)' and params == ["US", "GB"]

    def test_not_in(self, render_sql):
        assert render_sql({"$not_in": [{"$col": "c"}, [1]]})[0] == '"c" NOT IN (?)'

    def test_subquery(self, render_sql):
        sql, _ = render_sql({"$in": [{"$col": "id"}, {"$select": {"columns": ["uid"], "from": {"table": "t"}}}]})
        assert sql == '"id" IN (SELECT "uid" FROM "t")'

    def test_scalar_value(self, render_sql):
        sql, params = render_sql({"$in": [{"$col": "c"}, {"$ref": "/v"}]}, source={"v": 9})
        assert sql == '"c" IN (?)' and params == [9]

    def test_empty_raises(self, render_sql):
        with pytest.raises(ValueError, match="non-empty"):
            render_sql({"$in": [{"$col": "c"}, []]})


class TestBetween:
    def test_between(self, render_sql):
        sql, params = render_sql({"$between": [{"$col": "x"}, {"$val": 1}, {"$val": 9}]})
        assert sql == '"x" BETWEEN ? AND ?' and params == [1, 9]

    def test_not_between(self, render_sql):
        assert render_sql({"$not_between": [{"$col": "x"}, {"$val": 1}, {"$val": 9}]})[0] == '"x" NOT BETWEEN ? AND ?'


class TestLike:
    def test_like(self, render_sql):
        assert render_sql({"$like": [{"$col": "n"}, {"$val": "a%"}]}) == ('"n" LIKE ?', ["a%"])

    def test_not_like(self, render_sql):
        assert render_sql({"$not_like": [{"$col": "n"}, {"$val": "a%"}]})[0] == '"n" NOT LIKE ?'

    def test_escape(self, render_sql):
        sql, params = render_sql({"$like": [{"$col": "n"}, {"$val": "a!%"}], "escape": {"$val": "!"}})
        assert sql == '"n" LIKE ? ESCAPE ?' and params == ["a!%", "!"]


class TestNull:
    def test_is_null(self, render_sql):
        assert render_sql({"$is_null": {"$col": "x"}})[0] == '"x" IS NULL'

    def test_is_not_null(self, render_sql):
        assert render_sql({"$is_not_null": {"$col": "x"}})[0] == '"x" IS NOT NULL'


class TestExists:
    def test_exists(self, render_sql):
        sql, _ = render_sql({"$exists": {"$select": {"columns": ["id"], "from": {"table": "t"}}}})
        assert sql == 'EXISTS (SELECT "id" FROM "t")'

    def test_not_exists(self, render_sql):
        sql, _ = render_sql({"$not_exists": {"$select": {"columns": ["id"], "from": {"table": "t"}}}})
        assert sql.startswith("NOT EXISTS (")


class TestQuantified:
    @pytest.mark.parametrize("key,word", [("$any", "ANY"), ("$all", "ALL"), ("$some", "SOME")])
    def test_each(self, render_sql, key, word):
        sql, _ = render_sql({key: [{"$col": "x"}, ">", {"$select": {"columns": ["y"], "from": {"table": "t"}}}]})
        assert sql == f'"x" > {word} (SELECT "y" FROM "t")'

    def test_bad_operator(self, render_sql):
        with pytest.raises(ValueError, match="invalid comparison operator"):
            render_sql({"$any": [{"$col": "x"}, "LIKE", {"$select": {"columns": ["y"], "from": {"table": "t"}}}]})


# ─────────────────────────────────────────────────────────────────────────────
# Clauses inside $select
# ─────────────────────────────────────────────────────────────────────────────

class TestSelectBasics:
    def test_no_columns_is_star(self, render_sql):
        assert render_sql({"$select": {"from": {"table": "t"}}})[0] == 'SELECT * FROM "t"'

    def test_distinct(self, render_sql):
        assert render_sql({"$select": {"distinct": True, "columns": ["a"], "from": {"table": "t"}}})[0] == 'SELECT DISTINCT "a" FROM "t"'

    def test_projection_expr_wrapper_with_alias(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": [{"expr": {"$add": [{"$col": "a"}, {"$col": "b"}]}, "as": "s"}], "from": {"table": "t"}}})
        assert sql == 'SELECT ("a" + "b") AS "s" FROM "t"'

    def test_projection_expr_wrapper_no_alias(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": [{"expr": {"$col": "a"}}], "from": {"table": "t"}}})
        assert sql == 'SELECT "a" FROM "t"'

    def test_where_group_having_order(self, render_sql):
        sql, params = render_sql({"$select": {
            "columns": [{"$col": "dept"}, {"$func": {"name": "COUNT", "args": ["*"], "as": "n"}}],
            "from": {"table": "emp"},
            "where": {"$gt": [{"$col": "salary"}, {"$val": 100}]},
            "group_by": [{"$col": "dept"}],
            "having": {"$gt": [{"$func": {"name": "COUNT", "args": ["*"]}}, {"$val": 2}]},
            "order_by": [{"expr": {"$col": "n"}, "dir": "asc", "nulls": "last"}],
        }})
        assert sql == ('SELECT "dept", COUNT(*) AS "n" FROM "emp" WHERE "salary" > ? '
                       'GROUP BY "dept" HAVING COUNT(*) > ? ORDER BY "n" ASC NULLS LAST')
        assert params == [100, 2]


class TestOrderBy:
    def test_bare_operand(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "order_by": ["a"]}})
        assert sql.endswith('ORDER BY "a"')

    def test_bad_dir(self, render_sql):
        with pytest.raises(ValueError, match="dir must be"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "order_by": [{"expr": "a", "dir": "up"}]}})

    def test_bad_nulls(self, render_sql):
        with pytest.raises(ValueError, match="nulls must be"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "order_by": [{"expr": "a", "nulls": "middle"}]}})


class TestGroupBy:
    def test_rollup(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "group_by": {"$rollup": [{"$col": "a"}, {"$col": "b"}]}}})
        assert sql.endswith('GROUP BY ROLLUP ("a", "b")')

    def test_cube(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "group_by": {"$cube": [{"$col": "a"}]}}})
        assert sql.endswith('GROUP BY CUBE ("a")')

    def test_grouping_sets(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"},
                                         "group_by": {"$grouping_sets": [[{"$col": "a"}], [{"$col": "b"}]]}}})
        assert sql.endswith('GROUP BY GROUPING SETS (("a"), ("b"))')

    def test_invalid(self, render_sql):
        with pytest.raises(ValueError, match="invalid group_by"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "group_by": {"$nope": []}}})


class TestTableSourceAndJoins:
    def test_bare_string_table(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": "users"}})
        assert sql == 'SELECT "a" FROM "users"'

    def test_schema_qualified(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t", "schema": "public", "as": "x"}}})
        assert sql == 'SELECT "a" FROM "public"."t" AS "x"'

    def test_derived_table(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": {"$select": {"columns": ["a"], "from": {"table": "t"}}}, "as": "d"}}})
        assert sql == 'SELECT "a" FROM (SELECT "a" FROM "t") AS "d"'

    def test_derived_table_requires_alias(self, render_sql):
        with pytest.raises(ValueError, match="requires an alias"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": {"$select": {"columns": ["a"], "from": {"table": "t"}}}}}})

    def test_lateral(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": {"$select": {"columns": ["a"], "from": {"table": "t"}}}, "as": "d", "lateral": True}}})
        assert "FROM LATERAL (SELECT" in sql

    @pytest.mark.parametrize("jtype,kw", [
        ("inner", "INNER JOIN"), ("left", "LEFT JOIN"), ("right", "RIGHT JOIN"),
        ("full", "FULL JOIN"), ("cross", "CROSS JOIN"),
    ])
    def test_join_types(self, render_sql, jtype, kw):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"},
                                         "joins": [{"$join": {"type": jtype, "table": "o"}}]}})
        assert f"{kw} \"o\"" in sql

    def test_join_default_inner(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "joins": [{"$join": {"table": "o"}}]}})
        assert "INNER JOIN" in sql

    def test_join_on(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t", "as": "t"},
                                         "joins": [{"$join": {"type": "left", "table": "o", "as": "o",
                                                              "on": {"$eq": [{"$col": {"table": "o", "name": "tid"}}, {"$col": {"table": "t", "name": "id"}}]}}}]}})
        assert 'LEFT JOIN "o" AS "o" ON "o"."tid" = "t"."id"' in sql

    def test_join_using(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"},
                                         "joins": [{"$join": {"type": "inner", "table": "o", "using": ["id", "k"]}}]}})
        assert 'INNER JOIN "o" USING ("id", "k")' in sql

    def test_join_natural(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"},
                                         "joins": [{"$join": {"type": "inner", "table": "o", "natural": True}}]}})
        assert 'NATURAL INNER JOIN "o"' in sql

    def test_join_bad_type(self, render_sql):
        with pytest.raises(ValueError, match="invalid join type"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "joins": [{"$join": {"type": "weird", "table": "o"}}]}})


class TestPagination:
    def test_limit_offset(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "limit": 10, "offset": 5}})
        assert sql.endswith("LIMIT 10 OFFSET 5")

    def test_fetch_alias_in_limit_mode(self, render_sql):
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "fetch": 3}})
        assert sql.endswith("LIMIT 3")

    def test_fetch_mode(self, render_sql):
        opts = RenderOptions(pagination="fetch")
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "offset": 5, "fetch": 10}}, opts=opts)
        assert sql.endswith("OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY")

    def test_fetch_mode_limit_alias(self, render_sql):
        opts = RenderOptions(pagination="fetch")
        sql, _ = render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "limit": 7}}, opts=opts)
        assert sql.endswith("FETCH FIRST 7 ROWS ONLY")

    def test_bad_limit(self, render_sql):
        with pytest.raises(ValueError, match="limit must be"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "limit": 1.5}})

    def test_bad_offset(self, render_sql):
        with pytest.raises(ValueError, match="offset must be"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "offset": "x"}})

    def test_bad_fetch(self, render_sql):
        with pytest.raises(ValueError, match="fetch must be"):
            render_sql({"$select": {"columns": ["a"], "from": {"table": "t"}, "fetch": 2.5}})


# ─────────────────────────────────────────────────────────────────────────────
# CTE, set operations, VALUES
# ─────────────────────────────────────────────────────────────────────────────

class TestCTE:
    def test_simple(self, render_sql):
        sql, _ = render_sql({"$select": {
            "with": [{"name": "t", "query": {"$select": {"columns": ["id"], "from": {"table": "base"}}}}],
            "columns": ["id"], "from": {"table": "t"},
        }})
        assert sql == 'WITH "t" AS (SELECT "id" FROM "base") SELECT "id" FROM "t"'

    def test_columns_and_recursive(self, render_sql):
        sql, _ = render_sql({"$select": {
            "with": [{"name": "t", "columns": ["a", "b"], "recursive": True,
                      "query": {"$select": {"columns": ["a", "b"], "from": {"table": "base"}}}}],
            "columns": ["a"], "from": {"table": "t"},
        }})
        assert sql.startswith('WITH RECURSIVE "t" ("a", "b") AS (')


class TestSetOps:
    @pytest.mark.parametrize("key,word", [
        ("$union", "UNION"), ("$union_all", "UNION ALL"), ("$intersect", "INTERSECT"), ("$except", "EXCEPT"),
    ])
    def test_each(self, render_sql, key, word):
        q = {"$select": {"columns": ["a"], "from": {"table": "t"}}}
        sql, _ = render_sql({key: [q, q]})
        assert f" {word} " in sql

    def test_outer_order_and_pagination(self, render_sql):
        q = {"$select": {"columns": ["a"], "from": {"table": "t"}}}
        sql, _ = render_sql({"$union": [q, q], "order_by": [{"expr": "a"}], "limit": 5})
        assert sql.endswith('ORDER BY "a" LIMIT 5')

    def test_empty_raises(self, render_sql):
        with pytest.raises(ValueError, match="at least one query"):
            render_sql({"$union": []})


class TestValues:
    def test_rows(self, render_sql):
        sql, params = render_sql({"$values": [[1, "a"], [2, "b"]]})
        assert sql == "VALUES (?, ?), (?, ?)" and params == [1, "a", 2, "b"]

    def test_as_table_source(self, render_sql):
        sql, params = render_sql({"$select": {"columns": ["a"], "from": {"table": {"$values": [[1], [2]]}, "as": "v"}}})
        assert sql == 'SELECT "a" FROM (VALUES (?), (?)) AS "v"' and params == [1, 2]

    def test_empty_raises(self, render_sql):
        with pytest.raises(ValueError, match="at least one row"):
            render_sql({"$values": []})

    def test_ragged_raises(self, render_sql):
        with pytest.raises(ValueError, match="same length"):
            render_sql({"$values": [[1, 2], [3]]})
