"""Tests for the DML constructs: $insert / $update / $delete."""
import pytest

from j_perm_sql import RenderOptions, build_sql_write_specials


# ─────────────────────────────────────────────────────────────────────────────
# $insert
# ─────────────────────────────────────────────────────────────────────────────

class TestInsert:
    def test_values_single_row(self, render_write):
        sql, params = render_write({"$insert": {
            "into": "users",
            "columns": ["name", "age"],
            "values": [[{"$val": "Ann"}, {"$val": 30}]],
        }})
        assert sql == 'INSERT INTO "users" ("name", "age") VALUES (?, ?)'
        assert params == ["Ann", 30]

    def test_values_multi_row(self, render_write):
        sql, params = render_write({"$insert": {
            "into": "users",
            "columns": ["name"],
            "values": [[{"$val": "Ann"}], [{"$val": "Bob"}]],
        }})
        assert sql == 'INSERT INTO "users" ("name") VALUES (?), (?)'
        assert params == ["Ann", "Bob"]

    def test_values_without_columns(self, render_write):
        sql, params = render_write({"$insert": {
            "into": "users",
            "values": [[{"$val": 1}, {"$val": "Ann"}]],
        }})
        assert sql == 'INSERT INTO "users" VALUES (?, ?)'
        assert params == [1, "Ann"]

    def test_schema_qualified_target(self, render_write):
        sql, _ = render_write({"$insert": {
            "into": {"table": "users", "schema": "app"},
            "values": [[{"$val": 1}]],
        }})
        assert sql == 'INSERT INTO "app"."users" VALUES (?)'

    def test_insert_select(self, render_write):
        sql, params = render_write({"$insert": {
            "into": "archive",
            "columns": ["id", "name"],
            "query": {"$select": {
                "columns": ["id", "name"],
                "from": {"table": "users"},
                "where": {"$lt": [{"$col": "age"}, {"$val": 18}]},
            }},
        }})
        assert sql == (
            'INSERT INTO "archive" ("id", "name") '
            'SELECT "id", "name" FROM "users" WHERE "age" < ?'
        )
        assert params == [18]

    def test_value_from_source_ref(self, render_write):
        sql, params = render_write(
            {"$insert": {"into": "t", "values": [[{"$val": {"$ref": "/seed"}}]]}},
            source={"seed": 42},
        )
        assert sql == 'INSERT INTO "t" VALUES (?)'
        assert params == [42]

    def test_requires_exactly_one_of_values_or_query(self, render_write):
        with pytest.raises(ValueError, match="exactly one of 'values' or 'query'"):
            render_write({"$insert": {"into": "t"}})
        with pytest.raises(ValueError, match="exactly one of 'values' or 'query'"):
            render_write({"$insert": {"into": "t", "values": [[{"$val": 1}]],
                                      "query": {"$select": {}}}})

    def test_empty_values_rejected(self, render_write):
        with pytest.raises(ValueError, match="at least one row"):
            render_write({"$insert": {"into": "t", "values": []}})

    def test_ragged_rows_rejected(self, render_write):
        with pytest.raises(ValueError, match="same length"):
            render_write({"$insert": {"into": "t",
                                      "values": [[{"$val": 1}], [{"$val": 1}, {"$val": 2}]]}})

    def test_query_must_be_a_query_construct(self, render_write):
        with pytest.raises(ValueError, match="SELECT/set-op query construct"):
            render_write({"$insert": {"into": "t", "query": {"$val": 1}}})


# ─────────────────────────────────────────────────────────────────────────────
# $update
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdate:
    def test_basic(self, render_write):
        sql, params = render_write({"$update": {
            "table": "users",
            "set": {"name": {"$val": "Bob"}},
            "where": {"$eq": [{"$col": "id"}, {"$val": 5}]},
        }})
        assert sql == 'UPDATE "users" SET "name" = ? WHERE "id" = ?'
        assert params == ["Bob", 5]

    def test_multiple_assignments_and_expression(self, render_write):
        sql, params = render_write({"$update": {
            "table": "users",
            "set": {
                "name": {"$val": "Bob"},
                "age": {"$add": [{"$col": "age"}, {"$val": 1}]},
            },
            "where": {"$eq": [{"$col": "id"}, {"$val": 5}]},
        }})
        assert sql == 'UPDATE "users" SET "name" = ?, "age" = ("age" + ?) WHERE "id" = ?'
        assert params == ["Bob", 1, 5]

    def test_schema_qualified_target(self, render_write):
        sql, _ = render_write({"$update": {
            "table": {"table": "users", "schema": "app"},
            "set": {"x": {"$val": 1}},
            "all": True,
        }})
        assert sql == 'UPDATE "app"."users" SET "x" = ?'

    def test_correlated_subquery_in_value(self, render_write):
        sql, params = render_write({"$update": {
            "table": "users",
            "set": {"cnt": {"$select": {
                "columns": [{"$func": {"name": "COUNT", "args": ["*"]}}],
                "from": {"table": "orders"},
                "where": {"$eq": [{"$col": {"table": "orders", "name": "user_id"}},
                                  {"$col": {"table": "users", "name": "id"}}]},
            }}},
            "all": True,
        }})
        assert sql == (
            'UPDATE "users" SET "cnt" = (SELECT COUNT(*) FROM "orders" '
            'WHERE "orders"."user_id" = "users"."id")'
        )
        assert params == []

    def test_all_flag_allows_missing_where(self, render_write):
        sql, params = render_write({"$update": {
            "table": "t", "set": {"x": {"$val": 1}}, "all": True,
        }})
        assert sql == 'UPDATE "t" SET "x" = ?'
        assert params == [1]

    def test_missing_where_without_all_rejected(self, render_write):
        with pytest.raises(ValueError, match='"all": true'):
            render_write({"$update": {"table": "t", "set": {"x": {"$val": 1}}}})

    def test_set_must_be_non_empty_mapping(self, render_write):
        with pytest.raises(ValueError, match="non-empty mapping"):
            render_write({"$update": {"table": "t", "set": {}, "all": True}})
        with pytest.raises(ValueError, match="non-empty mapping"):
            render_write({"$update": {"table": "t", "set": [], "all": True}})


# ─────────────────────────────────────────────────────────────────────────────
# $delete
# ─────────────────────────────────────────────────────────────────────────────

class TestDelete:
    def test_basic(self, render_write):
        sql, params = render_write({"$delete": {
            "from": "users",
            "where": {"$lt": [{"$col": "age"}, {"$val": 18}]},
        }})
        assert sql == 'DELETE FROM "users" WHERE "age" < ?'
        assert params == [18]

    def test_all_flag_allows_missing_where(self, render_write):
        sql, params = render_write({"$delete": {"from": "t", "all": True}})
        assert sql == 'DELETE FROM "t"'
        assert params == []

    def test_missing_where_without_all_rejected(self, render_write):
        with pytest.raises(ValueError, match='"all": true'):
            render_write({"$delete": {"from": "t"}})


# ─────────────────────────────────────────────────────────────────────────────
# table target validation
# ─────────────────────────────────────────────────────────────────────────────

class TestTableTarget:
    def test_invalid_non_str_non_dict(self, render_write):
        with pytest.raises(ValueError, match="invalid table target"):
            render_write({"$delete": {"from": 123, "all": True}})

    def test_subquery_target_rejected(self, render_write):
        with pytest.raises(ValueError, match="invalid table target"):
            render_write({"$delete": {"from": {"$select": {}}, "all": True}})


# ─────────────────────────────────────────────────────────────────────────────
# specials assembly
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildWriteSpecials:
    def test_superset_of_read_constructs(self):
        specials = build_sql_write_specials(RenderOptions())
        # DML keys present
        assert {"$insert", "$update", "$delete"} <= set(specials)
        # plus the read surface (so subqueries / predicates resolve)
        assert {"$select", "$col", "$val", "$eq", "$and"} <= set(specials)
