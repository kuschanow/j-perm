"""Tests for RenderOptions: validation, quoting, and placeholder finalize."""
import pytest

from j_perm_sql import RenderOptions
from j_perm_sql.dialect import PLACEHOLDER


class TestValidation:
    def test_defaults(self):
        opts = RenderOptions()
        assert opts.paramstyle == "qmark"
        assert opts.identifier_quote == '"'
        assert opts.pagination == "limit"
        assert opts.concat_operator == "||"

    def test_bad_paramstyle(self):
        with pytest.raises(ValueError, match="paramstyle"):
            RenderOptions(paramstyle="weird")

    def test_bad_pagination(self):
        with pytest.raises(ValueError, match="pagination"):
            RenderOptions(pagination="weird")

    def test_bad_identifier_quote(self):
        with pytest.raises(ValueError, match="single character"):
            RenderOptions(identifier_quote="")


class TestIdentifiers:
    def test_quote_identifier_valid(self, opts):
        assert opts.quote_identifier("users") == '"users"'
        assert opts.quote_identifier("_x1") == '"_x1"'

    @pytest.mark.parametrize("bad", ["a b", "1col", "a-b", "a.b", "", 'a"b'])
    def test_quote_identifier_invalid(self, opts, bad):
        with pytest.raises(ValueError, match="invalid SQL identifier"):
            opts.quote_identifier(bad)

    def test_quote_identifier_non_str(self, opts):
        with pytest.raises(ValueError, match="invalid SQL identifier"):
            opts.quote_identifier(123)

    def test_quote_ref_star(self, opts):
        assert opts.quote_ref("*") == "*"

    def test_quote_ref_table_star(self, opts):
        assert opts.quote_ref("u.*") == '"u".*'

    def test_quote_ref_qualified(self, opts):
        assert opts.quote_ref("u.id") == '"u"."id"'

    def test_quote_ref_plain(self, opts):
        assert opts.quote_ref("id") == '"id"'

    def test_custom_quote_char(self):
        opts = RenderOptions(identifier_quote="`")
        assert opts.quote_identifier("col") == "`col`"


class TestTypeAndFunc:
    @pytest.mark.parametrize("ok", ["INTEGER", "VARCHAR(255)", "DECIMAL(10, 2)", "TIMESTAMP WITH TIME"])
    def test_validate_type_ok(self, opts, ok):
        assert opts.validate_type(ok) == ok

    @pytest.mark.parametrize("bad", ["int;drop", "a'b", 5])
    def test_validate_type_bad(self, opts, bad):
        with pytest.raises(ValueError, match="invalid SQL type"):
            opts.validate_type(bad)

    def test_validate_func_ok(self, opts):
        assert opts.validate_func_name("COUNT") == "COUNT"

    @pytest.mark.parametrize("bad", ["co unt", "do();drop", 7])
    def test_validate_func_bad(self, opts, bad):
        with pytest.raises(ValueError, match="invalid SQL function name"):
            opts.validate_func_name(bad)


class TestFinalize:
    def test_qmark(self, opts):
        sql, params = opts.finalize("a = ? AND b = ?", [1, 2])
        assert sql == "a = ? AND b = ?"
        assert params == [1, 2]

    def test_format(self):
        opts = RenderOptions(paramstyle="format")
        sql, params = opts.finalize("a = ? AND b = ?", [1, 2])
        assert sql == "a = %s AND b = %s"
        assert params == [1, 2]

    def test_numeric(self):
        opts = RenderOptions(paramstyle="numeric")
        sql, params = opts.finalize("a = ? AND b = ?", [1, 2])
        assert sql == "a = $1 AND b = $2"
        assert params == [1, 2]

    def test_named(self):
        opts = RenderOptions(paramstyle="named")
        sql, params = opts.finalize("a = ? AND b = ?", [1, 2])
        assert sql == "a = :p1 AND b = :p2"
        assert params == {"p1": 1, "p2": 2}

    def test_mismatch(self, opts):
        with pytest.raises(ValueError, match="placeholder/param mismatch"):
            opts.finalize("a = ?", [1, 2])

    def test_placeholder_constant(self):
        assert PLACEHOLDER == "?"
