"""Tests for the SQL text syntax (parse_sql + sql{ } / sql_write{ } stages)."""
import pytest

from j_perm import build_default_engine
from j_perm_sql import install_sql, install_sql_write
from j_perm_sql.text import parse_sql


# ───────────────────────── parse_sql: SELECT ───────────────────────────────

def test_select_star():
    assert parse_sql("select * from t") == {"$select": {"from": {"table": "t"}}}


def test_select_columns():
    assert parse_sql("select id, name from t")["$select"]["columns"] == [
        {"$col": {"name": "id"}}, {"$col": {"name": "name"}}]


def test_col_table_qualified():
    assert parse_sql("select u.id from users u")["$select"] == {
        "columns": [{"$col": {"table": "u", "name": "id"}}],
        "from": {"table": "users", "as": "u"}}


def test_col_alias():
    assert parse_sql("select name as nm from t")["$select"]["columns"] == [
        {"$col": {"name": "name", "as": "nm"}}]


def test_func_alias_and_star():
    q = parse_sql("select COUNT(*) as n from t")["$select"]["columns"][0]
    assert q == {"$func": {"name": "COUNT", "args": ["*"], "as": "n"}}


def test_func_args_and_distinct():
    assert parse_sql("select SUM(x) from t")["$select"]["columns"][0] == {
        "$func": {"name": "SUM", "args": [{"$col": {"name": "x"}}]}}
    assert parse_sql("select COUNT(distinct x) from t")["$select"]["columns"][0] == {
        "$func": {"name": "COUNT", "distinct": True, "args": [{"$col": {"name": "x"}}]}}


def test_func_no_args():
    assert parse_sql("select NOW() from t")["$select"]["columns"][0] == {
        "$func": {"name": "NOW", "args": []}}


def test_expr_alias_to_expr_form():
    q = parse_sql("select a + b as s from t")["$select"]["columns"][0]
    assert q == {"expr": {"$add": [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]}, "as": "s"}


def test_table_ref_as():
    assert parse_sql("select * from users as u")["$select"]["from"] == {"table": "users", "as": "u"}


def test_table_star_select():
    assert parse_sql("select t.* from t")["$select"]["columns"] == [
        {"$col": {"table": "t", "name": "*"}}]


def test_where_predicates():
    q = parse_sql("select * from t where age >= 18 and name = 'x'")["$select"]["where"]
    assert q == {"$and": [
        {"$gte": [{"$col": {"name": "age"}}, {"$val": 18}]},
        {"$eq": [{"$col": {"name": "name"}}, {"$val": "x"}]}]}


def test_all_comparison_ops():
    ops = {"=": "$eq", "<>": "$ne", "!=": "$ne", "<": "$lt", "<=": "$lte", ">": "$gt", ">=": "$gte"}
    for op, key in ops.items():
        w = parse_sql(f"select * from t where a {op} 1")["$select"]["where"]
        assert w == {key: [{"$col": {"name": "a"}}, {"$val": 1}]}


def test_or_not():
    w = parse_sql("select * from t where not a or b")["$select"]["where"]
    assert w == {"$or": [{"$not": {"$col": {"name": "a"}}}, {"$col": {"name": "b"}}]}


def test_in_literals_and_columns():
    w = parse_sql("select * from t where c in ('US', 'GB')")["$select"]["where"]
    assert w == {"$in": [{"$col": {"name": "c"}}, ["US", "GB"]]}
    w2 = parse_sql("select * from t where c in (a, b)")["$select"]["where"]
    assert w2 == {"$in": [{"$col": {"name": "c"}}, [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]]}


def test_arithmetic():
    assert parse_sql("select a - b from t")["$select"]["columns"][0] == {
        "$sub": [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]}
    assert parse_sql("select a * b from t")["$select"]["columns"][0] == {
        "$mul": [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]}
    assert parse_sql("select a / b from t")["$select"]["columns"][0] == {
        "$div": [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]}
    assert parse_sql("select a % b from t")["$select"]["columns"][0] == {
        "$mod": [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]}


def test_atom_literals_and_paren():
    assert parse_sql("select 3.14 from t")["$select"]["columns"][0] == {"$val": 3.14}
    assert parse_sql("select null from t")["$select"]["columns"][0] == {"$val": None}
    assert parse_sql("select true from t")["$select"]["columns"][0] == {"$val": True}
    assert parse_sql("select false from t")["$select"]["columns"][0] == {"$val": False}
    assert parse_sql("select (a) from t")["$select"]["columns"][0] == {"$col": {"name": "a"}}


def test_string_escapes():
    # backslash escapes inside SQL string literals are unescaped
    assert parse_sql(r"select 'a\nb\tc\\d' from t")["$select"]["columns"][0] == {
        "$val": "a\nb\tc\\d"}
    # double-quoted string literal form, with an unknown escape kept literal
    assert parse_sql(r'select "x\qy" from t')["$select"]["columns"][0] == {"$val": "x\\qy"}


def test_read_pointer_val():
    w = parse_sql("select * from t where a = $(/x)")["$select"]["where"]
    assert w == {"$eq": [{"$col": {"name": "a"}}, {"$val": {"$ref": "/x"}}]}


def test_joins_all_types():
    for kw, typ in [("inner", "inner"), ("left", "left"), ("right", "right"),
                    ("full", "full"), ("cross", "cross")]:
        q = parse_sql(f"select * from a {kw} join b on a.id = b.id")["$select"]
        assert q["joins"][0]["$join"]["type"] == typ


def test_join_default_inner():
    q = parse_sql("select * from a join b on a.id = b.id")["$select"]
    assert q["joins"][0]["$join"]["type"] == "inner"
    assert q["joins"][0]["$join"]["on"] == {
        "$eq": [{"$col": {"table": "a", "name": "id"}}, {"$col": {"table": "b", "name": "id"}}]}


def test_group_having_order_limit_offset():
    q = parse_sql("select a from t group by a, b having a > 1 "
                  "order by a, b desc limit 5 offset 2")["$select"]
    assert q["group_by"] == [{"$col": {"name": "a"}}, {"$col": {"name": "b"}}]
    assert q["having"] == {"$gt": [{"$col": {"name": "a"}}, {"$val": 1}]}
    assert q["order_by"] == [{"expr": {"$col": {"name": "a"}}},
                             {"expr": {"$col": {"name": "b"}}, "dir": "desc"}]
    assert q["limit"] == 5 and q["offset"] == 2


def test_order_asc():
    q = parse_sql("select a from t order by a asc")["$select"]
    assert q["order_by"] == [{"expr": {"$col": {"name": "a"}}, "dir": "asc"}]


# ───────────────────────── parse_sql: DML ──────────────────────────────────

def test_insert_with_columns():
    assert parse_sql("insert into users (name, age) values ('Ann', 30)") == {
        "$insert": {"into": "users", "columns": ["name", "age"],
                    "values": [[{"$val": "Ann"}, {"$val": 30}]]}}


def test_insert_no_columns_multi_rows():
    assert parse_sql("insert into t values (1), (2)") == {
        "$insert": {"into": "t", "values": [[{"$val": 1}], [{"$val": 2}]]}}


def test_update_where():
    assert parse_sql("update users set name = 'Bob' where id = 5") == {
        "$update": {"table": "users", "set": {"name": {"$val": "Bob"}},
                    "where": {"$eq": [{"$col": {"name": "id"}}, {"$val": 5}]}}}


def test_update_all():
    assert parse_sql("update t set x = 1 all") == {
        "$update": {"table": "t", "set": {"x": {"$val": 1}}, "all": True}}


def test_delete_where_and_all():
    assert parse_sql("delete from s where a < 1") == {
        "$delete": {"from": "s", "where": {"$lt": [{"$col": {"name": "a"}}, {"$val": 1}]}}}
    assert parse_sql("delete from s all") == {"$delete": {"from": "s", "all": True}}


# ───────────────────────── stages / engine integration ─────────────────────

def _sql_engine(**kw):
    eng = build_default_engine()
    calls = []
    install_sql(eng, lambda sql, params: calls.append((sql, params)) or [{"ok": 1}], **kw)
    return eng, calls


def _write_engine(**kw):
    eng = build_default_engine()
    calls = []
    install_sql_write(eng, lambda sql, params: calls.append((sql, params)) or [], **kw)
    return eng, calls


def test_engine_sql_block_to_dest():
    eng, calls = _sql_engine()
    r = eng.apply('/rows = sql{ select id from users where age >= $(/min) }',
                  source={"min": 18}, dest={})
    assert calls[0] == ('SELECT "id" FROM "users" WHERE "age" >= ?', [18])
    assert r == {"rows": [{"ok": 1}]}


def test_engine_sql_block_no_dest():
    eng, calls = _sql_engine()
    eng.apply('sql{ select * from t }', source={}, dest={})
    assert calls[0] == ('SELECT * FROM "t"', [])


def test_mixed_sql_core_json():
    eng, calls = _sql_engine()
    spec = [
        '/min = 21',
        {"op": "set", "path": "/tag", "value": "x"},
        '/rows = sql{ select id from u where age > $(@:/min) }',
    ]
    r = eng.apply(spec, source={}, dest={})
    assert calls[0] == ('SELECT "id" FROM "u" WHERE "age" > ?', [21])
    assert r["min"] == 21 and r["tag"] == "x" and r["rows"] == [{"ok": 1}]


def test_sql_write_block():
    eng, calls = _write_engine()
    eng.apply('sql_write{ insert into t (a) values ($(/v)) }', source={"v": 9}, dest={})
    assert calls[0] == ('INSERT INTO "t" ("a") VALUES (?)', [9])


def test_sql_write_update_guard():
    eng, calls = _write_engine()
    with pytest.raises(Exception):
        eng.apply('sql_write{ update t set a = 1 }', source={}, dest={})


def test_read_only_rejects_dml():
    eng, _ = _sql_engine()
    with pytest.raises(ValueError):
        eng.apply('sql{ insert into t values (1) }', source={}, dest={})


def test_unbalanced_braces():
    eng, _ = _sql_engine()
    with pytest.raises(ValueError):
        eng.apply('sql{ select * from t ', source={}, dest={})


def test_trailing_text_after_block():
    eng, _ = _sql_engine()
    with pytest.raises(ValueError):
        eng.apply('sql{ select * from t } extra', source={}, dest={})


def test_text_syntax_disabled_on_install():
    eng = build_default_engine(text_syntax=False)
    install_sql(eng, lambda sql, params: [], text_syntax=False)
    with pytest.raises(ValueError):
        eng.apply('sql{ select * from t }', source={}, dest={})


def test_sql_block_compiles():
    eng, calls = _sql_engine()
    compiled = eng.compile(['/rows = sql{ select id from t }'])
    assert compiled is not None
    compiled.apply(source={}, dest={})
    assert calls[0] == ('SELECT "id" FROM "t"', [])


def test_no_langforge_at_runtime():
    import sys
    import j_perm_sql.text  # noqa: F401
    assert "langforge" not in sys.modules
