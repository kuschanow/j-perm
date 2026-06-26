"""SQL CST -> j_perm_sql construct tree (op-dict query)."""


def is_nt(n):
    return hasattr(n, "children")

def nm(n):
    return n.name

def kids(n):
    return n.children

def tv(n):
    return n.token_value

def tn(n):
    return n.terminal_name

def _is_t(n, value=None):
    if is_nt(n):
        return False
    return value is None or tv(n) == value


def _unq(raw):
    s = raw[1:-1]
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            mp = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "'": "'", "\\": "\\"}
            out.append(mp.get(s[i + 1], "\\" + s[i + 1]))
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _flat(node, item="x"):
    out = []
    def rec(n):
        ch = kids(n)
        if len(ch) == 1:
            out.append(ch[0])
        else:
            rec(ch[0])
            out.append(ch[-1])
    rec(node)
    return out


# ---- expressions ----------------------------------------------------------
def xv(n):
    name = nm(n)
    ch = kids(n)
    if name in ("expr", "orx", "andx", "notx", "cmp", "addx", "mulx") and len(ch) == 1:
        return xv(ch[0])
    if name == "orx":
        return {"$or": [xv(ch[0]), xv(ch[2])]}
    if name == "andx":
        return {"$and": [xv(ch[0]), xv(ch[2])]}
    if name == "notx":
        return {"$not": xv(ch[1])}
    if name == "cmp":
        if len(ch) == 2:  # addx cmp_in
            inlist = [_raw(e) for e in _flat(kids(ch[1])[2])]
            return {"$in": [xv(ch[0]), inlist]}
        op = tv(_first(ch[1]))
        m = {"=": "$eq", "<>": "$ne", "!=": "$ne", "<": "$lt", "<=": "$lte",
             ">": "$gt", ">=": "$gte"}
        return {m[op]: [xv(ch[0]), xv(ch[2])]}
    if name == "addx":
        op = tv(ch[1])
        return {"$add" if op == "+" else "$sub": [xv(ch[0]), xv(ch[2])]}
    if name == "mulx":
        key = {"*": "$mul", "/": "$div", "%": "$mod"}[tv(ch[1])]
        return {key: [xv(ch[0]), xv(ch[2])]}
    if name == "atom":
        return _atom(ch)
    if name == "read":
        return {"$val": {"$ref": tv(ch[1])}}
    if name == "colref":
        return _colref(ch)
    if name == "func":
        return _func(ch)
    raise ValueError(f"sql xv: {name}")  # pragma: no cover


def _first(n):
    return n if not is_nt(n) else _first(kids(n)[0])


def _atom(ch):
    if len(ch) == 3:  # '(' expr ')'
        return xv(ch[1])
    n = ch[0]
    if is_nt(n):
        return xv(n)
    t = tn(n)
    if t == "INT":
        return {"$val": int(tv(n))}
    if t == "FLOAT":
        return {"$val": float(tv(n))}
    if t == "STRING":
        return {"$val": _unq(tv(n))}
    v = tv(n)
    if v == "null":
        return {"$val": None}
    if v == "true":
        return {"$val": True}
    if v == "false":
        return {"$val": False}
    raise ValueError(f"sql atom: {t}")  # pragma: no cover


def _raw(n):
    """Unwrap a value expression to a raw literal (for IN lists)."""
    v = xv(n)
    if isinstance(v, dict) and set(v) == {"$val"}:
        return v["$val"]
    return v


def _colref(ch):
    if len(ch) == 1:
        return {"$col": {"name": tv(ch[0])}}
    # IDENT '.' (IDENT | STAR)
    return {"$col": {"table": tv(ch[0]), "name": tv(ch[2])}}


def _func(ch):
    name = tv(ch[0])
    args_node = ch[2]
    ach = kids(args_node)
    d = {"name": name}
    if not ach:
        d["args"] = []
    elif _is_t(ach[0], "*"):
        d["args"] = ["*"]
    elif _is_t(ach[0], "distinct"):
        d["distinct"] = True
        d["args"] = [xv(e) for e in _flat(ach[1])]
    else:
        d["args"] = [xv(e) for e in _flat(ach[0])]
    return {"$func": d}


# ---- column items ---------------------------------------------------------
def _col_item(node):
    ch = kids(node)
    val = xv(ch[0])
    if len(ch) == 1:
        return val
    alias = tv(ch[2])  # expr 'as' IDENT
    if isinstance(val, dict) and "$col" in val:
        val["$col"]["as"] = alias
        return val
    if isinstance(val, dict) and "$func" in val:
        val["$func"]["as"] = alias
        return val
    return {"expr": val, "as": alias}


def _table_ref(node):
    ch = kids(node)
    d = {"table": tv(ch[0])}
    if len(ch) == 2:               # IDENT IDENT
        d["as"] = tv(ch[1])
    elif len(ch) == 3:             # IDENT 'as' IDENT
        d["as"] = tv(ch[2])
    return d


# ---- SELECT ---------------------------------------------------------------
def _select(node):
    ch = kids(node)
    # 'select' sel_cols 'from' table_ref joins where group having order limit offset
    sel = {}
    sel_cols = ch[1]
    if not _is_t(kids(sel_cols)[0], "*"):
        sel["columns"] = [_col_item(c) for c in _flat(kids(sel_cols)[0])]
    sel["from"] = _table_ref(ch[3])
    joins = _joins(ch[4])
    if joins:
        sel["joins"] = joins
    where = _opt(ch[5])
    if where is not None:
        sel["where"] = where
    grp = ch[6]
    if kids(grp):
        sel["group_by"] = [xv(e) for e in _flat(kids(grp)[2])]
    hav = ch[7]
    if kids(hav):
        sel["having"] = xv(kids(hav)[1])
    order = ch[8]
    if kids(order):
        sel["order_by"] = [_order_item(o) for o in _flat(kids(order)[2])]
    lim = ch[9]
    if kids(lim):
        sel["limit"] = int(tv(kids(lim)[1]))
    off = ch[10]
    if kids(off):
        sel["offset"] = int(tv(kids(off)[1]))
    return {"$select": sel}


def _joins(node):
    out = []
    def rec(n):
        ch = kids(n)
        if not ch:
            return
        rec(ch[0])
        out.append(_join_clause(ch[1]))
    rec(node)
    return out


def _join_clause(node):
    ch = kids(node)  # jointype 'join' table_ref 'on' expr
    jt = kids(ch[0])
    typ = tv(jt[0]) if jt else "inner"
    tref = _table_ref(ch[2])
    d = {"type": typ, **tref, "on": xv(ch[4])}
    return {"$join": d}


def _opt(node):
    ch = kids(node)
    if not ch:
        return None
    return xv(ch[1])  # 'where' expr


def _order_item(node):
    ch = kids(node)
    d = {"expr": xv(ch[0])}
    if len(ch) == 2:
        d["dir"] = tv(ch[1])  # asc/desc
    return d


# ---- INSERT / UPDATE / DELETE --------------------------------------------
def _insert(node):
    ch = kids(node)  # 'insert' 'into' IDENT collist_opt 'values' rows
    d = {"into": tv(ch[2])}
    collist = ch[3]
    if kids(collist):
        d["columns"] = [tv(x) for x in _flat(kids(collist)[1]) if not is_nt(x)]
    rows = ch[5]
    d["values"] = [[xv(v) for v in _flat(kids(r)[1])] for r in _flat(rows)]
    return {"$insert": d}


def _update(node):
    ch = kids(node)  # 'update' IDENT 'set' assigns where_or_all
    d = {"table": tv(ch[1])}
    sets = {}
    for a in _flat(ch[3]):
        ac = kids(a)
        sets[tv(ac[0])] = xv(ac[2])
    d["set"] = sets
    _where_or_all(ch[4], d)
    return {"$update": d}


def _delete(node):
    ch = kids(node)  # 'delete' 'from' IDENT where_or_all
    d = {"from": tv(ch[2])}
    _where_or_all(ch[3], d)
    return {"$delete": d}


def _where_or_all(node, d):
    ch = kids(node)
    if _is_t(ch[0], "all"):
        d["all"] = True
    else:
        d["where"] = xv(ch[1])


_TOP = {
    "select_stmt": _select, "insert_stmt": _insert,
    "update_stmt": _update, "delete_stmt": _delete,
}


def transform(tree):
    """sql_stmt -> query construct dict."""
    inner = kids(tree)[0]
    return _TOP[nm(inner)](inner)
