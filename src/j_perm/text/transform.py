"""CST -> op-dict transformer.

Operates on the nodes produced by the generated standalone parser
(``j_perm/text/_generated/parser.py``): ``NonTerminalNode`` has ``.name`` and
``.children``; ``TerminalNode`` has ``.terminal_name`` / ``.token_value``.
"""


# ---- node accessors -------------------------------------------------------
def is_nt(n):
    return hasattr(n, "children")

def nt_name(n):
    return n.name

def kids(n):
    return n.children

def tval(n):
    return n.token_value

def tname(n):
    return n.terminal_name


def _is_term(n, value=None):
    if is_nt(n):
        return False
    return value is None or tval(n) == value


# ---- string unescape ------------------------------------------------------
def _unescape(raw):
    # raw includes surrounding quotes
    s = raw[1:-1]
    out = []
    i = 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            nxt = s[i + 1]
            mp = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\"}
            out.append(mp.get(nxt, "\\" + nxt))
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


# ---- pointer detection ----------------------------------------------------
def _is_pointer_literal(value):
    return isinstance(value, str) and (
        value.startswith("/") or value[:2] in ("@:", "&:", "!:", "_:")
    )


# ---- expression transform -------------------------------------------------
def xv(n):
    """Transform an expression node into a j-perm value."""
    name = nt_name(n)
    ch = kids(n)

    # single-child passthrough for the precedence cascade
    if name in ("expr", "coalesce", "orx", "andx", "cmp", "addx", "mulx",
                "powx", "notx", "unary", "arg") and len(ch) == 1:
        return xv(ch[0])

    if name == "coalesce":      # coalesce '??' orx
        return {"$or": [xv(ch[0]), xv(ch[2])]}
    if name == "orx":
        return {"$or": [xv(ch[0]), xv(ch[2])]}
    if name == "andx":
        return {"$and": [xv(ch[0]), xv(ch[2])]}
    if name == "notx":          # 'not' notx
        return {"$not": xv(ch[1])}
    if name == "cmp":           # cmp cmpop addx
        op = tval(_first_term(ch[1]))
        m = {"==": "$eq", "!=": "$ne", "<": "$lt", "<=": "$lte",
             ">": "$gt", ">=": "$gte"}
        if op == "in":
            return {"$in": [xv(ch[0]), xv(ch[2])]}
        return {m[op]: [xv(ch[0]), xv(ch[2])]}
    if name == "addx":          # addx ('+'|'-') mulx
        op = tval(ch[1])
        return {"$add" if op == "+" else "$sub": [xv(ch[0]), xv(ch[2])]}
    if name == "mulx":
        op = tval(ch[1])
        key = {"*": "$mul", "/": "$div", "%": "$mod"}[op]
        return {key: [xv(ch[0]), xv(ch[2])]}
    if name == "powx":          # unary '**' powx
        return {"$pow": [xv(ch[0]), xv(ch[2])]}
    if name == "unary":
        if _is_term(ch[0], "-"):
            inner = xv(ch[1])
            if isinstance(inner, (int, float)):
                return -inner
            return {"$sub": [0, inner]}
        if _is_term(ch[0]):  # 'exists' POINTER
            return {"$exists": tval(ch[1])}
    if name == "atom":
        return _atom(ch)
    if name == "read":
        return _read(ch)
    if name == "call":
        return _call(ch)
    if name == "listx":
        return _list(ch)
    if name == "dictx":
        return _dict(ch)
    if name == "evalx":
        return _eval(ch)
    raise ValueError(f"xv: unhandled expr node {name!r}")  # pragma: no cover


def _first_term(n):
    return n if not is_nt(n) else _first_term(kids(n)[0])


def _atom(ch):
    if len(ch) == 3:  # '(' expr ')'
        return xv(ch[1])
    n = ch[0]
    if is_nt(n):
        return xv(n)
    t = tname(n)
    v = tval(n)
    if t == "INT":
        return int(v)
    if t == "FLOAT":
        return float(v)
    if t == "STRING":
        return _unescape(v)
    if t == "POINTER":
        return v  # bare pointer = string literal
    if v == "true":
        return True
    if v == "false":
        return False
    if v == "null":
        return None
    raise ValueError(f"_atom: {t} {v!r}")  # pragma: no cover


def _read(ch):
    # '$(' POINTER ')' ['raw'] | '$(' POINTER '??' expr ')'
    path = tval(ch[1])
    res = {"$ref": path}
    if len(ch) >= 4 and _is_term(ch[2], "??"):
        res["$default"] = xv(ch[3])
    if any(_is_term(c, "raw") for c in ch):
        res["$raw"] = True
    return res


def _args(node):
    """Return (positional_list, named_dict) from an `args` node."""
    pos, nm = [], {}
    if not kids(node):
        return pos, nm
    arglist = kids(node)[0]  # arg_list
    items = _flatten_list(arglist, "arg_list", "arg")
    for a in items:
        ach = kids(a)
        if len(ach) == 3 and _is_term(ach[1], ":"):  # IDENT ':' expr
            nm[tval(ach[0])] = xv(ach[2])
        else:
            pos.append(xv(ach[0]))
    return pos, nm


def _flatten_list(node, list_name, item_name):
    """Flatten left-recursive `L -> L sep item | item` into [item...]."""
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


_STR1 = {"upper": "$str_upper", "lower": "$str_lower"}


def _call(ch):
    if _is_term(ch[0], "raw"):           # raw '(' expr ')'
        return {"$raw": xv(ch[2])}
    name = tval(ch[0])
    pos, nm = _args(ch[2])
    if name in ("int", "float", "bool", "str"):
        return {"$cast": {"value": pos[0], "type": name}}
    if name in _STR1:
        return {_STR1[name]: pos[0]}
    if name in ("strip", "lstrip", "rstrip"):
        key = {"strip": "$str_strip", "lstrip": "$str_lstrip", "rstrip": "$str_rstrip"}[name]
        if len(pos) == 1:
            return {key: pos[0]}
        return {key: {"string": pos[0], "chars": pos[1]}}
    if name == "split":
        d = {"string": pos[0], "delimiter": pos[1]}
        if "maxsplit" in nm:
            d["maxsplit"] = nm["maxsplit"]
        return {"$str_split": d}
    if name == "join":
        return {"$str_join": {"array": pos[0], "separator": pos[1]}}
    if name == "replace":
        d = {"string": pos[0], "old": pos[1], "new": pos[2]}
        if "count" in nm:
            d["count"] = nm["count"]
        return {"$str_replace": d}
    if name in ("contains", "startswith", "endswith"):
        key = {"contains": "$str_contains", "startswith": "$str_startswith",
               "endswith": "$str_endswith"}[name]
        sub = {"contains": "substring", "startswith": "prefix", "endswith": "suffix"}[name]
        return {key: {"string": pos[0], sub: pos[1]}}
    if name == "round":
        if len(pos) == 1 and not nm:
            return {"$round": pos[0]}
        d = {"value": pos[0]}
        if len(pos) > 1:
            d["ndigits"] = pos[1]
        if "mode" in nm:
            d["mode"] = nm["mode"]
        return {"$round": d}
    if name == "slice":
        d = {"string": pos[0]}
        if len(pos) > 1:
            d["start"] = pos[1]
        if len(pos) > 2:
            d["end"] = pos[2]
        return {"$str_slice": d}
    if name in ("regex_match", "regex_search", "regex_findall", "regex_groups"):
        d = {"pattern": pos[0], "string": pos[1]}
        if "flags" in nm:
            d["flags"] = nm["flags"]
        return {"$" + name: d}
    if name == "regex_replace":
        d = {"pattern": pos[0], "replacement": pos[1], "string": pos[2]}
        if "count" in nm:
            d["count"] = nm["count"]
        if "flags" in nm:
            d["flags"] = nm["flags"]
        return {"$regex_replace": d}
    if name == "ref":
        d = {"$ref": pos[0]}
        if "default" in nm:
            d["$default"] = nm["default"]
        return d
    # default: user function
    return {"$func": name, "args": pos}


def _list(ch):
    elems = kids(ch[1])  # elems node children
    if not elems:        # '[' <empty> ']'
        return []
    return [xv(e) for e in _flatten_list(elems[0], "expr_list", "expr")]


def _dict(ch):
    pairs = kids(ch[1])  # pairs node children
    if not pairs:        # '{' <empty> '}'
        return {}
    plist = pairs[0]  # pair_list
    out = {}
    for p in _flatten_list(plist, "pair_list", "pair"):
        pch = kids(p)
        key = _unescape(tval(pch[0])) if tname(pch[0]) == "STRING" else tval(pch[0])
        out[key] = xv(pch[2])
    return out


def _eval(ch):
    body = xblock(ch[2])
    res = {"$eval": body}
    if len(ch) >= 6 and _is_term(ch[4], "select"):
        res["$select"] = tval(ch[5])
    return res


# ---- statement transform --------------------------------------------------
def xblock(node):
    """block node -> list of op-dicts."""
    steps = []
    for c in kids(node):
        if is_nt(c) and nt_name(c) == "stmts":
            for item in _collect_stmts(c):
                steps.extend(xstmt(item))
    return steps


def _collect_stmts(node):
    """stmts -> stmt seps_opt | stmts stmt seps_opt  =>  [stmt nodes]."""
    out = []
    def rec(n):
        for c in kids(n):
            if is_nt(c) and nt_name(c) == "stmts":
                rec(c)
            elif is_nt(c) and nt_name(c) == "stmt":
                out.append(c)
    rec(node)
    return out


def xsuite(node):
    """suite -> list of op-dicts."""
    ch = kids(node)
    if _is_term(ch[0], ":") and len(ch) == 2:    # ':' stmt
        return xstmt(ch[1])
    if _is_term(ch[0], ":"):                       # ':' NEWLINE INDENT block DEDENT
        for c in ch:
            if is_nt(c) and nt_name(c) == "block":
                return xblock(c)
        return []  # pragma: no cover
    # '{' block '}'
    return xblock(ch[1])


def xstmt(node):
    """stmt node -> list of op-dicts (usually one)."""
    inner = kids(node)[0]
    name = nt_name(inner)
    ch = kids(inner)
    fn = _STMT.get(name)
    if fn is None:
        raise ValueError(f"xstmt: unhandled {name!r}")  # pragma: no cover
    res = fn(ch)
    return res if isinstance(res, list) else [res]


def _assign(ch):
    path = tval(ch[0])
    if len(ch) == 5:  # POINTER '[' ']' '=' expr  -> append
        return {"op": "set", "path": path + "/-", "value": xv(ch[4])}
    return {"op": "set", "path": path, "value": xv(ch[2])}


def _copy(ch):
    dst = tval(ch[0])
    src = tval(ch[2])
    strict = _is_term(ch[1], "<-!")
    d = {"op": "copy", "from": src, "path": dst, "ignore_missing": not strict}
    if len(ch) >= 5 and _is_term(ch[3], "??"):
        d["default"] = xv(ch[4])
    return d


def _del(ch):
    strict = _is_term(ch[1], "!")
    plist = ch[-1]
    paths = [tval(p) for p in _flatten_list(plist, "ptr_list", "POINTER")
             if not is_nt(p)]
    return [{"op": "delete", "path": p, "ignore_missing": not strict} for p in paths]


def _assert(ch):
    d = {"op": "assert", "path": tval(ch[1])}
    if len(ch) >= 4 and _is_term(ch[2], "=="):
        d["equals"] = xv(ch[3])
    return d


def _if(ch):
    # 'if' expr suite elifs else_opt
    cond = xv(ch[1])
    then = xsuite(ch[2])
    elifs = ch[3]
    else_opt = ch[4]
    else_branch = _else(else_opt)
    # build nested elif chain from innermost
    elif_pairs = _collect_elifs(elifs)
    for econd, ethen in reversed(elif_pairs):
        else_branch = [{"op": "if", "cond": econd, "then": ethen,
                        **({"else": else_branch} if else_branch is not None else {})}]
    d = {"op": "if", "cond": cond, "then": then}
    if else_branch is not None:
        d["else"] = else_branch
    return d


def _collect_elifs(node):
    pairs = []
    def rec(n):
        ch = kids(n)
        if not ch:
            return
        rec(ch[0])
        # elifs ['elif'|seps 'elif'] expr suite  — expr/suite are the last two children
        pairs.append((xv(ch[-2]), xsuite(ch[-1])))
    rec(node)
    return pairs


def _else(node):
    ch = kids(node)
    if not ch:
        return None
    # ['else'|seps 'else'] suite  — suite is the last child
    return xsuite(ch[-1])


def _foreach(ch):
    var = tval(ch[1])
    src = xv(ch[3])
    d = {"op": "foreach", "as": var}
    if _is_pointer_literal(src):
        d["in"] = src
    else:
        d["in_value"] = src
    # optional default
    idx = 4
    if len(ch) >= 6 and _is_term(ch[4], "default"):
        d["default"] = xv(ch[5])
        idx = 6
    d["do"] = xsuite(ch[idx])
    return d


def _while(ch):
    return {"op": "while", "cond": xv(ch[1]), "do": xsuite(ch[2])}


def _dowhile(ch):
    # 'do' suite 'while' expr
    return {"op": "while", "cond": xv(ch[3]), "do": xsuite(ch[1]), "do_while": True}


def _try(ch):
    d = {"op": "try", "do": xsuite(ch[1])}
    exc = ch[2]
    fin = ch[3]
    if kids(exc):
        d["except"] = xsuite(kids(exc)[1])
    if kids(fin):
        d["finally"] = xsuite(kids(fin)[1])
    return d


def _def(ch):
    # 'def' IDENT '(' params ')' defopts suite failopt
    name = tval(ch[1])
    params = _params(ch[3])
    d = {"$def": name, "params": params}
    defopts = ch[5]
    if kids(defopts):  # 'context' '=' IDENT
        d["context"] = tval(kids(defopts)[2])
    d["body"] = xsuite(ch[6])
    failopt = ch[7]
    if kids(failopt):
        d["on_failure"] = xsuite(kids(failopt)[1])
    return d


def _params(node):
    ch = kids(node)
    if not ch:
        return []
    plist = ch[0]
    return [tval(p) for p in _flatten_list(plist, "param_list", "IDENT") if not is_nt(p)]


def _return(ch):
    if len(ch) == 1:
        return {"$return": None}
    return {"$return": xv(ch[1])}


def _raise(ch):
    return {"$raise": xv(ch[1])}


def _exec(ch):
    if _is_term(ch[1], None) and tname(ch[1]) == "POINTER":
        d = {"op": "exec", "from": tval(ch[1])}
        if len(ch) >= 3 and _is_term(ch[2], "merge"):
            d["merge"] = True
        return d
    return {"op": "exec", "actions": xsuite(ch[1])}


def _op(ch):
    # 'op' STRING '(' args ')' [suite]
    opname = _unescape(tval(ch[1]))
    pos, nm = _args(ch[3])
    d = {"op": opname, **nm}
    if len(ch) >= 6:
        d["do"] = xsuite(ch[5])
    return d


def _opfn(ch):
    # POINTER '=' opfn '(' args ')'  where opfn -> 'serialize'|'deserialize'|...
    dst = tval(ch[0])
    opname = tval(kids(ch[2])[0])
    pos, nm = _args(ch[4])
    d = {"op": opname, "path": dst, **nm}
    if pos:
        src = pos[0]
        if _is_pointer_literal(src):
            d["from"] = src
        else:
            d["value"] = src
    return d


def _callstmt(ch):
    return xv(ch[0])  # call -> {"$func":...}


_STMT = {
    "assign": _assign, "copy": _copy, "del": _del, "assertst": _assert,
    "ifst": _if, "foreachst": _foreach, "whilest": _while, "dowhilest": _dowhile,
    "tryst": _try, "defst": _def, "returnst": _return, "raisest": _raise,
    "breakst": lambda ch: {"$break": None}, "continuest": lambda ch: {"$continue": None},
    "exitst": lambda ch: {"$exit": None},
    "execst": _exec, "opst": _op, "opfnst": _opfn, "callstmt": _callstmt,
}


def transform(tree):
    """program -> list of op-dicts."""
    block = kids(tree)[0]
    return xblock(block)
