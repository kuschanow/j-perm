grammar earley
start sql_stmt

sql_stmt -> select_stmt
sql_stmt -> insert_stmt
sql_stmt -> update_stmt
sql_stmt -> delete_stmt

# ---------- SELECT ----------
select_stmt -> 'select' sel_cols 'from' table_ref joins where_opt group_opt having_opt order_opt limit_opt offset_opt

sel_cols -> STAR
sel_cols -> col_items
col_items -> col_item
col_items -> col_items ',' col_item
col_item -> expr
col_item -> expr 'as' IDENT

table_ref -> IDENT
table_ref -> IDENT IDENT
table_ref -> IDENT 'as' IDENT

joins ->
joins -> joins join_clause
join_clause -> jointype 'join' table_ref 'on' expr
jointype ->
jointype -> 'inner'
jointype -> 'left'
jointype -> 'right'
jointype -> 'full'
jointype -> 'cross'

where_opt ->
where_opt -> 'where' expr
group_opt ->
group_opt -> 'group' 'by' group_cols
group_cols -> expr
group_cols -> group_cols ',' expr
having_opt ->
having_opt -> 'having' expr
order_opt ->
order_opt -> 'order' 'by' order_items
order_items -> order_item
order_items -> order_items ',' order_item
order_item -> expr
order_item -> expr 'asc'
order_item -> expr 'desc'
limit_opt ->
limit_opt -> 'limit' INT
offset_opt ->
offset_opt -> 'offset' INT

# ---------- INSERT ----------
insert_stmt -> 'insert' 'into' IDENT collist_opt 'values' rows
collist_opt ->
collist_opt -> '(' name_list ')'
name_list -> IDENT
name_list -> name_list ',' IDENT
rows -> row
rows -> rows ',' row
row -> '(' val_list ')'
val_list -> expr
val_list -> val_list ',' expr

# ---------- UPDATE ----------
update_stmt -> 'update' IDENT 'set' assigns where_or_all
assigns -> assign_one
assigns -> assigns ',' assign_one
assign_one -> IDENT '=' expr
where_or_all -> 'where' expr
where_or_all -> 'all'

# ---------- DELETE ----------
delete_stmt -> 'delete' 'from' IDENT where_or_all

# ---------- expressions / predicates ----------
expr -> orx
orx -> orx 'or' andx
orx -> andx
andx -> andx 'and' notx
andx -> notx
notx -> 'not' notx
notx -> cmp
cmp -> cmp cmpop addx
cmp -> addx cmp_in
cmp -> addx
cmpop -> '='
cmpop -> '<>'
cmpop -> '!='
cmpop -> '<'
cmpop -> '<='
cmpop -> '>'
cmpop -> '>='
cmp_in -> 'in' '(' val_list ')'
addx -> addx '+' mulx
addx -> addx '-' mulx
addx -> mulx
mulx -> mulx '*' atom
mulx -> mulx '/' atom
mulx -> mulx '%' atom
mulx -> atom
atom -> INT
atom -> FLOAT
atom -> STRING
atom -> 'null'
atom -> 'true'
atom -> 'false'
atom -> read
atom -> colref
atom -> func
atom -> '(' expr ')'
read -> '$(' POINTER ')'
colref -> IDENT
colref -> IDENT '.' IDENT
colref -> IDENT '.' STAR
func -> IDENT '(' func_args ')'
func_args ->
func_args -> 'distinct' arg_list
func_args -> arg_list
func_args -> STAR
arg_list -> expr
arg_list -> arg_list ',' expr
