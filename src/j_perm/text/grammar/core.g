grammar earley
start program

program -> block

block -> seps_opt
block -> seps_opt stmts
stmts -> stmt seps_opt
stmts -> stmts stmt seps_opt
seps_opt ->
seps_opt -> seps_opt sep
sep -> NEWLINE
sep -> ';'

stmt -> assign
stmt -> copy
stmt -> del
stmt -> assertst
stmt -> ifst
stmt -> foreachst
stmt -> whilest
stmt -> dowhilest
stmt -> tryst
stmt -> defst
stmt -> returnst
stmt -> raisest
stmt -> breakst
stmt -> continuest
stmt -> execst
stmt -> opst
stmt -> callstmt

suite -> ':' NEWLINE INDENT block DEDENT
suite -> ':' stmt
suite -> '{' block '}'

assign -> POINTER '=' expr
assign -> POINTER '[' ']' '=' expr
copy -> POINTER '<-' POINTER
copy -> POINTER '<-' POINTER '??' expr
copy -> POINTER '<-!' POINTER
del -> 'del' ptr_list
del -> 'del' '!' ptr_list
ptr_list -> POINTER
ptr_list -> ptr_list ',' POINTER
assertst -> 'assert' POINTER
assertst -> 'assert' POINTER '==' expr

ifst -> 'if' expr suite elifs else_opt
elifs ->
elifs -> elifs seps_opt 'elif' expr suite
else_opt ->
else_opt -> seps_opt 'else' suite

foreachst -> 'foreach' IDENT 'in' expr suite
foreachst -> 'foreach' IDENT 'in' expr 'default' expr suite

whilest -> 'while' expr suite
dowhilest -> 'do' suite 'while' expr

tryst -> 'try' suite except_opt finally_opt
except_opt ->
except_opt -> 'except' suite
finally_opt ->
finally_opt -> 'finally' suite

defst -> 'def' IDENT '(' params ')' defopts suite failopt
params ->
params -> param_list
param_list -> IDENT
param_list -> param_list ',' IDENT
defopts ->
defopts -> 'context' '=' IDENT
failopt ->
failopt -> 'on_failure' suite

returnst -> 'return'
returnst -> 'return' expr
raisest -> 'raise' expr
breakst -> 'break'
continuest -> 'continue'
execst -> 'exec' POINTER
execst -> 'exec' POINTER 'merge'
execst -> 'exec' suite
opst -> 'op' STRING '(' args ')' suite
opst -> 'op' STRING '(' args ')'
callstmt -> call

expr -> coalesce
coalesce -> coalesce '??' orx
coalesce -> orx
orx -> orx 'or' andx
orx -> andx
andx -> andx 'and' notx
andx -> notx
notx -> 'not' notx
notx -> cmp
cmp -> cmp cmpop addx
cmp -> addx
cmpop -> '=='
cmpop -> '!='
cmpop -> '<'
cmpop -> '<='
cmpop -> '>'
cmpop -> '>='
cmpop -> 'in'
addx -> addx '+' mulx
addx -> addx '-' mulx
addx -> mulx
mulx -> mulx '*' powx
mulx -> mulx '/' powx
mulx -> mulx '%' powx
mulx -> powx
powx -> unary '**' powx
powx -> unary
unary -> '-' unary
unary -> 'exists' POINTER
unary -> atom

atom -> INT
atom -> FLOAT
atom -> STRING
atom -> 'true'
atom -> 'false'
atom -> 'null'
atom -> POINTER
atom -> read
atom -> call
atom -> listx
atom -> dictx
atom -> evalx
atom -> '(' expr ')'

read -> '$(' POINTER ')'
read -> '$(' POINTER ')' 'raw'
read -> '$(' POINTER '??' expr ')'

call -> IDENT '(' args ')'
call -> 'raw' '(' expr ')'
args ->
args -> arg_list
arg_list -> arg
arg_list -> arg_list ',' arg
arg -> expr
arg -> IDENT ':' expr

listx -> '[' elems ']'
elems ->
elems -> expr_list
expr_list -> expr
expr_list -> expr_list ',' expr

dictx -> '{' pairs '}'
pairs ->
pairs -> pair_list
pair_list -> pair
pair_list -> pair_list ',' pair
pair -> STRING ':' expr
pair -> IDENT ':' expr

evalx -> 'eval' '{' block '}'
evalx -> 'eval' '{' block '}' 'select' POINTER
