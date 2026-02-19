"""Tests for context pointer prefixes: &: (temp_read_only), !: (temp), _: (source).

Prefix table:
  /path       → source
  @:/path     → dest
  &:/path     → temp_read_only  (function args, loop variables, try-error info)
  !:/path     → temp            (mutable scratch space, not in final output)
  _:/path     → source          (alias, same as /path)
"""

import pytest
from j_perm import build_default_engine, ExecutionContext


# ─────────────────────────────────────────────────────────────────────────────
# &: prefix — temp_read_only (args)
# ─────────────────────────────────────────────────────────────────────────────


class TestAmpersandPrefix:
    """&: reads from ctx.temp_read_only (function params, foreach var, try errors)."""

    def test_ampersand_reads_function_param(self):
        """&:/param_name resolves to the function argument."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "greet",
                    "params": ["name"],
                    "body": [{"/msg": "Hello, ${&:/name}!"}],
                    "return": "/msg",
                },
                {"/result": {"$func": "greet", "args": ["World"]}},
            ],
            source={},
            dest={},
        )

        assert result == {"result": "Hello, World!"}

    def test_ampersand_ref_reads_function_param(self):
        """$ref with &:/param also resolves function argument."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "double",
                    "params": ["x"],
                    "body": [{"/val": {"$add": [{"$ref": "&:/x"}, {"$ref": "&:/x"}]}}],
                    "return": "/val",
                },
                {"/result": {"$func": "double", "args": [5]}},
            ],
            source={},
            dest={},
        )

        assert result == {"result": 10}

    def test_ampersand_reads_foreach_var(self):
        """&:/item resolves to the current foreach loop variable."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/items",
                "as": "item",
                "do": {"/out[]": "&:/item"},
            },
            source={"items": [1, 2, 3]},
            dest={},
        )

        assert result == {"out": [1, 2, 3]}

    def test_ampersand_reads_custom_foreach_var_name(self):
        """&:/x resolves when 'as' is set to 'x'."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/letters",
                "as": "x",
                "do": {"/out[]": "${&:/x}"},
            },
            source={"letters": ["a", "b"]},
            dest={},
        )

        assert result == {"out": ["a", "b"]}

    def test_ampersand_reads_try_error_message(self):
        """&:/_error_message contains the error message inside an except block."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [{"$raise": "boom"}],
                "except": [{"/caught": "${&:/_error_message}"}],
            },
            source={},
            dest={},
        )

        assert result == {"caught": "boom"}

    def test_ampersand_reads_try_error_type(self):
        """&:/_error_type contains the exception class name inside an except block."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "try",
                "do": [{"$raise": "oops"}],
                "except": [{"/etype": "${&:/_error_type}"}],
            },
            source={},
            dest={},
        )

        assert result["etype"] == "JPermError"

    def test_ampersand_in_jmespath(self):
        """args.* namespace in JMESPath templates resolves temp_read_only."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "f",
                    "params": ["n"],
                    "body": [{"/doubled": "${?add(args.n, args.n)}"}],
                    "return": "/doubled",
                },
                {"/result": {"$func": "f", "args": [7]}},
            ],
            source={},
            dest={},
        )

        assert result == {"result": 14}


# ─────────────────────────────────────────────────────────────────────────────
# !: prefix — temp (mutable scratch space)
# ─────────────────────────────────────────────────────────────────────────────


class TestExclamationPrefix:
    """!: reads/writes ctx.temp — mutable scratch space shared within a call."""

    def test_exclamation_reads_from_temp(self):
        """!:/key resolves a value placed into ctx.temp by engine internals or a handler."""
        engine = build_default_engine()

        # Populate temp via ExecutionContext directly in a custom scenario.
        # We use apply_to_context with a pre-populated ctx to verify the read path.
        ctx = ExecutionContext(
            source={},
            dest={},
            engine=engine,
            temp={"scratch": "hello"},
        )
        engine.apply_to_context([{"/result": "${!:/scratch}"}], ctx)

        assert ctx.dest["result"] == "hello"

    def test_exclamation_in_jmespath(self):
        """temp.* namespace in JMESPath resolves ctx.temp."""
        engine = build_default_engine()

        ctx = ExecutionContext(
            source={},
            dest={},
            engine=engine,
            temp={"value": 42},
        )
        engine.apply_to_context([{"/result": "${?temp.value}"}], ctx)

        assert ctx.dest["result"] == 42


# ─────────────────────────────────────────────────────────────────────────────
# _: prefix — source (alias for plain /)
# ─────────────────────────────────────────────────────────────────────────────


class TestUnderscorePrefix:
    """_: resolves against ctx.source, same as a plain / pointer."""

    def test_underscore_reads_source(self):
        """_:/path reads from source, identical to /path."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": "${_:/name}"},
            source={"name": "Alice"},
            dest={},
        )

        assert result == {"result": "Alice"}

    def test_underscore_ref_reads_source(self):
        """$ref with _:/ reads from source."""
        engine = build_default_engine()

        result = engine.apply(
            {"/result": {"$ref": "_:/name"}},
            source={"name": "Bob"},
            dest={},
        )

        assert result == {"result": "Bob"}

    def test_underscore_same_as_plain_slash(self):
        """_:/path and /path produce identical results."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/via_plain": "${/value}"},
                {"/via_underscore": "${_:/value}"},
            ],
            source={"value": 99},
            dest={},
        )

        assert result["via_plain"] == result["via_underscore"] == 99

    def test_underscore_in_function_body_reads_original_source(self):
        """Inside a function body, _:/path still reads the original source."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "getConfig",
                    "body": [{"/cfg": {"$ref": "_:/config/key"}}],
                    "return": "/cfg",
                },
                {"/result": {"$func": "getConfig"}},
            ],
            source={"config": {"key": "production"}},
            dest={},
        )

        assert result == {"result": "production"}


# ─────────────────────────────────────────────────────────────────────────────
# args.* in JMESPath — temp_read_only namespace
# ─────────────────────────────────────────────────────────────────────────────


class TestArgsJMESPathNamespace:
    """JMESPath 'args.*' resolves against ctx.temp_read_only."""

    def test_args_in_foreach_condition(self):
        """args.item accessible via JMESPath in foreach do-block."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/users",
                "as": "item",
                "do": {
                    "op": "if",
                    "cond": "${?args.item.age >= `18`}",
                    "then": {"/adults[]": "&:/item"},
                },
            },
            source={"users": [{"name": "Alice", "age": 17}, {"name": "Bob", "age": 22}]},
            dest={},
        )

        assert result == {"adults": [{"name": "Bob", "age": 22}]}

    def test_args_in_function_jmespath(self):
        """args.param accessible via JMESPath inside function body."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {
                    "$def": "double_it",
                    "params": ["n"],
                    "body": [{"/val": "${?add(args.n, args.n)}"}],
                    "return": "/val",
                },
                {"/result": {"$func": "double_it", "args": [6]}},
            ],
            source={},
            dest={},
        )

        assert result == {"result": 12}


# ─────────────────────────────────────────────────────────────────────────────
# Shorthand assignment with new prefixes
# ─────────────────────────────────────────────────────────────────────────────


class TestShorthandPrefixRecognition:
    """AssignShorthandProcessor treats &:, !:, @:, _: as copy-from pointers."""

    def test_shorthand_ampersand_prefix_in_foreach(self):
        """Shorthand '/dest': '&:/src' expands to copy inside foreach."""
        engine = build_default_engine()

        result = engine.apply(
            {
                "op": "foreach",
                "in": "/nums",
                "as": "n",
                "do": {"/out[]": "&:/n"},
            },
            source={"nums": [10, 20]},
            dest={},
        )

        assert result == {"out": [10, 20]}

    def test_shorthand_dest_prefix_in_body(self):
        """Shorthand '/y': '@:/x' copies from dest."""
        engine = build_default_engine()

        result = engine.apply(
            [
                {"/x": 5},
                {"/y": "@:/x"},
            ],
            source={},
            dest={},
        )

        assert result == {"x": 5, "y": 5}
