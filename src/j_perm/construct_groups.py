"""Pre-defined groups of construct handlers for convenient registration.

This module provides organized groups of handlers that can be used when
building custom engines or extending the default engine.

Example usage::

    from j_perm import build_default_engine
    from j_perm.construct_groups import STRING_HANDLERS, REGEX_HANDLERS

    # Build engine with only specific groups
    engine = build_default_engine(specials={
        **STRING_HANDLERS,
        **REGEX_HANDLERS,
    })

    # Or extend default engine with additional handlers
    from j_perm.construct_groups import ALL_HANDLERS
    engine = build_default_engine(specials={
        **ALL_HANDLERS,
        "$custom": my_custom_handler,
    })
"""
from .handlers.constructs import (
    # Core
    ref_handler, eval_handler, raw_handler,
    # Logical
    and_handler, or_handler, not_handler,
    # Comparison
    gt_handler, gte_handler, lt_handler, lte_handler, eq_handler, ne_handler, in_handler,
    exists_handler,
    # Math
    add_handler, sub_handler, mul_handler, div_handler, pow_handler, mod_handler,
    # String
    str_split_handler, str_join_handler, str_slice_handler,
    str_upper_handler, str_lower_handler,
    str_strip_handler, str_lstrip_handler, str_rstrip_handler,
    str_replace_handler, str_contains_handler,
    str_startswith_handler, str_endswith_handler,
    # Regex
    regex_match_handler, regex_search_handler, regex_findall_handler,
    regex_replace_handler, regex_groups_handler,
)

# ─────────────────────────────────────────────────────────────────────────────
# Core handlers
# ─────────────────────────────────────────────────────────────────────────────

CORE_HANDLERS = {
    "$ref": ref_handler,
    "$eval": eval_handler,
    "$raw": raw_handler,
}

# ─────────────────────────────────────────────────────────────────────────────
# Logical operators
# ─────────────────────────────────────────────────────────────────────────────

LOGICAL_HANDLERS = {
    "$and": and_handler,
    "$or": or_handler,
    "$not": not_handler,
}

# ─────────────────────────────────────────────────────────────────────────────
# Comparison operators
# ─────────────────────────────────────────────────────────────────────────────

COMPARISON_HANDLERS = {
    "$gt": gt_handler,
    "$gte": gte_handler,
    "$lt": lt_handler,
    "$lte": lte_handler,
    "$eq": eq_handler,
    "$ne": ne_handler,
    "$in": in_handler,
    "$exists": exists_handler,
}

# ─────────────────────────────────────────────────────────────────────────────
# Mathematical operators
# ─────────────────────────────────────────────────────────────────────────────

MATH_HANDLERS = {
    "$add": add_handler,
    "$sub": sub_handler,
    "$mul": mul_handler,
    "$div": div_handler,
    "$pow": pow_handler,
    "$mod": mod_handler,
}

# ─────────────────────────────────────────────────────────────────────────────
# String operations
# ─────────────────────────────────────────────────────────────────────────────

STRING_HANDLERS = {
    "$str_split": str_split_handler,
    "$str_join": str_join_handler,
    "$str_slice": str_slice_handler,
    "$str_upper": str_upper_handler,
    "$str_lower": str_lower_handler,
    "$str_strip": str_strip_handler,
    "$str_lstrip": str_lstrip_handler,
    "$str_rstrip": str_rstrip_handler,
    "$str_replace": str_replace_handler,
    "$str_contains": str_contains_handler,
    "$str_startswith": str_startswith_handler,
    "$str_endswith": str_endswith_handler,
}

# ─────────────────────────────────────────────────────────────────────────────
# Regular expression operations
# ─────────────────────────────────────────────────────────────────────────────

REGEX_HANDLERS = {
    "$regex_match": regex_match_handler,
    "$regex_search": regex_search_handler,
    "$regex_findall": regex_findall_handler,
    "$regex_replace": regex_replace_handler,
    "$regex_groups": regex_groups_handler,
}

# ─────────────────────────────────────────────────────────────────────────────
# Combined groups
# ─────────────────────────────────────────────────────────────────────────────

# All handlers except $cast (which requires casters parameter)
ALL_HANDLERS_NO_CAST = {
    **CORE_HANDLERS,
    **LOGICAL_HANDLERS,
    **COMPARISON_HANDLERS,
    **MATH_HANDLERS,
    **STRING_HANDLERS,
    **REGEX_HANDLERS,
}


# Helper function to get all handlers including $cast
def get_all_handlers(casters=None):
    """Get all handlers including $cast with specified casters.

    Args:
        casters: Optional dict of casters for $cast handler.
                 If None, uses BUILTIN_CASTERS.

    Returns:
        Dict of all handler constructs.

    Example::

        from j_perm.construct_groups import get_all_handlers
        from j_perm.casters import BUILTIN_CASTERS

        handlers = get_all_handlers(casters=BUILTIN_CASTERS)
        engine = build_default_engine(specials=handlers)
    """
    from .handlers.constructs import make_cast_handler
    from .casters import BUILTIN_CASTERS

    resolved_casters = casters if casters is not None else BUILTIN_CASTERS

    return {
        **ALL_HANDLERS_NO_CAST,
        "$cast": make_cast_handler(resolved_casters),
    }


def get_all_handlers_with_limits(
        casters=None,
        regex_timeout=2.0,
        regex_allowed_flags=None,
        pow_max_base=1e6,
        pow_max_exponent=1000,
        mul_max_string_result=1_000_000,
        mul_max_operand=1e9,
        add_max_number_result=1e15,
        add_max_string_result=100_000_000,
        sub_max_number_result=1e15,
        str_max_split_results=100_000,
        str_max_join_result=10_000_000,
        str_max_replace_result=10_000_000,
):
    """Get all handlers with custom security limits.

    Args:
        casters: Optional dict of casters for $cast handler.
        regex_timeout: Timeout for regex operations in seconds.
        regex_allowed_flags: Bitmask of allowed regex flags. None means all allowed.
        pow_max_base: Maximum base value for $pow.
        pow_max_exponent: Maximum exponent value for $pow.
        mul_max_string_result: Maximum length of string result in $mul.
        mul_max_operand: Maximum numeric operand value in $mul.
        add_max_number_result: Maximum numeric result from $add.
        add_max_string_result: Maximum string length result from $add.
        sub_max_number_result: Maximum numeric result from $sub.
        str_max_split_results: Maximum number of results from $str_split.
        str_max_join_result: Maximum length of result from $str_join.
        str_max_replace_result: Maximum length of result from $str_replace.

    Returns:
        Dict of all handler constructs with specified limits.

    Example::

        from j_perm.construct_groups import get_all_handlers_with_limits

        handlers = get_all_handlers_with_limits(
            regex_timeout=5.0,
            pow_max_exponent=500,
            add_max_string_result=1_000_000,
        )
        engine = build_default_engine(specials=handlers)
    """
    from .handlers.constructs import (
        make_cast_handler,
        make_add_handler,
        make_sub_handler,
        make_mul_handler,
        make_pow_handler,
        make_str_split_handler,
        make_str_join_handler,
        make_str_replace_handler,
        make_regex_match_handler,
        make_regex_search_handler,
        make_regex_findall_handler,
        make_regex_replace_handler,
        make_regex_groups_handler,
    )
    from .casters import BUILTIN_CASTERS

    resolved_casters = casters if casters is not None else BUILTIN_CASTERS

    return {
        **CORE_HANDLERS,
        **LOGICAL_HANDLERS,
        **COMPARISON_HANDLERS,
        # Math handlers with limits
        "$add": make_add_handler(
            max_number_result=add_max_number_result,
            max_string_result=add_max_string_result,
        ),
        "$sub": make_sub_handler(
            max_number_result=sub_max_number_result,
        ),
        "$mul": make_mul_handler(
            max_string_result=mul_max_string_result,
            max_operand=mul_max_operand,
        ),
        "$div": div_handler,
        "$pow": make_pow_handler(
            max_base=pow_max_base,
            max_exponent=pow_max_exponent,
        ),
        "$mod": mod_handler,
        # String handlers with limits
        "$str_split": make_str_split_handler(
            max_results=str_max_split_results,
        ),
        "$str_join": make_str_join_handler(
            max_result_length=str_max_join_result,
        ),
        "$str_slice": str_slice_handler,
        "$str_upper": str_upper_handler,
        "$str_lower": str_lower_handler,
        "$str_strip": str_strip_handler,
        "$str_lstrip": str_lstrip_handler,
        "$str_rstrip": str_rstrip_handler,
        "$str_replace": make_str_replace_handler(
            max_result_length=str_max_replace_result,
        ),
        "$str_contains": str_contains_handler,
        "$str_startswith": str_startswith_handler,
        "$str_endswith": str_endswith_handler,
        # Regex handlers with limits
        "$regex_match": make_regex_match_handler(
            timeout=regex_timeout,
            allowed_flags=regex_allowed_flags,
        ),
        "$regex_search": make_regex_search_handler(
            timeout=regex_timeout,
            allowed_flags=regex_allowed_flags,
        ),
        "$regex_findall": make_regex_findall_handler(
            timeout=regex_timeout,
            allowed_flags=regex_allowed_flags,
        ),
        "$regex_replace": make_regex_replace_handler(
            timeout=regex_timeout,
            allowed_flags=regex_allowed_flags,
        ),
        "$regex_groups": make_regex_groups_handler(
            timeout=regex_timeout,
            allowed_flags=regex_allowed_flags,
        ),
        "$cast": make_cast_handler(resolved_casters),
    }
