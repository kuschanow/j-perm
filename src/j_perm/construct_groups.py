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
    ref_handler, eval_handler,
    # Logical
    and_handler, or_handler, not_handler,
    # Comparison
    gt_handler, gte_handler, lt_handler, lte_handler, eq_handler, ne_handler, in_handler,
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