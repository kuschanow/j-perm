"""Stage sub-package — concrete StageProcessor implementations and helpers."""

from .shorthands import (
    AssertShorthandMatcher,
    AssertShorthandProcessor,
    DeleteShorthandMatcher,
    DeleteShorthandProcessor,
    AssignShorthandMatcher,
    AssignShorthandProcessor,
    build_default_shorthand_stages,
)

__all__ = [
    "AssertShorthandMatcher",
    "AssertShorthandProcessor",
    "DeleteShorthandMatcher",
    "DeleteShorthandProcessor",
    "AssignShorthandMatcher",
    "AssignShorthandProcessor",
    "build_default_shorthand_stages",
]
