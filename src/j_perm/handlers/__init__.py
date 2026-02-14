"""Handlers sub-package — concrete ActionHandler + ActionMatcher implementations,
grouped by logical system.

template   – ``${…}`` substitution + its matcher + unescape function
special    – special-construct dispatch (``$ref``, ``$eval``, ``$and``, ``$or``, ``$not``)
constructs – built-in special handlers (ref_handler, eval_handler, and_handler, or_handler, not_handler)
container  – recursive descent into lists / dicts
identity   – scalar pass-through (catch-all)
ops        – all 13 built-in operation handlers
function   – function definition, calling, and error raising
"""

from .constructs import (
    ref_handler, eval_handler, and_handler, or_handler, not_handler
)
from .container import ContainerMatcher, RecursiveDescentHandler
from .function import (
    DefMatcher, CallMatcher, DefHandler, CallHandler,
    RaiseMatcher, RaiseHandler, JPermError
)
from .identity import IdentityHandler
from .ops import (
    SetHandler, CopyHandler,
    DeleteHandler,
    ForeachHandler, WhileHandler, IfHandler, ExecHandler,
    UpdateHandler, DistinctHandler,
    AssertHandler,
)
from .special import SpecialFn, SpecialMatcher, SpecialResolveHandler
from .template import TemplMatcher, TemplSubstHandler, template_unescape

__all__ = [
    # template
    "TemplMatcher",
    "TemplSubstHandler",
    "template_unescape",
    # special
    "SpecialFn",
    "SpecialMatcher",
    "SpecialResolveHandler",
    # constructs
    "ref_handler",
    "eval_handler",
    "and_handler",
    "or_handler",
    "not_handler",
    # container
    "ContainerMatcher",
    "RecursiveDescentHandler",
    # identity
    "IdentityHandler",
    # ops
    "SetHandler",
    "CopyHandler",
    "DeleteHandler",
    "ForeachHandler",
    "WhileHandler",
    "IfHandler",
    "ExecHandler",
    "UpdateHandler",
    "DistinctHandler",
    "AssertHandler",
    # function
    "DefMatcher",
    "CallMatcher",
    "DefHandler",
    "CallHandler",
    "RaiseMatcher",
    "RaiseHandler",
    "JPermError",
]
