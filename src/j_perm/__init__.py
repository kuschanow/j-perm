"""j_perm DSL engine.

Package layout
--------------
core.py          – ABCs, registries, Pipeline, Engine, ExecutionContext, UnescapeRule
matchers.py      – shared ActionMatchers (OpMatcher, AlwaysMatcher)
stages/          – StageProcessor impls (shorthand expansion, …)
handlers/        – ActionHandler + co-located ActionMatcher impls, by logical system
                     template   – ``${…}`` substitution
                     special    – ``$ref`` / ``$eval`` dispatch
                     container  – recursive list/dict descent
                     identity   – scalar pass-through
resolvers/       – ValueResolver impls (JSON Pointer, …)
factory.py       – ``build_default_engine``
"""

# -- core ----------------------------------------------------------
from .core import (
    ExecutionContext,
    ValueResolver,
    StageMatcher,
    StageProcessor,
    AsyncStageProcessor,
    StageNode,
    StageRegistry,
    Stage,  # alias → StageProcessor
    Middleware,
    AsyncMiddleware,
    ActionMatcher,
    ActionHandler,
    AsyncActionHandler,
    ActionNode,
    ActionTypeRegistry,
    Pipeline,
    Engine,
    UnescapeRule
)
# -- factory -------------------------------------------------------
from .factory import build_default_engine
# -- handlers (grouped by logical system) --------------------------
from .handlers import (
    # template
    TemplMatcher,
    TemplSubstHandler,
    template_unescape,
    # special
    SpecialFn,
    SpecialMatcher,
    SpecialResolveHandler,
    # constructs
    ref_handler,
    eval_handler,
    and_handler,
    or_handler,
    not_handler,
    # container
    ContainerMatcher,
    RecursiveDescentHandler,
    # identity
    IdentityHandler,
    # ops
    SetHandler,
    CopyHandler,
    DeleteHandler,
    ForeachHandler,
    WhileHandler,
    IfHandler,
    ExecHandler,
    UpdateHandler,
    DistinctHandler,
    AssertHandler,
    # function
    DefMatcher,
    CallMatcher,
    DefHandler,
    CallHandler,
    RaiseMatcher,
    RaiseHandler,
    JPermError,
)
# -- shared matchers -----------------------------------------------
from .matchers import (
    OpMatcher,
    AlwaysMatcher,
)
# -- resolvers -----------------------------------------------------
from .resolvers import PointerResolver
# -- stages --------------------------------------------------------
from .stages import (
    AssertShorthandMatcher,
    AssertShorthandProcessor,
    DeleteShorthandMatcher,
    DeleteShorthandProcessor,
    AssignShorthandMatcher,
    AssignShorthandProcessor,
    build_default_shorthand_stages,
)
# -- pointer processor ------------------------------------------------
from .pointer_processor import PointerProcessor

__all__ = [
    # core
    "ExecutionContext",
    "ValueResolver",
    "StageMatcher",
    "StageProcessor",
    "AsyncStageProcessor",
    "StageNode",
    "StageRegistry",
    "Stage",
    "Middleware",
    "AsyncMiddleware",
    "ActionMatcher",
    "ActionHandler",
    "AsyncActionHandler",
    "ActionNode",
    "ActionTypeRegistry",
    "Pipeline",
    "Engine",
    "UnescapeRule",
    # shared matchers
    "OpMatcher",
    "AlwaysMatcher",
    # stages
    "AssertShorthandMatcher",
    "AssertShorthandProcessor",
    "DeleteShorthandMatcher",
    "DeleteShorthandProcessor",
    "AssignShorthandMatcher",
    "AssignShorthandProcessor",
    "build_default_shorthand_stages",
    # handlers
    "TemplMatcher",
    "TemplSubstHandler",
    "template_unescape",
    "SpecialFn",
    "SpecialMatcher",
    "SpecialResolveHandler",
    "ref_handler",
    "eval_handler",
    "and_handler",
    "or_handler",
    "not_handler",
    "ContainerMatcher",
    "RecursiveDescentHandler",
    "IdentityHandler",
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
    "DefMatcher",
    "CallMatcher",
    "DefHandler",
    "CallHandler",
    "RaiseMatcher",
    "RaiseHandler",
    "JPermError",
    # resolvers
    "PointerResolver",
    # factory
    "build_default_engine",
    # pointer processor
    "PointerProcessor",
]
