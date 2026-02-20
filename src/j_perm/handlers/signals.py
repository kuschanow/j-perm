"""Control flow signal exceptions for loops, functions, and value pipeline.

These exceptions are used internally to implement control flow commands.
They are *not* errors — they are intentional signals that propagate up the
call stack to be caught by the appropriate handler.

``BreakSignal``, ``ContinueSignal``, ``ReturnSignal`` are local signals caught
by loop/function handlers — they do **not** inherit from ``PipelineSignal``.

``RawValueSignal`` is a ``PipelineSignal``: ``Pipeline.run`` intercepts it via
``handle(ctx)`` which re-raises, causing ``Engine.process_value`` to stop the
stabilisation loop and return the raw value as-is.
"""

from ..core import ControlFlowSignal, PipelineSignal


class BreakSignal(ControlFlowSignal):
    """Raised by ``$break`` to exit the innermost ``foreach`` or ``while`` loop."""

    def __init__(self) -> None:
        super().__init__("$break used outside of a loop")


class ContinueSignal(ControlFlowSignal):
    """Raised by ``$continue`` to skip to the next iteration of the innermost loop."""

    def __init__(self) -> None:
        super().__init__("$continue used outside of a loop")


class ReturnSignal(ControlFlowSignal):
    """Raised by ``$return`` to exit the current function with a value.

    Attributes:
        value: The return value (result of evaluating the ``$return`` expression).
    """

    def __init__(self, value=None) -> None:
        self.value = value
        super().__init__("$return used outside of a function")


class RawValueSignal(PipelineSignal):
    """Raised to short-circuit the value-pipeline stabilisation loop.

    Inherits from ``PipelineSignal`` so ``Pipeline.run`` intercepts it and
    calls ``handle(ctx)``, which updates ``ctx.dest`` and re-raises — causing
    ``Engine.process_value`` to catch the propagated signal and break the loop.

    Two usage patterns:

    * **Wrapper construct** — use ``$raw`` as the only key to return a literal
      without *any* value-pipeline processing::

          {"$raw": {"$ref": "/not/evaluated"}}
          {"$raw": "hello ${not_substituted}"}

      ``raw_handler`` raises ``RawValueSignal(node["$raw"])`` directly.

    * **Flag on any construct** — add ``"$raw": True`` to stop further
      iterations after the construct resolves::

          {"$ref": "&:result", "$raw": True}

      ``SpecialResolveHandler`` (and any other handler that opts in) raises
      ``RawValueSignal(result)`` after executing the primary construct.

    Attributes:
        value: The final value to return unchanged.
    """

    def __init__(self, value=None) -> None:
        self.value = value
        super().__init__("$raw: stop further value-pipeline processing")

    def handle(self, ctx) -> None:
        ctx.dest = self.value
        raise self