"""Control flow signal exceptions for loops and functions.

These exceptions are used internally to implement ``$break``, ``$continue``,
and ``$return`` control flow commands.  They are *not* errors — they are
intentional signals that propagate up the call stack to be caught by the
appropriate loop or function handler.
"""


class BreakSignal(Exception):
    """Raised by ``$break`` to exit the innermost ``foreach`` or ``while`` loop."""

    def __init__(self) -> None:
        super().__init__("$break used outside of a loop")


class ContinueSignal(Exception):
    """Raised by ``$continue`` to skip to the next iteration of the innermost loop."""

    def __init__(self) -> None:
        super().__init__("$continue used outside of a loop")


class ReturnSignal(Exception):
    """Raised by ``$return`` to exit the current function with a value.

    Attributes:
        value: The return value (result of evaluating the ``$return`` expression).
    """

    def __init__(self, value=None) -> None:
        self.value = value
        super().__init__("$return used outside of a function")
        