"""PointerProcessor - JSON pointer handler with data source prefix support.

Supported prefixes:
    @:/path  - access dest (or _real_dest from metadata)
    _:/path  - access metadata
    /path    - access source (default)

Examples:
    processor.get("@:/user/name", ctx)  # reads from ctx.dest
    processor.get("_:/config", ctx)      # reads from ctx.metadata
    processor.get("/data", ctx)          # reads from ctx.source
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Tuple

if TYPE_CHECKING:
    from .core import ExecutionContext
    from .resolvers.pointer import ValueResolver


class PointerProcessor:
    """Processes pointers with prefixes and delegates calls to ValueResolver."""

    def resolve(self, path: str, ctx: ExecutionContext) -> Tuple[str, Any]:
        """Resolves path with prefix and returns normalized path and data source.

        Args:
            path: Path with optional prefix (@:/, _:/, or plain /)
            ctx: Execution context

        Returns:
            Tuple of (normalized_path, data_object)

        Examples:
            "@:/user/name" -> ("/user/name", ctx.dest)
            "_:/config" -> ("/config", ctx.metadata)
            "/data" -> ("/data", ctx.source)
        """
        if path.startswith("@:/") or path.startswith("@:"):
            # Dest pointer
            normalized = path[2:].lstrip("/")
            normalized = "/" + normalized if normalized else "/"
            # Use _real_dest from metadata if available (for nested value contexts)
            data_source = ctx.metadata.get('_real_dest', ctx.dest)
            return normalized, data_source

        elif path.startswith("_:/") or path.startswith("_:"):
            # Metadata pointer
            normalized = path[2:].lstrip("/")
            normalized = "/" + normalized if normalized else "/"
            return normalized, ctx.metadata

        else:
            # Source pointer (default)
            return path, ctx.source

    def get(self, pointer: str, ctx: ExecutionContext) -> Any:
        """Gets value by pointer with prefix support.

        Args:
            pointer: JSON pointer with optional prefix
            ctx: Execution context

        Returns:
            Value at the specified path

        Raises:
            KeyError: If path does not exist
        """
        processed_path, data_source = self.resolve(pointer, ctx)
        return ctx.engine.resolver.get(processed_path, data_source)

    def set(
        self,
        pointer: str,
        ctx: ExecutionContext,
        value: Any
    ) -> None:
        """Sets value by pointer in dest.

        Args:
            pointer: JSON pointer (prefixes ignored - always writes to dest)
            ctx: Execution context
            value: Value to set

        Note:
            Write operations always execute in ctx.dest, as source is immutable.
            Prefixes @:/, _:/ are ignored.
        """
        # Remove prefix if present (set always to dest)
        clean_path = pointer
        if pointer.startswith("@:/") or pointer.startswith("@:"):
            clean_path = "/" + pointer[2:].lstrip("/")
        elif pointer.startswith("_:/") or pointer.startswith("_:"):
            clean_path = "/" + pointer[2:].lstrip("/")

        ctx.engine.resolver.set(clean_path, ctx.dest, value)

    def delete(
        self,
        pointer: str,
        ctx: ExecutionContext,
        ignore_missing: bool = False
    ) -> None:
        """Deletes value by pointer from dest.

        Args:
            pointer: JSON pointer (prefixes ignored - always deletes from dest)
            ctx: Execution context
            ignore_missing: Whether to ignore missing paths

        Raises:
            KeyError: If path does not exist and ignore_missing=False

        Note:
            Delete operations always execute in ctx.dest, as source is immutable.
            Prefixes @:/, _:/ are ignored for backward compatibility.
        """
        # Remove prefix if present (delete always from dest)
        clean_path = pointer
        if pointer.startswith("@:/") or pointer.startswith("@:"):
            clean_path = "/" + pointer[2:].lstrip("/")
        elif pointer.startswith("_:/") or pointer.startswith("_:"):
            clean_path = "/" + pointer[2:].lstrip("/")

        # resolver.delete doesn't support ignore_missing, handle manually
        if ignore_missing and not self.exists("@:" + clean_path, ctx):
            return

        ctx.engine.resolver.delete(clean_path, ctx.dest)

    def exists(self, pointer: str, ctx: ExecutionContext) -> bool:
        """Checks if path exists with prefix support.

        Args:
            pointer: JSON pointer with optional prefix
            ctx: Execution context

        Returns:
            True if path exists, False otherwise
        """
        processed_path, data_source = self.resolve(pointer, ctx)
        return ctx.engine.resolver.exists(processed_path, data_source)