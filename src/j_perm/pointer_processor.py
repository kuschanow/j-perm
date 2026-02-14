"""PointerProcessor - обработчик JSON pointers с поддержкой префиксов источников данных.

Поддерживаемые префиксы:
    @:/path  - доступ к dest (или _real_dest из metadata)
    _:/path  - доступ к metadata
    /path    - доступ к source (по умолчанию)

Примеры:
    processor.get("@:/user/name", ctx)  # читает из ctx.dest
    processor.get("_:/config", ctx)      # читает из ctx.metadata
    processor.get("/data", ctx)          # читает из ctx.source
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Tuple

if TYPE_CHECKING:
    from .core import ExecutionContext
    from .resolvers.pointer import ValueResolver


class PointerProcessor:
    """Обрабатывает указатели с префиксами и делегирует вызовы ValueResolver."""

    def resolve(self, path: str, ctx: ExecutionContext) -> Tuple[str, Any]:
        """Разрешает путь с префиксом и возвращает нормализованный путь и источник данных.

        Args:
            path: Путь с возможным префиксом (@:/, _:/, или обычный /)
            ctx: Контекст выполнения

        Returns:
            Кортеж (нормализованный_путь, объект_данных)

        Examples:
            "@:/user/name" -> ("/user/name", ctx.dest)
            "_:/config" -> ("/config", ctx.metadata)
            "/data" -> ("/data", ctx.source)
        """
        if path.startswith("@:/") or path.startswith("@:"):
            # Dest pointer
            normalized = path[2:].lstrip("/")
            normalized = "/" + normalized if normalized else "/"
            # Используем _real_dest из metadata если доступен (для вложенных value контекстов)
            data_source = ctx.metadata.get('_real_dest', ctx.dest)
            return normalized, data_source

        elif path.startswith("_:/") or path.startswith("_:"):
            # Metadata pointer
            normalized = path[2:].lstrip("/")
            normalized = "/" + normalized if normalized else "/"
            return normalized, ctx.metadata

        else:
            # Source pointer (по умолчанию)
            return path, ctx.source

    def get(self, pointer: str, ctx: ExecutionContext) -> Any:
        """Получает значение по указателю с учетом префикса.

        Args:
            pointer: JSON pointer с возможным префиксом
            ctx: Контекст выполнения

        Returns:
            Значение по указанному пути

        Raises:
            KeyError: Если путь не существует
        """
        processed_path, data_source = self.resolve(pointer, ctx)
        return ctx.engine.resolver.get(processed_path, data_source)

    def set(
        self,
        pointer: str,
        ctx: ExecutionContext,
        value: Any
    ) -> None:
        """Устанавливает значение по указателю в dest.

        Args:
            pointer: JSON pointer (префиксы игнорируются - всегда пишет в dest)
            ctx: Контекст выполнения
            value: Значение для установки

        Note:
            Операции записи всегда выполняются в ctx.dest, т.к. source неизменяем.
            Префиксы @:/, _:/ игнорируются.
        """
        # Убираем префикс если есть (set всегда в dest)
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
        """Удаляет значение по указателю из dest.

        Args:
            pointer: JSON pointer (префиксы игнорируются - всегда удаляет из dest)
            ctx: Контекст выполнения
            ignore_missing: Игнорировать ли отсутствующие пути

        Raises:
            KeyError: Если путь не существует и ignore_missing=False

        Note:
            Операции удаления всегда выполняются в ctx.dest, т.к. source неизменяем.
            Префиксы @:/, _:/ игнорируются для обратной совместимости.
        """
        # Убираем префикс если есть (delete всегда из dest)
        clean_path = pointer
        if pointer.startswith("@:/") or pointer.startswith("@:"):
            clean_path = "/" + pointer[2:].lstrip("/")
        elif pointer.startswith("_:/") or pointer.startswith("_:"):
            clean_path = "/" + pointer[2:].lstrip("/")

        # resolver.delete не поддерживает ignore_missing, обрабатываем вручную
        if ignore_missing and not self.exists("@:" + clean_path, ctx):
            return

        ctx.engine.resolver.delete(clean_path, ctx.dest)

    def exists(self, pointer: str, ctx: ExecutionContext) -> bool:
        """Проверяет существование пути с учетом префикса.

        Args:
            pointer: JSON pointer с возможным префиксом
            ctx: Контекст выполнения

        Returns:
            True если путь существует, False иначе
        """
        processed_path, data_source = self.resolve(pointer, ctx)
        return ctx.engine.resolver.exists(processed_path, data_source)