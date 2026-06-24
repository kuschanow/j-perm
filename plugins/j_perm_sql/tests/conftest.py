"""Shared fixtures for the j_perm_sql test-suite."""
import pytest
from j_perm import ExecutionContext, build_default_engine

from j_perm_sql import RenderOptions, install_sql, install_sql_write
from j_perm_sql.handler import SqlRenderer
from j_perm_sql.pipeline import (
    SQL_PIPELINE_NAME,
    SQL_WRITE_PIPELINE_NAME,
    build_sql_pipeline,
    build_sql_write_pipeline,
)


@pytest.fixture
def opts():
    """Default render options (qmark, double-quote identifiers)."""
    return RenderOptions()


@pytest.fixture
def render_sql():
    """Render a SQL construct tree to ``(sql, params)`` for a given dialect.

    Self-contained: builds a fresh engine, registers the SQL pipeline, and
    renders via :class:`SqlRenderer`.  Accepts an optional ``source`` document
    so ``$val`` data references can be exercised.
    """
    def _render(query, *, opts=None, source=None):
        opts = opts or RenderOptions()
        engine = build_default_engine()
        engine.register_pipeline(SQL_PIPELINE_NAME, build_sql_pipeline(opts))
        ctx = ExecutionContext(source=source or {}, dest={}, engine=engine)
        return SqlRenderer(opts).render(query, ctx)

    return _render


@pytest.fixture
def ctx():
    """An execution context whose engine has the SQL pipeline registered."""
    engine = build_default_engine()
    engine.register_pipeline(SQL_PIPELINE_NAME, build_sql_pipeline())
    return ExecutionContext(source={}, dest={}, engine=engine)


@pytest.fixture
def render_write():
    """Render a DML construct tree to ``(sql, params)`` via the write pipeline."""
    def _render(query, *, opts=None, source=None):
        opts = opts or RenderOptions()
        engine = build_default_engine()
        engine.register_pipeline(SQL_WRITE_PIPELINE_NAME, build_sql_write_pipeline(opts))
        ctx = ExecutionContext(source=source or {}, dest={}, engine=engine)
        return SqlRenderer(opts, SQL_WRITE_PIPELINE_NAME).render(query, ctx)

    return _render


@pytest.fixture
def write_engine():
    """Factory: build an engine with write SQL installed and a recording executor.

    Returns ``(engine, calls)`` where *calls* accumulates ``(sql, params)``.
    """
    def _make(*, paramstyle="qmark", dialect=None, result=None, op="sql_write"):
        engine = build_default_engine()
        calls = []

        def executor(sql, params):
            calls.append((sql, params))
            return result if result is not None else {"rowcount": 1}

        install_sql_write(engine, executor, paramstyle=paramstyle, dialect=dialect, op=op)
        return engine, calls

    return _make


@pytest.fixture
def sql_engine():
    """Factory: build an engine with SQL installed and a recording executor.

    Returns ``(engine, calls)`` where *calls* accumulates ``(sql, params)``.
    """
    def _make(*, paramstyle="qmark", dialect=None, result=None, op="sql"):
        engine = build_default_engine()
        calls = []

        def executor(sql, params):
            calls.append((sql, params))
            return result if result is not None else {"rows": []}

        install_sql(engine, executor, paramstyle=paramstyle, dialect=dialect, op=op)
        return engine, calls

    return _make
