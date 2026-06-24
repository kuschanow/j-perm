"""j_perm_sql — build and execute SQL queries from j-perm constructs.

SQL is described with a tree of ``$``-constructs (``$select``, ``$col``,
``$val``, predicates, joins, …) and rendered by an **isolated** named pipeline.
A single top-level operation, ``op: sql``, renders that tree to a parameterized
``(sql, params)`` pair and hands it to a configurable executor (any ORM's raw
execute).  The SQL constructs are never visible to the engine's normal value
pipeline — they only mean anything inside ``op: sql``.

Quick start::

    from j_perm import build_default_engine
    from j_perm_sql import install_sql

    engine = build_default_engine()
    install_sql(engine, my_orm_raw_execute, paramstyle="qmark")

    engine.apply(
        {"op": "sql", "to": "/rows", "query": {"$select": {
            "columns": [{"$col": {"name": "id"}}],
            "from": {"table": "users"},
            "where": {"$gte": [{"$col": {"name": "age"}}, {"$val": 18}]},
        }}},
        source={}, dest={},
    )
"""
from .constructs import build_sql_specials
from .dialect import PLACEHOLDER, RenderOptions
from .handler import AsyncSqlHandler, SqlHandler, SqlRenderer
from .install import install_sql
from .pipeline import SQL_PIPELINE_NAME, build_sql_pipeline
from .render import fragment, is_fragment, is_query, render

__all__ = [
    "install_sql",
    "RenderOptions",
    "PLACEHOLDER",
    "SqlHandler",
    "AsyncSqlHandler",
    "SqlRenderer",
    "build_sql_pipeline",
    "build_sql_specials",
    "SQL_PIPELINE_NAME",
    "fragment",
    "is_fragment",
    "is_query",
    "render",
]
