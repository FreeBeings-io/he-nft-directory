"""Postgres layer: schema management and small helpers."""

from __future__ import annotations

from importlib import resources

import psycopg
from psycopg.rows import dict_row


async def connect(dsn: str, *, autocommit: bool = False) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row, autocommit=autocommit)


def connect_sync(dsn: str) -> psycopg.Connection:
    """Synchronous connection for the WSGI API layer."""
    return psycopg.connect(dsn, row_factory=dict_row)


async def apply_schema(conn: psycopg.AsyncConnection) -> None:
    sql = resources.files("henftdir").joinpath("schema.sql").read_text()
    await conn.execute(sql)
    await conn.commit()
