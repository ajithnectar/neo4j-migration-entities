from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterable, Sequence, Any

import psycopg2
from psycopg2.extensions import connection as _PGConnection
from psycopg2.extras import execute_batch

from app_config.settings import PostgresConfig

logger = logging.getLogger(__name__)


def create_pg_connection(cfg: PostgresConfig) -> _PGConnection:
    logger.info(
        "Connecting to PostgreSQL: host=%s port=%s db=%s",
        cfg.host,
        cfg.port,
        cfg.dbname,
    )
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.username,
        password=cfg.password,
    )
    conn.autocommit = False
    return conn


@contextmanager
def pg_connection(cfg: PostgresConfig) -> Iterable[_PGConnection]:
    conn = create_pg_connection(cfg)
    try:
        yield conn
        conn.commit()
    except Exception:
        logger.exception("Error during PostgreSQL operation, rolling back")
        conn.rollback()
        raise
    finally:
        conn.close()


def batch_insert(
    conn: _PGConnection,
    insert_sql: str,
    rows: Sequence[Sequence[Any]],
    page_size: int = 1000,
) -> None:
    """
    Efficiently insert many rows with a prepared INSERT statement.
    Example insert_sql:
        INSERT INTO domains (id, name) VALUES (%s, %s)
    """
    logger.info("Inserting %s rows into PostgreSQL", len(rows))
    with conn.cursor() as cur:
        execute_batch(cur, insert_sql, rows, page_size=page_size)


