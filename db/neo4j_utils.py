from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Optional

from neo4j import GraphDatabase, Driver, Session

from app_config.settings import Neo4jConfig

logger = logging.getLogger(__name__)


def create_neo4j_driver(cfg: Neo4jConfig) -> Driver:
    logger.info("Creating Neo4j driver for %s", cfg.uri)
    driver = GraphDatabase.driver(cfg.uri, auth=(cfg.username, cfg.password))
    return driver


@contextmanager
def neo4j_session(driver: Driver) -> Iterable[Session]:
    session = driver.session()
    try:
        yield session
    finally:
        session.close()


def run_query(
    session: Session,
    cypher: str,
    parameters: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    logger.debug("Running Cypher: %s", cypher)
    result = session.run(cypher, parameters or {})
    records = [record.data() for record in result]
    logger.debug("Fetched %s records from Neo4j", len(records))
    return records


def stream_query(
    session: Session,
    cypher: str,
    parameters: Optional[dict[str, Any]] = None,
    transform: Optional[Callable[[dict[str, Any]], Any]] = None,
) -> Iterable[Any]:
    logger.debug("Streaming Cypher: %s", cypher)
    result = session.run(cypher, parameters or {})
    for record in result:
        data = record.data()
        yield transform(data) if transform else data


