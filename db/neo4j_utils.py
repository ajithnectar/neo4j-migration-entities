import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, List, Optional

from neo4j import GraphDatabase, Driver, Session

from app_config.settings import Neo4jConfig

logger = logging.getLogger(__name__)


def create_neo4j_driver(cfg):
    # type: (...) -> Driver
    """
    Create Neo4j driver - matches the working script approach.
    The working script uses neo4j 5.x which supports protocol 5.0.
    For Python 3.6, use neo4j 4.4.18 (supports up to protocol 4.1).
    For Python 3.7+, use neo4j 5.x (supports protocol 5.0+).
    """
    logger.info("Creating Neo4j driver for %s", cfg.uri)
    # Same approach as working script - just create driver without verify_connectivity
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
    parameters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    logger.debug("Running Cypher: %s", cypher)
    result = session.run(cypher, parameters or {})
    records = [record.data() for record in result]
    logger.debug("Fetched %s records from Neo4j", len(records))
    return records


def stream_query(
    session: Session,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    transform: Optional[Callable[[Dict[str, Any]], Any]] = None,
) -> Iterable[Any]:
    logger.debug("Streaming Cypher: %s", cypher)
    result = session.run(cypher, parameters or {})
    for record in result:
        data = record.data()
        yield transform(data) if transform else data


