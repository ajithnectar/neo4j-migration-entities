from __future__ import annotations

import logging
from typing import Iterable, Sequence

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.neo4j_utils import run_query
from db.postgres_utils import batch_insert

logger = logging.getLogger(__name__)


def fetch_domains(session: Session) -> Iterable[dict]:
    cypher = """
    MATCH (n:DefaultTenant)
    RETURN
        coalesce(n.id, id(n)) AS tenant_id,
        n.clientId AS client_id,
        n.clientName AS client_name,
        coalesce(n.status, 'ACTIVE') AS status,
        n.applicationUrl AS application_url,
        n.settingsClientId AS settings_client_id
    ORDER BY tenant_id
    """
    return run_query(session, cypher)


def map_domains_to_rows(domains: Iterable[dict]) -> Sequence[tuple]:
    rows: list[tuple] = []
    for domain_id, d in enumerate(domains, start=1):
        client_id = d.get("client_id") or d.get("tenant_id")
        rows.append(
            (
                domain_id,  # domain_id: sequential 1..n as requested
                client_id,
                d.get("client_name"),
                d.get("status") or "ACTIVE",
                d.get("application_url"),
                d.get("settings_client_id"),
            )
        )
    return rows


def migrate_domains(session: Session, conn: _PGConnection) -> None:
    logger.info("Starting domain migration")
    domains = fetch_domains(session)
    rows = map_domains_to_rows(domains)

    if not rows:
        logger.info("No domains found to migrate")
        return

    insert_sql = """
        INSERT INTO public.domain (
            domain_id,
            domain,
            name,
            status,
            url_sub_domain,
            settings_client_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (domain_id) DO UPDATE SET
            domain = EXCLUDED.domain,
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            url_sub_domain = EXCLUDED.url_sub_domain,
            settings_client_id = EXCLUDED.settings_client_id
    """
    batch_insert(conn, insert_sql, rows)
    logger.info("Domain migration completed. Migrated %s rows", len(rows))


