from __future__ import annotations

import logging
from typing import Iterable, Sequence

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.neo4j_utils import run_query
from db.postgres_utils import batch_insert
from datetime import datetime
from re import split

logger = logging.getLogger(__name__)


def fetch_clients(session: Session) -> Iterable[dict]:
    cypher = """
        MATCH (root:DefaultTenant {clientId: 'buildingdemo'})
        RETURN
            root.clientId AS client_id,
            root.clientName AS client_name,
            root.location AS location,
            root.locationName AS location_name,
            coalesce(root.status, 'ACTIVE') AS status,
            root.domain AS domain,
            root.typeName AS type_name,
            root.createdBy AS created_by,
            root.createdOn AS created_on,
            root.updatedBy AS updated_by,
            root.updatedOn AS updated_on,
            root.identifier AS identifier

        UNION

        MATCH (:DefaultTenant {clientId: 'buildingdemo'})-[:tenant]->(child)
        RETURN
            child.clientId AS client_id,
            child.clientName AS client_name,
            child.location AS location,
            child.locationName AS location_name,
            coalesce(child.status, 'ACTIVE') AS status,
            child.domain AS domain,
            child.typeName AS type_name,
            child.createdBy AS created_by,
            child.createdOn AS created_on,
            child.updatedBy AS updated_by,
            child.updatedOn AS updated_on,
            child.identifier AS identifier

        ORDER BY client_id;

    """
    return run_query(session, cypher)


def map_clients_to_rows(clients: Iterable[dict]) -> Sequence[tuple]:
    rows: list[tuple] = []
    for record in clients:
        client_id = record.get("client_id")
        domain = record.get("domain")
        typeOfClient = record.get("type_name").replace(" ", "")
        if domain and domain.lower() == "alpine":
            domain = "nectarit"
        ticket_prefix = (client_id or "").upper() if client_id else None
        created_on = record.get("created_on")
        if not created_on:  # handles None, "", empty strings
            created_on = int(datetime.utcnow().timestamp() * 1000)
        rows.append(
            (
                client_id,
                record.get("client_name"),
                record.get("location"),
                record.get("location_name"),
                0,  # ticket_start_index
                record.get("status") or "ACTIVE",
                ticket_prefix,
                typeOfClient,
                domain,
                record.get("created_by"),
                created_on
            )
        )

    return rows


def migrate_clients(session: Session, conn: _PGConnection) -> None:
    logger.info("Starting client migration")
    clients = fetch_clients(session)
    rows = map_clients_to_rows(clients)

    if not rows:
        logger.info("No clients found to migrate")
        return
    print("DEBUG row count =", len(rows[0]))
    print("DEBUG row details =", rows[0])
    insert_sql = """
        INSERT INTO public.clients (
            client_id,
            client_name,
            location,
            location_name,
            ticket_start_index,
            status,
            ticket_prefix,
            type,
            colony,
            created_by,
            created_on
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (client_id) DO UPDATE SET
            client_name = EXCLUDED.client_name,
            location = EXCLUDED.location,
            location_name = EXCLUDED.location_name,
            ticket_start_index = EXCLUDED.ticket_start_index,
            status = EXCLUDED.status,
            ticket_prefix = EXCLUDED.ticket_prefix,
            type = EXCLUDED.type,
            colony = EXCLUDED.colony,
            created_by = EXCLUDED.created_by,
            created_on = EXCLUDED.created_on
    """
    batch_insert(conn, insert_sql, rows)
    logger.info("Client migration completed. Migrated %s rows", len(rows))


