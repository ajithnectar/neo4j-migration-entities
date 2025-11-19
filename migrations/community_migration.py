from __future__ import annotations

import logging
from typing import Iterable, Sequence

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.neo4j_utils import run_query
from db.postgres_utils import batch_insert

logger = logging.getLogger(__name__)


def fetch_communities(session: Session) -> Iterable[dict]:
    logger.info("Fetching communities from Neo4j")
    cypher = """
    MATCH (n:Community {domain: 'bdclone'})
    RETURN
        n.clientId AS client_id,
        n.clientName AS client_name,
        n.location AS location,
        n.locationName AS location_name,
        coalesce(n.status, 'ACTIVE') AS status,
        n.domain AS domain,
        n.typeName AS type_name,
        n.createdBy AS created_by,
        n.createdOn AS created_on,
        n.updatedBy AS updated_by,
        n.updatedOn AS updated_on,
        n.identifier AS identifier
    ORDER BY client_id
    """
    try:
        communities = run_query(session, cypher)
        logger.info("Fetched %s communities from Neo4j", len(communities) if isinstance(communities, list) else "unknown")
        return communities
    except Exception as e:
        logger.error("Error fetching communities from Neo4j: %s", str(e))
        raise


def map_communities_to_rows(communities: Iterable[dict]) -> Sequence[tuple]:
    logger.info("Mapping communities to database rows")
    rows: list[tuple] = []
    skipped_count = 0
    
    for record in communities:
        try:
            client_id = record.get("client_id")
            domain = record.get("domain")
            ticket_prefix = (client_id or "").upper() if client_id else None
            
            # Handle type_name safely - it might be None
            type_name = record.get("type_name")
            if type_name is None:
                logger.warning("type_name is None for client_id: %s, using empty string", client_id)
                type_name_processed = ""
            else:
                type_name_processed = type_name.replace(" ", "")
            
            # Warn if critical fields are missing
            if not client_id:
                logger.warning("Skipping record with missing client_id: %s", record)
                skipped_count += 1
                continue

            rows.append(
                (
                    client_id,
                    record.get("client_name"),
                    record.get("location"),
                    record.get("location_name"),
                    0,
                    record.get("status") or "ACTIVE",
                    ticket_prefix,
                    type_name_processed,
                    domain,
                    record.get("created_by"),
                    record.get("created_on"),
                    record.get("identifier")
                )
            )
        except Exception as e:
            logger.error("Error mapping community record %s: %s", record.get("client_id", "unknown"), str(e))
            skipped_count += 1
            continue

    if skipped_count > 0:
        logger.warning("Skipped %s invalid community records during mapping", skipped_count)
    
    logger.info("Mapped %s communities to rows", len(rows))
    return rows


def migrate_communities(session: Session, conn: _PGConnection) -> None:
    logger.info("Starting community migration")
    try:
        communities = fetch_communities(session)
        rows = map_communities_to_rows(communities)

        if not rows:
            logger.info("No communities found to migrate")
            return
        
        logger.info("Preparing to insert %s community rows into PostgreSQL", len(rows))
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
                created_on,
                reference_number
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                created_on = EXCLUDED.created_on,
                reference_number = EXCLUDED.reference_number
        """

        batch_insert(conn, insert_sql, rows)
        logger.info("Community migration completed successfully. Migrated %s rows", len(rows))
    except Exception as e:
        logger.error("Community migration failed: %s", str(e), exc_info=True)
        raise


