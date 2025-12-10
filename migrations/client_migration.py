import logging
from typing import Dict, Iterable, List, Sequence, Tuple

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.neo4j_utils import run_query
from db.postgres_utils import batch_insert
from datetime import datetime
from re import split

logger = logging.getLogger(__name__)


def fetch_clients(session: Session):
    # type: (Session) -> Iterable[Dict]
    cypher = """
        MATCH (root:DefaultTenant {clientId: 'datalkz'})
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

        MATCH (:DefaultTenant {clientId: 'datalkz'})-[:tenant*]->(child)
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


def map_clients_to_rows(clients):
    # type: (Iterable[Dict]) -> Sequence[Tuple]
    rows = []  # type: List[Tuple]
    for record in clients:
        client_id = record.get("client_id")
        domain = record.get("domain")
        typeOfClient = record.get("type_name").replace(" ", "")
        if domain and domain.lower() == "alpine":
            domain = "emaar"
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


def sort_rows_for_foreign_key(rows):
    # type: (List[Tuple]) -> List[Tuple]
    """
    Sort rows to ensure parent rows (referenced by colony foreign key) are inserted first.
    The colony column references client_id in the same table, so we need to insert
    rows where client_id matches a colony value before inserting the child rows.
    
    Uses topological sort to handle multiple levels of dependencies.
    Also validates that colony values reference existing client_ids in the batch.
    If a colony references a non-existent client_id, it will be set to None to avoid FK violations.
    """
    if not rows:
        return rows
    
    # Create a mapping from client_id to row (will be updated with fixed rows)
    client_id_to_row = {row[0]: row for row in rows if row[0]}
    client_ids = set(client_id_to_row.keys())
    
    # Fix invalid colony references and build dependency graph
    dependencies = {}  # Maps client_id to set of client_ids that depend on it
    
    for row in rows:
        client_id = row[0]
        colony = row[8]  # colony is at index 8
        
        # If colony references a client_id that doesn't exist in this batch, set it to None
        if colony and colony not in client_ids:
            logger.warning(
                "Colony '%s' references client_id that doesn't exist in batch. Setting to None for client_id='%s'",
                colony, client_id
            )
            # Create a new tuple with colony set to None
            row = tuple(row[i] if i != 8 else None for i in range(len(row)))
            colony = None
            # Update the row in our mapping with the fixed version
            if client_id:
                client_id_to_row[client_id] = row
        
        # Build dependency graph: if this row's colony references another client_id, 
        # that other client_id is a dependency
        if colony and colony in client_ids and colony != client_id:
            if colony not in dependencies:
                dependencies[colony] = set()
            dependencies[colony].add(client_id)
    
    # Topological sort: Kahn's algorithm
    # Calculate in-degrees (how many dependencies each node has)
    in_degree = {client_id: 0 for client_id in client_ids}
    for parent_id, children in dependencies.items():
        for child_id in children:
            in_degree[child_id] = in_degree.get(child_id, 0) + 1
    
    # Start with nodes that have no dependencies (colony is None or not in batch)
    queue = [client_id for client_id in client_ids if in_degree.get(client_id, 0) == 0]
    sorted_rows = []
    
    while queue:
        # Get a node with no dependencies
        current_id = queue.pop(0)
        if current_id in client_id_to_row:
            sorted_rows.append(client_id_to_row[current_id])
        
        # Remove this node and update in-degrees of its dependents
        if current_id in dependencies:
            for dependent_id in dependencies[current_id]:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)
    
    # Add any remaining rows that weren't part of the dependency graph
    # (shouldn't happen, but just in case)
    added_ids = {row[0] for row in sorted_rows if row[0]}
    for row in rows:
        if row[0] and row[0] not in added_ids:
            sorted_rows.append(row)
    
    return sorted_rows


def migrate_clients(session: Session, conn: _PGConnection):
    # type: (Session, _PGConnection) -> None
    logger.info("Starting client migration")
    clients = fetch_clients(session)
    rows = map_clients_to_rows(clients)

    if not rows:
        logger.info("No clients found to migrate")
        return
    
    # Sort rows to ensure parent rows are inserted before child rows
    # This is necessary because colony has a foreign key constraint to client_id
    rows = sort_rows_for_foreign_key(rows)
    
    print("DEBUG row count =", len(rows))
    if rows:
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


