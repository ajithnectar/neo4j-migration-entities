import csv
import logging
from pathlib import Path
from typing import Dict, List, Sequence, Tuple, Union

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.postgres_utils import batch_insert, fetch_asset_types
from db.neo4j_utils import run_query

logger = logging.getLogger(__name__)


def export_asset_types_from_neo4j(session: Session, csv_file_path: Union[str, Path] = "AssetTypeToMigrate.csv"):
    """Fetch asset type data from Neo4j and save to CSV file.
    
    Args:
        session: Neo4j session
        csv_file_path: Path to save the CSV file
    """
    logger.info("Fetching asset type data from Neo4j")
    
    cypher = """
        MATCH (n:Template {name:'Asset'})-[:extends*]->(parent:Template)-[:extends]->(child:Template)
        RETURN parent.name AS parent_name, 
               child.name AS child_name, 
               child.templateName AS child_template_name
    """
    
    try:
        records = run_query(session, cypher)
        
        if not records:
            logger.warning("No asset type data found in Neo4j")
            print("No asset type data found in Neo4j")
            return
        
        csv_path = Path(csv_file_path)
        logger.info("Saving %s asset type records to %s", len(records), csv_path)
        
        # Define fieldnames based on the query
        fieldnames = ["parent_name", "child_name", "child_template_name"]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            for record in records:
                # Convert None values to empty strings for CSV
                row = {k: (v if v is not None else '') for k, v in record.items()}
                writer.writerow(row)
        
        logger.info("Successfully saved %s asset type records to %s", len(records), csv_path)
        print(f"✓ Exported {len(records)} asset type records to {csv_path}")
        
    except Exception as e:
        logger.error("Failed to export asset types from Neo4j: %s", str(e), exc_info=True)
        print(f"✗ Failed to export asset types from Neo4j: {e}")
        raise


def read_asset_type_csv(csv_file_path: Union[str, Path] = "AssetTypeToMigrate.csv"):
    # type: (Union[str, Path]) -> List[Dict]
    """Read data from AssetTypeToMigrate.csv file."""
    csv_path = Path(csv_file_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            # Clean up keys (remove BOM if present) and strip quotes/whitespace
            cleaned_row = {}
            for k, v in row.items():
                clean_key = k.lstrip('\ufeff')
                if v is None:
                    cleaned_row[clean_key] = None
                elif isinstance(v, str):
                    # Strip quotes from beginning and end, then strip whitespace
                    v = v.strip().strip('"').strip("'").strip()
                    cleaned_row[clean_key] = v if v else None
                else:
                    cleaned_row[clean_key] = v
            rows.append(cleaned_row)
        return rows


def map_asset_types_to_rows(asset_type_data, start_id=1):
    # type: (List[Dict], int) -> Sequence[Tuple]
    """Map CSV data to database rows for asset_type table.
    
    Maps:
    - child_name -> name
    - parent_name -> parent_name
    - child_template_name -> template_name
    - status -> "ACTIVE" (default)
    - client_id -> "emaar" (hardcoded)
    - id -> auto-generated BIGINT (sequential starting from start_id)
    
    Args:
        asset_type_data: CSV data as list of dicts
        start_id: Starting ID for BIGINT generation (default: 1)
    """
    logger.info("Mapping asset types to database rows")
    rows = []  # type: List[Tuple]
    skipped_count = 0
    current_id = start_id
    
    for record in asset_type_data:
        try:
            name = record.get("child_name")
            parent_name = record.get("parent_name")
            template_name = record.get("child_template_name")
            
            # Skip if name is missing or empty (required field)
            if not name:
                logger.warning("Skipping record with missing child_name: %s", record)
                skipped_count += 1
                continue
            
            # Ensure values are cleaned (strip quotes and whitespace, convert empty to None)
            def clean_value(value):
                if value is None:
                    return None
                if isinstance(value, str):
                    value = value.strip().strip('"').strip("'").strip()
                    return value if value else None
                return value
            
            name = clean_value(name)
            parent_name = clean_value(parent_name)
            template_name = clean_value(template_name)
            
            # Skip if name is empty after cleaning
            if not name:
                logger.warning("Skipping record with empty child_name after cleaning: %s", record)
                skipped_count += 1
                continue
            
            # Generate BIGINT id sequentially
            asset_type_id = current_id
            current_id += 1
            
            # parent_name and template_name can be None
            rows.append(
                (
                    asset_type_id,  # id (BIGINT)
                    name,  # name
                    parent_name,  # parent_name (None if empty)
                    "ACTIVE",  # status (default)
                    template_name,  # template_name (None if empty)
                    "emaar",  # client_id (hardcoded)
                )
            )
        except Exception as e:
            logger.error("Error mapping asset type record %s: %s", record.get("child_name", "unknown"), str(e))
            skipped_count += 1
            continue
    
    if skipped_count > 0:
        logger.warning("Skipped %s invalid asset type records during mapping", skipped_count)
    
    logger.info("Mapped %s asset types to rows", len(rows))
    return rows


def migrate_asset_types(session: Session, conn: _PGConnection, csv_file_path: Union[str, Path] = "AssetTypeToMigrate.csv"):
    """Migrate asset types from CSV to PostgreSQL.
    
    If CSV file doesn't exist, it will be created by fetching data from Neo4j first.
    
    Args:
        session: Neo4j session
        conn: PostgreSQL connection
        csv_file_path: Path to AssetTypeToMigrate.csv file
    """
    logger.info("Starting asset type migration")
    
    # Check if CSV file exists, if not, fetch from Neo4j first
    csv_path = Path(csv_file_path)
    if not csv_path.exists():
        logger.info("CSV file not found at %s, fetching from Neo4j...", csv_path)
        print(f"CSV file not found. Fetching asset type data from Neo4j...")
        export_asset_types_from_neo4j(session, csv_file_path)
    
    try:
        asset_type_data = read_asset_type_csv(csv_file_path)
        
        # Get the maximum existing id from the database to start generating IDs from there
        start_id = 1
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id), 0) FROM public.asset_type")
            result = cur.fetchone()
            if result and result[0]:
                start_id = int(result[0]) + 1
            logger.info("Starting ID generation from %s", start_id)
        
        rows = map_asset_types_to_rows(asset_type_data, start_id)

        if not rows:
            logger.info("No asset types found to migrate")
            print("No asset types to migrate")
            return
        
        logger.info("Preparing to insert %s asset type rows into PostgreSQL", len(rows))
        print(f"Preparing to insert {len(rows)} asset types...")
        
        insert_sql = """
            INSERT INTO public.asset_type (
                id,
                name,
                parent_name,
                status,
                template_name,
                client_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                parent_name = EXCLUDED.parent_name,
                status = EXCLUDED.status,
                template_name = EXCLUDED.template_name,
                client_id = EXCLUDED.client_id
        """

        batch_insert(conn, insert_sql, rows)
        logger.info("Asset type migration completed successfully. Migrated %s rows", len(rows))
        print(f"✓ Migrated {len(rows)} asset types")
    except Exception as e:
        logger.error("Asset type migration failed: %s", str(e), exc_info=True)
        print(f"✗ Asset type migration failed: {e}")
        raise


def fetch_asset_types_from_db(_: Session, conn: _PGConnection, csv_file_path: Union[str, Path] = "assetType.csv"):
    """Fetch all asset types from public.asset_type table and save to CSV.
    
    Equivalent to: SELECT * FROM public.asset_type
    
    Args:
        _: Neo4j session (not used, but required for signature compatibility)
        conn: PostgreSQL connection
        csv_file_path: Path to save the CSV file (default: assetType.csv)
    """
    logger.info("Fetching asset types from public.asset_type")
    try:
        asset_types = fetch_asset_types(conn)
        
        if not asset_types:
            logger.info("No asset types found in database")
            print("No asset types found in database")
            return
        
        logger.info("Successfully fetched %s asset types", len(asset_types))
        print(f"\n✓ Fetched {len(asset_types)} asset types from public.asset_type")
        
        # Save to CSV file
        csv_path = Path(csv_file_path)
        logger.info("Saving asset types to CSV file: %s", csv_path)
        
        # Get column names from the first record
        fieldnames = list(asset_types[0].keys())
        
        # Write to CSV
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            for asset_type in asset_types:
                # Convert None values to empty strings for CSV
                row = {k: (v if v is not None else '') for k, v in asset_type.items()}
                writer.writerow(row)
        
        logger.info("Successfully saved %s asset types to %s", len(asset_types), csv_path)
        print(f"✓ Saved {len(asset_types)} asset types to {csv_path}")
        
        # Display first few records as sample
        if asset_types:
            print("\nSample records (first 5):")
            print("-" * 80)
            for i, asset_type in enumerate(asset_types[:5], 1):
                print(f"{i}. {asset_type}")
            if len(asset_types) > 5:
                print(f"... and {len(asset_types) - 5} more records")
            print("-" * 80)
        
        return asset_types
    except Exception as e:
        logger.error("Failed to fetch asset types: %s", str(e), exc_info=True)
        print(f"✗ Failed to fetch asset types: {e}")
        raise

