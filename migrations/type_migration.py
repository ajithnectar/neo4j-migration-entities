import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple, Union

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.postgres_utils import batch_insert
from db.neo4j_utils import run_query

logger = logging.getLogger(__name__)


def export_types_from_neo4j(session: Session, csv_file_path: Union[str, Path] = "typeToMigrate.csv"):
    """Fetch type data from Neo4j and save to CSV file.
    
    Args:
        session: Neo4j session
        csv_file_path: Path to save the CSV file
    """
    logger.info("Fetching type data from Neo4j")
    
    cypher = """
        MATCH (n:Template)-[:extends*]->(parent:Template)-[:extends]->(child:Template)
        RETURN parent.name AS parent_name, 
               child.name AS child_name, 
               child.templateName AS child_template_name, 
               child.displayName AS child_displayName
    """
    
    try:
        records = run_query(session, cypher)
        
        if not records:
            logger.warning("No type data found in Neo4j")
            print("No type data found in Neo4j")
            return
        
        csv_path = Path(csv_file_path)
        logger.info("Saving %s type records to %s", len(records), csv_path)
        
        # Define fieldnames based on the query
        fieldnames = ["parent_name", "child_name", "child_template_name", "child_displayName"]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            for record in records:
                # Convert None values to empty strings for CSV
                row = {k: (v if v is not None else '') for k, v in record.items()}
                writer.writerow(row)
        
        logger.info("Successfully saved %s type records to %s", len(records), csv_path)
        print(f"✓ Exported {len(records)} type records to {csv_path}")
        
    except Exception as e:
        logger.error("Failed to export types from Neo4j: %s", str(e), exc_info=True)
        print(f"✗ Failed to export types from Neo4j: {e}")
        raise


def read_type_csv(csv_file_path: Union[str, Path] = "typeToMigrate.csv"):
    # type: (Union[str, Path]) -> List[Dict]
    """Read data from typeToMigrate.csv file."""
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


def map_types_to_rows(type_data):
    # type: (List[Dict]) -> Sequence[Tuple]
    """Map CSV data to database rows for types table.
    
    Maps:
    - child_name -> name
    - parent_name -> parent_name
    - child_template_name -> template_name
    - status -> "ACTIVE" (default)
    """
    logger.info("Mapping types to database rows")
    rows = []  # type: List[Tuple]
    skipped_count = 0
    
    for record in type_data:
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
            
            # parent_name and template_name can be None
            rows.append(
                (
                    name,  # name
                    parent_name,  # parent_name (None if empty)
                    "ACTIVE",  # status (default)
                    template_name,  # template_name (None if empty)
                )
            )
        except Exception as e:
            logger.error("Error mapping type record %s: %s", record.get("child_name", "unknown"), str(e))
            skipped_count += 1
            continue
    
    if skipped_count > 0:
        logger.warning("Skipped %s invalid type records during mapping", skipped_count)
    
    logger.info("Mapped %s types to rows", len(rows))
    return rows


def migrate_types(session: Session, conn: _PGConnection, csv_file_path: Union[str, Path] = "typeToMigrate.csv"):
    """Migrate types from CSV to PostgreSQL.
    
    If CSV file doesn't exist, it will be created by fetching data from Neo4j first.
    
    Args:
        session: Neo4j session
        conn: PostgreSQL connection
        csv_file_path: Path to typeToMigrate.csv file
    """
    logger.info("Starting type migration")
    
    # Check if CSV file exists, if not, fetch from Neo4j first
    csv_path = Path(csv_file_path)
    if not csv_path.exists():
        logger.info("CSV file not found at %s, fetching from Neo4j...", csv_path)
        print(f"CSV file not found. Fetching type data from Neo4j...")
        export_types_from_neo4j(session, csv_file_path)
    
    try:
        type_data = read_type_csv(csv_file_path)
        rows = map_types_to_rows(type_data)
        if not rows:
            logger.info("No types found to migrate")
            print("No types to migrate")
            return
        
        logger.info("Preparing to insert %s type rows into PostgreSQL", len(rows))
        print(f"Preparing to insert {len(rows)} types...")
        
        insert_sql = """
            INSERT INTO public.types (
                name,
                parent_name,
                status,
                template_name
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                parent_name = EXCLUDED.parent_name,
                status = EXCLUDED.status,
                template_name = EXCLUDED.template_name
        """

        batch_insert(conn, insert_sql, rows)
        logger.info("Type migration completed successfully. Migrated %s rows", len(rows))
        print(f"✓ Migrated {len(rows)} types")
    except Exception as e:
        logger.error("Type migration failed: %s", str(e), exc_info=True)
        print(f"✗ Type migration failed: {e}")
        raise

