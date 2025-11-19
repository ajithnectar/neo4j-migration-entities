from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Sequence

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.postgres_utils import batch_insert

logger = logging.getLogger(__name__)


def read_type_csv(csv_file_path: str | Path = "typeToMigrate.csv") -> list[dict]:
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


def map_types_to_rows(type_data: list[dict]) -> Sequence[tuple]:
    """Map CSV data to database rows for types table.
    
    Maps:
    - child_name -> name
    - parent_name -> parent_name
    - child_template_name -> template_name
    - status -> "ACTIVE" (default)
    """
    logger.info("Mapping types to database rows")
    rows: list[tuple] = []
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


def migrate_types(_: Session, conn: _PGConnection, csv_file_path: str | Path = "typeToMigrate.csv") -> None:
    """Migrate types from CSV to PostgreSQL.
    
    Args:
        _: Neo4j session (not used, but required for signature compatibility)
        conn: PostgreSQL connection
        csv_file_path: Path to typeToMigrate.csv file
    """
    logger.info("Starting type migration")
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

