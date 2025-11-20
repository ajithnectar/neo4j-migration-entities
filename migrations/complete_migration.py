from __future__ import annotations

import csv
import logging
import re
import uuid
from pathlib import Path
from psycopg2.extensions import connection as _PGConnection
from db.postgres_utils import batch_insert
from app_config.utils import convert_epoch_to_timestamp

logger = logging.getLogger(__name__)


def read_csv_data(csv_file_path: str | Path = "buildingdemodata.csv") -> list[dict]:
    """Read data from a single CSV file."""
    csv_path = Path(csv_file_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            # Clean up keys (remove BOM if present) and normalize asset_id/identifier
            cleaned_row = {}
            for k, v in row.items():
                # Remove BOM from key names
                clean_key = k.lstrip('\ufeff')
                # Map 'identifier' to 'asset_id' for backward compatibility
                if clean_key == 'identifier':
                    clean_key = 'asset_id'
                
                # Strip quotes and whitespace from values, convert empty strings to None
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


def read_multiple_csv_files(csv_file_pattern: str | Path = "data_*.csv", base_dir: str | Path = "data") -> list[dict]:
    """Read data from multiple CSV files matching a pattern.
    
    This function reads ALL matching CSV files first, then returns the combined data.
    Deduplication happens later during migration processing.
    
    Args:
        csv_file_pattern: Pattern to match CSV files (e.g., "data_*.csv" or "data_1.csv")
                         If a base filename is provided, it will automatically look for numbered variants
                         (e.g., data_1.csv, data_2.csv, data_3.csv, etc.)
        base_dir: Base directory to search for CSV files (default: "data")
    
    Returns:
        Combined list of all records from all matching CSV files (before deduplication)
    """
    base_path = Path(base_dir)
    pattern = str(csv_file_pattern)
    
    # If pattern doesn't contain wildcards, check for numbered variants
    if '*' not in pattern and '?' not in pattern:
        csv_path = base_path / pattern
        base_name = csv_path.stem  # e.g., "data_1" -> "data_"
        extension = csv_path.suffix  # e.g., ".csv"
        
        # Extract base prefix (e.g., "data_" from "data_1")
        # Try to find the pattern by removing trailing digits
        base_prefix = re.sub(r'\d+$', '', base_name)  # Remove trailing numbers
        if not base_prefix:
            base_prefix = base_name
        
        # Look for all files matching the pattern (e.g., data_1.csv, data_2.csv, etc.)
        pattern = f"{base_prefix}*{extension}"
    
    # Find all matching CSV files
    csv_files = list(base_path.glob(pattern))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found matching pattern: {pattern} in {base_path}")
    
    # Sort files naturally (handles numbered files correctly: data_1, data_2, data_10 instead of data_1, data_10, data_2)
    def natural_sort_key(path: Path) -> tuple:
        """Natural sort key that handles numbers in filenames."""
        parts = re.split(r'(\d+)', str(path.name))
        return tuple(int(part) if part.isdigit() else part.lower() for part in parts)
    
    csv_files = sorted(csv_files, key=natural_sort_key)
    
    all_rows = []
    total_files = len(csv_files)
    
    print(f"Found {total_files} CSV file(s) to process:")
    for idx, csv_file in enumerate(csv_files, 1):
        print(f"  {idx}. {csv_file.name}")
    
    # STEP 1: Read ALL files first (don't process/insert yet)
    print(f"\n{'='*60}")
    print("STEP 1: Reading all CSV files...")
    print(f"{'='*60}")
    for idx, csv_file in enumerate(csv_files, 1):
        print(f"\nReading file {idx}/{total_files}: {csv_file.name}...")
        try:
            rows = read_csv_data(csv_file)
            all_rows.extend(rows)
            print(f"  ✓ Loaded {len(rows)} records from {csv_file.name}")
        except Exception as e:
            logger.error(f"Error reading {csv_file.name}: {e}")
            print(f"  ✗ Error reading {csv_file.name}: {e}")
            raise
    
    print(f"\n{'='*60}")
    print(f"✓ All files read. Total records loaded: {len(all_rows)} from {total_files} file(s)")
    print(f"{'='*60}\n")
    
    return all_rows


def get_subcommunity_rows(data: list[dict]) -> list[tuple]:
    """Extract unique subcommunities."""
    seen = set()
    rows = []
    for record in data:
        sub_id = record.get("sub_community_id")
        if sub_id and sub_id not in seen:
            seen.add(sub_id)
            rows.append((
                sub_id,
                record.get("sub_community_location"),
                record.get("sub_community_name"),
                record.get("sub_community_status") or "ACTIVE",
                record.get("community_id"),
                record.get("sub_community_domain"),
                record.get("sub_community_type")
            ))
    return rows


def get_building_rows(data: list[dict]) -> list[tuple]:
    """Extract unique buildings."""
    seen = set()
    rows = []
    for record in data:
        bldg_id = record.get("building_id")
        if bldg_id and bldg_id not in seen:
            seen.add(bldg_id)
            rows.append((
                bldg_id,
                record.get("building_name"),
                record.get("building_status") or "ACTIVE",
                record.get("building_location"),
                record.get("building_site_code"),
                convert_epoch_to_timestamp(record.get("building_open_time")) if record.get("building_open_time") else None,
                convert_epoch_to_timestamp(record.get("building_close_time")) if record.get("building_close_time") else None,
                record.get("building_domain"),
                record.get("building_type"),
                record.get("building_created_by"),
                convert_epoch_to_timestamp(record.get("building_created_on")) if record.get("building_created_on") else None,
                record.get("sub_community_id")
            ))
    return rows


def get_space_rows(data: list[dict]) -> list[tuple]:
    """Extract unique spaces."""
    seen = set()
    rows = []
    skipped_count = 0
    for record in data:
        space_id = record.get("spaces_id")
        if not space_id:
            skipped_count += 1
            continue
        if space_id == "null":
            continue
        
        if space_id in seen:
            continue
        
        building_id = record.get("building_id")
        layout_raw = record.get("spaces_layout")
        try:
            layout = int(layout_raw)
        except (TypeError, ValueError):
            layout = 0
        
        if not building_id:
            logger.warning("Skipping space %s due to missing building_id", space_id)
            skipped_count += 1
            continue
        
        seen.add(space_id)
        rows.append(
            (
                space_id,
                layout,
                record.get("spaces_name"),
                record.get("spaces_status") or "ACTIVE",
                building_id,
                record.get("spaces_domain"),
                record.get("spaces_type"),
            )
        )
    
    if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} records with missing or empty space_id")
    
    return rows


def load_asset_type_map(asset_type_csv: str | Path = "assetType.csv") -> dict[str, str]:
    """Load asset type name → id mapping from CSV.
    
    Uses the 'name' column as the key since that's what's used in the data records.
    """
    csv_path = Path(asset_type_csv)
    if not csv_path.exists():
        logger.warning("Asset type CSV not found at %s", csv_path)
        return {}

    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        mapping: dict[str, str] = {}
        for row in reader:
            name = (row.get("name") or "").strip()
            type_id = row.get("id")
            if name and type_id:
                mapping[name] = type_id
    return mapping


def _safe_float(value: str | None) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_asset_rows(data: list[dict], asset_type_map: dict[str, str]) -> list[tuple]:
    """Extract unique assets."""
    seen = set()
    rows = []
    skipped_count = 0
    for record in data:
        asset_id = record.get("asset_id")
        if not asset_id:
            skipped_count += 1
            continue
        if asset_id in seen:
            continue
        seen.add(asset_id)

        asset_type_name = (record.get("asset_type") or "").strip()
        asset_type_id = asset_type_map.get(asset_type_name)
        if asset_type_name and not asset_type_id:
            logger.warning("Asset type '%s' not found in lookup table", asset_type_name)

        rows.append(
            (
                asset_id,
                record.get("asset_code"),
                _safe_float(record.get("cost_of_purchase")),
                record.get("created_by"),
                convert_epoch_to_timestamp(record.get("created_on")) if record.get("created_on") else None,
                record.get("asset_name"),
                record.get("asset_make"),
                record.get("asset_model"),
                record.get("asset_status") or "ACTIVE",
                record.get("asset_updated_by"),
                convert_epoch_to_timestamp(record.get("asset_updated_on")) if record.get("asset_updated_on") else None,
                record.get("analytics_profile_id"),
                record.get("community_id"),
                record.get("asset_domain"),
                record.get("asset_settings_id"),
                record.get("building_id"),
                record.get("sub_community_id"),
                record.get("active_contract"),
                asset_type_id,
            )
        )
    return rows


def get_asset_space_rows(data: list[dict]) -> list[tuple]:
    """Extract unique asset-space relationships."""
    seen = set()
    rows = []
    for record in data:
        asset_id = record.get("asset_id")  # Use CSV asset_id (UUID) instead of asset_name
        space_id = record.get("spaces_id")
        
        # Skip if asset_id or space_id is missing, None, or empty/whitespace
        if not asset_id or not space_id:
            continue
        asset_id = str(asset_id).strip()
        space_id = str(space_id).strip()
        if not asset_id or not space_id:
            continue
        
        # Create unique key for asset-space pair
        pair_key = (asset_id, space_id)
        if pair_key in seen:
            continue
        
        seen.add(pair_key)
        rows.append((asset_id, space_id))
    
    return rows


def get_data_point_rows(data: list[dict]) -> list[tuple[str, tuple]]:
    """Extract unique data points based on point_name (name field).
    
    Returns:
        list: List of (csv_data_point_id, row_data) tuples to maintain order
    """
    seen = set()  # Track seen point_name values
    rows = []
    
    for record in data:
        csv_data_point_id = record.get("data_point_id")
        point_name = record.get("point_name")
        
        # Skip if point_name is missing
        if not point_name:
            continue
        
        # Skip if we've already seen this point_name (deduplicate by point_name)
        if point_name in seen:
            continue
        
        seen.add(point_name)
        
        # Row without id (will be auto-generated)
        row_data = (
            record.get("access_type"),
            record.get("point_data_type"),
            record.get("point_display_name"),
            point_name,  # This is the unique field
            record.get("remote_data_type"),
            record.get("point_status") or "ACTIVE",
            record.get("point_symbol"),
            record.get("point_unit"),
        )
        
        rows.append((csv_data_point_id, row_data))
    
    return rows


def get_asset_point_rows(data: list[dict], csv_to_point_id_map: dict[str, str]) -> list[tuple]:
    """Extract unique asset-point relationships.
    
    Args:
        data: CSV data
        csv_to_point_id_map: Mapping from CSV data_point_id to auto-generated point_id
    """
    seen = set()
    rows = []
    for record in data:
        asset_id = record.get("asset_id")
        csv_data_point_id = record.get("data_point_id")
        
        if not asset_id or not csv_data_point_id:
            continue
        
        # Get the auto-generated point_id from the mapping
        point_id = csv_to_point_id_map.get(csv_data_point_id)
        if not point_id:
            continue  # Skip if data_point wasn't created
        
        # Create unique key for asset-point pair
        pair_key = (asset_id, point_id)
        if pair_key in seen:
            continue
        
        seen.add(pair_key)
        
        precedence_raw = record.get("point_precedence")
        precedence = None
        if precedence_raw and precedence_raw.strip():
            try:
                precedence = int(precedence_raw)
            except (TypeError, ValueError):
                precedence = None
        
        # Row without id (will be auto-generated)
        rows.append((
            record.get("point_expression"),
            precedence,
            record.get("point_status") or "ACTIVE",
            record.get("point_symbol"),
            record.get("point_unit"),
            asset_id,
            point_id,
        ))
    return rows


def get_asset_type_point_rows(data: list[dict], asset_type_map: dict[str, str], csv_to_point_id_map: dict[str, str]) -> list[tuple]:
    """Extract unique asset-type-point relationships.
    
    Args:
        data: CSV data
        asset_type_map: Mapping from asset_type name to asset_type_id
        csv_to_point_id_map: Mapping from CSV data_point_id to auto-generated point_id
    """
    seen = set()
    rows = []
    for record in data:
        asset_type_name = (record.get("asset_type") or "").strip()
        asset_type_id = asset_type_map.get(asset_type_name)
        csv_data_point_id = record.get("data_point_id")
        
        if not asset_type_id or not csv_data_point_id:
            continue
        
        # Get the auto-generated point_id from the mapping
        point_id = csv_to_point_id_map.get(csv_data_point_id)
        if not point_id:
            continue  # Skip if data_point wasn't created
        
        # Create unique key for asset-type-point pair
        pair_key = (asset_type_id, point_id)
        if pair_key in seen:
            continue
        
        seen.add(pair_key)
        
        precedence_raw = record.get("point_precedence")
        precedence = None
        if precedence_raw and precedence_raw.strip():
            try:
                precedence = int(precedence_raw)
            except (TypeError, ValueError):
                precedence = None
        
        # Row without id (will be auto-generated)
        rows.append((
            record.get("point_expression"),
            precedence,
            record.get("point_status") or "ACTIVE",
            record.get("point_symbol"),
            record.get("point_unit"),
            asset_type_id,
            point_id,  # Use auto-generated point_id
        ))
    return rows


def migrate_subcommunities(conn: _PGConnection, data: list[dict]) -> None:
    """Migrate subcommunities to PostgreSQL."""
    rows = get_subcommunity_rows(data)
    if not rows:
        print("No subcommunities to migrate")
        return
    
    logger.info(f"Found {len(rows)} subcommunities to process")
    print(f"Processing {len(rows)} subcommunities...")
    
    # Filter out subcommunities where community_id doesn't exist in clients table
    # First, fetch all valid client_ids in a single query (much faster than individual queries)
    valid_client_id_map = {}  # Maps lowercase input to actual client_id from DB
    skipped_community_count = 0
    
    logger.info("Validating subcommunity community_ids against clients table...")
    with conn.cursor() as cur:
        # Get unique community_ids from the rows (skip null/empty)
        unique_community_ids = list(set(
            row[4] for row in rows if row[4]  # community_id is at index 4
        ))
        
        if unique_community_ids:
            # Fetch all valid client_ids in a single query (case-insensitive)
            # Use ANY with array for case-insensitive comparison
            cur.execute("""
                SELECT DISTINCT client_id FROM public.clients
                WHERE LOWER(client_id) = ANY(
                    SELECT LOWER(unnest(%s::text[]))
                )
            """, (unique_community_ids,))
            
            # Create a map: lowercase -> actual client_id
            for (client_id,) in cur.fetchall():
                valid_client_id_map[client_id.lower()] = client_id
    
    # Now filter rows in memory using the valid client_ids map
    valid_rows = []
    for row in rows:
        community_id = row[4]  # community_id is at index 4 in the tuple
        
        # Skip if community_id is null or empty
        if not community_id:
            skipped_community_count += 1
            logger.warning("Skipping subcommunity %s due to missing community_id", row[0])
            continue
        
        # Check if community_id exists in the valid client_ids map (case-insensitive)
        actual_client_id = valid_client_id_map.get(community_id.lower())
        if not actual_client_id:
            skipped_community_count += 1
            logger.warning("Skipping subcommunity %s: community_id '%s' does not exist in clients table", row[0], community_id)
            continue
        
        # Update the row with the correct client_id (preserving case from DB)
        row_list = list(row)
        row_list[4] = actual_client_id
        valid_rows.append(tuple(row_list))
    
    if not valid_rows:
        print("No valid subcommunities to migrate (all subcommunities have invalid or missing community_id)")
        if skipped_community_count > 0:
            print(f"Skipped {skipped_community_count} subcommunities due to invalid/missing community_id")
        return
    
    if skipped_community_count > 0:
        logger.info(f"Skipped {skipped_community_count} subcommunities due to invalid/missing community_id")
    
    logger.info(f"Inserting {len(valid_rows)} valid subcommunities into PostgreSQL...")
    sql = """
        INSERT INTO public.sub_community (
            identifier, geo_location, name, status, community_id, domain, type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (identifier) DO UPDATE SET
            geo_location = EXCLUDED.geo_location,
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            community_id = EXCLUDED.community_id,
            domain = EXCLUDED.domain,
            type = EXCLUDED.type
    """
    batch_insert(conn, sql, valid_rows)
    logger.info(f"Successfully inserted {len(valid_rows)} subcommunities")
    print(f"✓ Migrated {len(valid_rows)} subcommunities")


def migrate_buildings(conn: _PGConnection, data: list[dict]) -> None:
    """Migrate buildings to PostgreSQL."""
    rows = get_building_rows(data)
    if not rows:
        print("No buildings to migrate")
        return
    
    sql = """
        INSERT INTO public.building (
            identifier, name, status, geo_location, site_code,
            store_open_time, store_close_time, domain, type,
            created_by, created_on, sub_community_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (identifier) DO UPDATE SET
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            geo_location = EXCLUDED.geo_location,
            site_code = EXCLUDED.site_code,
            store_open_time = EXCLUDED.store_open_time,
            store_close_time = EXCLUDED.store_close_time,
            domain = EXCLUDED.domain,
            type = EXCLUDED.type,
            created_by = EXCLUDED.created_by,
            created_on = EXCLUDED.created_on,
            sub_community_id = EXCLUDED.sub_community_id
    """
    batch_insert(conn, sql, rows)
    print(f"✓ Migrated {len(rows)} buildings")


def migrate_spaces(conn: _PGConnection, data: list[dict]) -> None:
    """Migrate spaces to PostgreSQL."""
    rows = get_space_rows(data)
    print(rows) 
    if not rows:
        print("No spaces to migrate")
        return

    sql = """
        INSERT INTO public.space (
            identifier, layout_hierarchy, name, status,
            building_identifier, domain, type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (identifier) DO UPDATE SET
            layout_hierarchy = EXCLUDED.layout_hierarchy,
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            building_identifier = EXCLUDED.building_identifier,
            domain = EXCLUDED.domain,
            type = EXCLUDED.type
    """
    batch_insert(conn, sql, rows)
    print(f"✓ Migrated {len(rows)} spaces")


def migrate_assets(conn: _PGConnection, data: list[dict], asset_type_csv: str | Path = "assetType.csv") -> None:
    """Migrate assets to PostgreSQL."""
    asset_type_map = load_asset_type_map(asset_type_csv)
    rows = get_asset_rows(data, asset_type_map)
    if not rows:
        print("No assets to migrate")
        logger.warning("No asset rows found. Check if asset_id values exist in CSV.")
        return

    logger.info(f"Preparing to insert {len(rows)} assets")
    if rows:
        logger.debug(f"First asset identifier: {rows[0][0]}")

    sql = """
        INSERT INTO public.assets (
            identifier, asset_code, cost_of_purchase, created_by, created_on,
            display_name, make, model, status, updated_by, updated_on,
            analytics_profile_id, client_id, colony, asset_settings_id, site_id,
            sub_community_id, active_contract, type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (identifier) DO UPDATE SET
            asset_code = EXCLUDED.asset_code,
            cost_of_purchase = EXCLUDED.cost_of_purchase,
            created_by = EXCLUDED.created_by,
            created_on = EXCLUDED.created_on,
            display_name = EXCLUDED.display_name,
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            status = EXCLUDED.status,
            updated_by = EXCLUDED.updated_by,
            updated_on = EXCLUDED.updated_on,
            analytics_profile_id = EXCLUDED.analytics_profile_id,
            client_id = EXCLUDED.client_id,
            colony = EXCLUDED.colony,
            asset_settings_id = EXCLUDED.asset_settings_id,
            site_id = EXCLUDED.site_id,
            sub_community_id = EXCLUDED.sub_community_id,
            active_contract = EXCLUDED.active_contract,
            type = EXCLUDED.type
    """
    try:
        batch_insert(conn, sql, rows)
        print(f"✓ Migrated {len(rows)} assets")
    except Exception as e:
        logger.error(f"Error inserting assets: {e}")
        raise
    
    # Migrate asset-space relationships
    asset_space_rows = get_asset_space_rows(data)
    if asset_space_rows:
        # Filter out spaces that don't exist in the database
        # First, fetch all valid space identifiers in a single query (much faster than individual queries)
        valid_space_ids = set()
        with conn.cursor() as cur:
            # Get unique space IDs from the asset-space rows
            unique_space_ids = list(set(space_id for _, space_id in asset_space_rows))
            if unique_space_ids:
                # Use a single query with IN clause to check all spaces at once
                placeholders = ','.join(['%s'] * len(unique_space_ids))
                cur.execute(f"""
                    SELECT identifier FROM public.space
                    WHERE identifier IN ({placeholders})
                """, unique_space_ids)
                valid_space_ids = {row[0] for row in cur.fetchall()}
        
        # Filter asset-space rows in memory using the valid space IDs set
        valid_asset_space_rows = [
            (asset_id, space_id) for asset_id, space_id in asset_space_rows
            if space_id in valid_space_ids
        ]
        
        # Log skipped relationships
        skipped_count = len(asset_space_rows) - len(valid_asset_space_rows)
        if skipped_count > 0:
            logger.warning("Skipping %s asset-space relationships: spaces do not exist", skipped_count)
        
        if valid_asset_space_rows:
            asset_space_sql = """
                INSERT INTO public.asset_spaces (
                    identifier, spaces_identifier
                ) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """
            batch_insert(conn, asset_space_sql, valid_asset_space_rows)
            print(f"✓ Migrated {len(valid_asset_space_rows)} asset-space relationships")
        else:
            print("No valid asset-space relationships to migrate (all spaces missing)")
    else:
        print("No asset-space relationships to migrate")


def migrate_points(conn: _PGConnection, data: list[dict], asset_type_csv: str | Path = "assetType.csv") -> None:
    """Migrate points, asset-points, and asset-type-points to PostgreSQL.
    
    Order of operations:
    1. First create data_point entries (auto-generate id and point_id)
    2. Query back to get mapping from CSV data_point_id to auto-generated point_id
    3. Use the point_id to create asset_point relationships (auto-generate id)
    4. Use the point_id to create asset_type_point relationships (auto-generate id)
    """
    # Step 1: Get data point rows (with csv_id for mapping)
    data_point_rows = get_data_point_rows(data)
    if not data_point_rows:
        print("No data points to migrate")
        return
    
    # Insert data_points with auto-generated UUID
    # Insert one by one with generated UUIDs
    inserted_data_points = []  # Store (csv_id, db_id, point_id) tuples
    with conn.cursor() as cur:
        for csv_data_point_id, row_data in data_point_rows:
            # Generate UUID for id and point_id (point_id will be same as id)
            data_point_id = str(uuid.uuid4())
            point_id = data_point_id  # point_id should be the same as id
            
            # Check if record already exists using name (point_name) - this should be unique
            point_name = row_data[3]  # name is index 3
            cur.execute("""
                SELECT id, point_id FROM public.data_point
                WHERE name = %s
                LIMIT 1
            """, (point_name,))
            existing_result = cur.fetchone()
            
            if existing_result:
                # Row already exists, use existing IDs
                db_id, existing_point_id = existing_result
                point_id = existing_point_id if existing_point_id else db_id
                inserted_data_points.append((csv_data_point_id, db_id, point_id))
                logger.debug("Data point with name '%s' already exists, using existing id: %s", point_name, db_id)
            else:
                # Insert new row with generated UUID
                # row_data structure: (access_type, data_type, display_name, name, remote_data_type, status, symbol, unit)
                # Cast UUID strings to UUID type for PostgreSQL
                # No ON CONFLICT needed since we already check for existence above
                cur.execute("""
                    INSERT INTO public.data_point (
                        id, access_type, data_type, display_name, name,
                        point_id, remote_data_type, status, symbol, unit
                    ) VALUES (%s::uuid, %s, %s, %s, %s, %s::uuid, %s, %s, %s, %s)
                    RETURNING id, point_id
                """, (data_point_id, row_data[0], row_data[1], row_data[2], row_data[3], 
                      point_id, row_data[4], row_data[5], row_data[6], row_data[7]))
                
                result = cur.fetchone()
                if result:
                    db_id, returned_point_id = result
                    point_id = returned_point_id if returned_point_id else db_id
                    inserted_data_points.append((csv_data_point_id, db_id, point_id))
                else:
                    # If RETURNING didn't work, query again
                    cur.execute("""
                        SELECT id, point_id FROM public.data_point
                        WHERE name = %s
                        LIMIT 1
                    """, (point_name,))
                    result = cur.fetchone()
                    if result:
                        db_id, returned_point_id = result
                        point_id = returned_point_id if returned_point_id else db_id
                        inserted_data_points.append((csv_data_point_id, db_id, point_id))
    
    conn.commit()
    print(f"✓ Migrated {len(inserted_data_points)} data points")
    
    # Create mapping from CSV data_point_id to auto-generated point_id (UUID)
    csv_to_point_id_map: dict[str, str] = {
        csv_id: point_id for csv_id, _, point_id in inserted_data_points
    }
    
    if not csv_to_point_id_map:
        logger.error("No data_points were found after insertion. Cannot proceed with asset_point and asset_type_point.")
        return
    
    print(f"✓ Created mapping for {len(csv_to_point_id_map)} data points")
    
    # Step 4: Migrate asset-point relationships (using auto-generated UUID for id)
    asset_point_rows = get_asset_point_rows(data, csv_to_point_id_map)
    if asset_point_rows:
        inserted_count = 0
        with conn.cursor() as cur:
            for row_data in asset_point_rows:
                # Generate UUID for id
                asset_point_id = str(uuid.uuid4())
                # Check if relationship already exists
                cur.execute("""
                    SELECT id FROM public.asset_point
                    WHERE asset_id = %s AND data_point_id = %s
                    LIMIT 1
                """, (row_data[5], row_data[6]))  # asset_id is index 5, data_point_id is index 6
                existing = cur.fetchone()
                
                if not existing:
                    # Insert new row with generated UUID
                    cur.execute("""
                        INSERT INTO public.asset_point (
                            id, expression, precedence, status, symbol, unit, asset_id, data_point_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (asset_point_id, *row_data))
                    inserted_count += 1
        conn.commit()
        print(f"✓ Migrated {inserted_count} asset-point relationships")
    else:
        print("No asset-point relationships to migrate")
    
    # Step 5: Migrate asset-type-point relationships (using auto-generated UUID for id)
    asset_type_map = load_asset_type_map(asset_type_csv)
    asset_type_point_rows = get_asset_type_point_rows(data, asset_type_map, csv_to_point_id_map)
    if asset_type_point_rows:
        inserted_count = 0
        with conn.cursor() as cur:
            for row_data in asset_type_point_rows:
                # Generate UUID for id
                asset_type_point_id = str(uuid.uuid4())
                # Check if relationship already exists
                cur.execute("""
                    SELECT id FROM public.asset_type_point
                    WHERE asset_type_id = %s AND data_point_id = %s
                    LIMIT 1
                """, (row_data[5], row_data[6]))  # asset_type_id is index 5, data_point_id is index 6
                existing = cur.fetchone()
                
                if not existing:
                    # Insert new row with generated UUID
                    cur.execute("""
                        INSERT INTO public.asset_type_point (
                            id, expression, precedence, status, symbol, unit, asset_type_id, data_point_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (asset_type_point_id, *row_data))
                    inserted_count += 1
        conn.commit()
        print(f"✓ Migrated {inserted_count} asset-type-point relationships")
    else:
        print("No asset-type-point relationships to migrate")


def show_menu() -> str:
    """Display migration menu and get user choice."""
    print("\n" + "="*50)
    print("DATA MIGRATION MENU")
    print("="*50)
    print("1. Start from Subcommunity")
    print("2. Start from Building")
    print("3. Start from Space")
    print("4. Start from Asset")
    print("5. Start from Point")
    print("6. Exit")
    print("="*50)
    
    while True:
        choice = input("\nEnter your choice (1-6): ").strip()
        if choice in ['1', '2', '3', '4', '5', '6']:
            return choice
        print("Invalid choice. Please enter a number between 1 and 6.")


def run_migration(conn: _PGConnection, csv_file_path: str | Path = "data_*.csv", data_dir: str | Path = "data") -> None:
    """Main migration function with menu-driven selection.
    
    This function:
    1. Reads ALL CSV files from the data folder first
    2. Combines all data from all files
    3. Then processes unique data during migration (deduplication happens in get_*_rows functions)
    
    Args:
        conn: PostgreSQL connection
        csv_file_path: CSV file path or pattern (e.g., "data_*.csv" or "data_1.csv")
                      Default: "data_*.csv" - will find all data_*.csv files in the data folder
        data_dir: Directory containing CSV files (default: "data")
    """
    # Print database name
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        db_name = cur.fetchone()[0]
        print(f"Database: {db_name}\n")
    
    try:
        print("="*60)
        print("CSV DATA LOADING")
        print("="*60)
        # Automatically detect and process multiple CSV files
        # If pattern contains wildcards, use it directly; otherwise look for numbered variants
        csv_path = Path(csv_file_path)
        csv_path_str = str(csv_file_path)
        
        if '*' in csv_path_str or '?' in csv_path_str:
            # Pattern provided, use it directly
            base_dir = csv_path.parent if csv_path.parent != Path('.') else Path(data_dir)
            pattern = csv_path.name if csv_path.parent == Path('.') else csv_path_str
            data = read_multiple_csv_files(pattern, base_dir)
        else:
            # Single file name - automatically look for numbered variants
            base_dir = csv_path.parent if csv_path.parent != Path('.') else Path(data_dir)
            pattern = csv_path.name
            data = read_multiple_csv_files(pattern, base_dir)
        
        print(f"\n{'='*60}")
        print(f"STEP 2: Processing unique data from {len(data)} total records...")
        print(f"{'='*60}\n")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        print(f"✗ Error: {e}")
        return
    
    choice = show_menu()
    
    if choice == '1':
        print("\n→ Migrating Subcommunities...")
        migrate_subcommunities(conn, data)
    elif choice == '2':
        print("\n→ Migrating Buildings...")
        migrate_buildings(conn, data)
    elif choice == '3':
        print("\n→ Migrating Spaces...")
        migrate_spaces(conn, data)
    elif choice == '4':
        print("\n→ Migrating Assets...")
        migrate_assets(conn, data)
    elif choice == '5':
        print("\n→ Migrating Points...")
        migrate_points(conn, data)
    elif choice == '6':
        print("\n✓ Exiting migration")
        return
    
    print("\n✓ Migration completed")

