from __future__ import annotations

import csv
import logging
import uuid
from pathlib import Path
from psycopg2.extensions import connection as _PGConnection
from db.postgres_utils import batch_insert
from app_config.utils import convert_epoch_to_timestamp

logger = logging.getLogger(__name__)


def read_csv_data(csv_file_path: str | Path = "buildingdemodata.csv") -> list[dict]:
    """Read data from CSV file."""
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
                cleaned_row[clean_key] = v if v else None
            rows.append(cleaned_row)
        return rows


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
    for record in data:
        space_id = record.get("spaces_id")
        building_id = record.get("building_id")
        layout_raw = record.get("spaces_layout")
        try:
            layout = int(layout_raw)
        except (TypeError, ValueError):
            layout = 0
        if not space_id or space_id in seen:
            continue
        if not building_id:
            logger.warning("Skipping space %s due to missing building_id", space_id)
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
    return rows


def load_asset_type_map(asset_type_csv: str | Path = "assetType.csv") -> dict[str, str]:
    """Load asset type name → id mapping from CSV."""
    csv_path = Path(asset_type_csv)
    if not csv_path.exists():
        logger.warning("Asset type CSV not found at %s", csv_path)
        return {}

    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        mapping: dict[str, str] = {}
        for row in reader:
            name = (row.get("template_name") or "").strip()
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
    """Extract unique data points.
    
    Returns:
        list: List of (csv_data_point_id, row_data) tuples to maintain order
    """
    seen = set()
    rows = []
    
    for record in data:
        csv_data_point_id = record.get("data_point_id")
        if not csv_data_point_id or csv_data_point_id in seen:
            continue
        seen.add(csv_data_point_id)
        
        # Row without id (will be auto-generated)
        row_data = (
            record.get("access_type"),
            record.get("point_data_type"),
            record.get("point_display_name"),
            record.get("point_name"),
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
    batch_insert(conn, sql, rows)
    print(f"✓ Migrated {len(rows)} subcommunities")


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
        asset_space_sql = """
            INSERT INTO public.asset_spaces (
                identifier, spaces_identifier
            ) VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """
        batch_insert(conn, asset_space_sql, asset_space_rows)
        print(f"✓ Migrated {len(asset_space_rows)} asset-space relationships")


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
            
            # Check if record already exists using name and display_name
            cur.execute("""
                SELECT id, point_id FROM public.data_point
                WHERE name = %s AND display_name = %s
                LIMIT 1
            """, (row_data[3], row_data[2]))  # name is index 3, display_name is index 2
            existing_result = cur.fetchone()
            
            if existing_result:
                # Row already exists, use existing IDs
                db_id, existing_point_id = existing_result
                point_id = existing_point_id if existing_point_id else db_id
                inserted_data_points.append((csv_data_point_id, db_id, point_id))
            else:
                # Insert new row with generated UUID
                # row_data structure: (access_type, data_type, display_name, name, remote_data_type, status, symbol, unit)
                cur.execute("""
                    INSERT INTO public.data_point (
                        id, access_type, data_type, display_name, name,
                        point_id, remote_data_type, status, symbol, unit
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (data_point_id, row_data[0], row_data[1], row_data[2], row_data[3], 
                      point_id, row_data[4], row_data[5], row_data[6], row_data[7]))
                inserted_data_points.append((csv_data_point_id, data_point_id, point_id))
    
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


def run_migration(conn: _PGConnection, csv_file_path: str | Path = "buildingdemodata.csv") -> None:
    """Main migration function with menu-driven selection."""
    try:
        print("Reading CSV data...")
        data = read_csv_data(csv_file_path)
        print(f"✓ Loaded {len(data)} records\n")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        print(f"✗ Error: {e}")
        return
    
    choice = show_menu()
    
    if choice == '1':
        print("\n→ Migrating Subcommunities...")
        migrate_subcommunities(conn, data)
        migrate_buildings(conn, data)
        migrate_spaces(conn, data)
        migrate_assets(conn, data)
    elif choice == '2':
        print("\n→ Migrating Buildings...")
        migrate_buildings(conn, data)
        migrate_spaces(conn, data)
        migrate_assets(conn, data)
    elif choice == '3':
        print("\n→ Migrating Spaces...")
        migrate_spaces(conn, data)
        migrate_assets(conn, data)
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

