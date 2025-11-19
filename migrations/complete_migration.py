from __future__ import annotations

import csv
import logging
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
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [{k: v if v else None for k, v in row.items()} for row in reader]


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
    for record in data:
        asset_id = record.get("asset_name")
        if not asset_id or asset_id in seen:
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
        asset_id = record.get("asset_name")
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
        return

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
    batch_insert(conn, sql, rows)
    print(f"✓ Migrated {len(rows)} assets")
    
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


def migrate_points(conn: _PGConnection, data: list[dict]) -> None:
    """Migrate points to PostgreSQL."""
    print("Point migration - Not yet implemented")
    # TODO: Implement point migration logic


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
    elif choice == '2':
        print("\n→ Migrating Buildings...")
        migrate_buildings(conn, data)
        migrate_spaces(conn, data)
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

