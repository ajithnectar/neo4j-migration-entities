import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Union

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.neo4j_utils import run_query

logger = logging.getLogger(__name__)

# Neo4j query templates (subcommunity_id will be injected)
COUNT_QUERY_TEMPLATE = """
MATCH (subCommunity:SubCommunity {{identifier:'{subcommunity_id}'}})-[:tags]->(building)
      -[:equips]->(asset)-[:sharePoint]->(point:Point)
OPTIONAL MATCH (asset)<-[:tags]-(spaces)
WHERE subCommunity.status <> 'DELETED'
  AND building.status <> 'DELETED'
  AND asset.status <> 'DELETED'
  AND point.status <> 'DELETED'
  AND (spaces IS NULL OR spaces.status <> 'DELETED')
RETURN count(*) AS total_count
"""

# Neo4j query to fetch data (with SKIP and LIMIT placeholders)
DATA_QUERY_TEMPLATE = """
MATCH (subCommunity:SubCommunity {{identifier:'{subcommunity_id}'}})-[:tags]->(building)
      -[:equips]->(asset)-[:sharePoint]->(point:Point)
OPTIONAL MATCH (asset)<-[:tags]-(spaces)
WHERE subCommunity.status <> 'DELETED'
  AND building.status <> 'DELETED'
  AND asset.status <> 'DELETED'
  AND point.status <> 'DELETED'
  AND (spaces IS NULL OR spaces.status <> 'DELETED')
RETURN
    asset.identifier         AS asset_id,
    asset.displayName        AS asset_name,
    asset.assetCode          AS asset_code,
    asset.location           AS location,
    asset.costOfPurchase     AS cost_of_purchase,
    asset.createdBy          AS created_by,
    asset.createdOn          AS created_on,
    asset.status             AS asset_status,
    asset.domain             AS asset_domain,
    building.ownerClientId   AS community_id,
    building.ownerName       AS community_name,
    subCommunity.identifier  AS sub_community_id,
    subCommunity.name        AS sub_community_name,
    subCommunity.location    AS sub_community_location,
    subCommunity.status      AS sub_community_status,
    subCommunity.domain      AS sub_community_domain,
    subCommunity.createdBy   AS sub_community_created_by,
    subCommunity.createdOn   AS sub_community_created_on,
    labels(subCommunity)[0]  AS sub_community_type,
    labels(asset)[0]         AS asset_type,
    building.identifier      AS building_id,
    building.name            AS building_name,
    building.status          AS building_status,
    building.location        AS building_location,
    building.siteCode        AS building_site_code,
    building.storeOpenTime   AS building_open_time,
    building.storeCloseTime  AS building_close_time,
    building.domain          AS building_domain,
    building.createdBy       AS building_created_by,
    building.createdOn       AS building_created_on,
    labels(building)[0]      AS building_type,
    spaces.identifier        AS spaces_id,
    spaces.name              AS spaces_name,
    spaces.layoutHierarchy   AS spaces_layout,
    spaces.status            AS spaces_status,
    spaces.domain            AS spaces_domain,
    labels(spaces)[0]        AS spaces_type,
    point.identifier         AS data_point_id,
    point.pointName          AS point_name,
    point.displayName        AS point_display_name,
    point.dataType           AS point_data_type,
    point.remoteDataType     AS remote_data_type,
    point.accessType         AS access_type,
    point.status             AS point_status,
    point.unitSymbol         AS point_symbol,
    point.unit               AS point_unit,
    point.expression         AS point_expression,
    point.precedence         AS point_precedence,
    point.type               AS point_type
ORDER BY building_name, asset_name, point_name DESC
"""

# CSV column order (matching the query return order)
CSV_COLUMNS = [
    "asset_id",
    "asset_name",
    "asset_code",
    "location",
    "cost_of_purchase",
    "created_by",
    "created_on",
    "asset_status",
    "asset_domain",
    "community_id",
    "community_name",
    "sub_community_id",
    "sub_community_name",
    "sub_community_location",
    "sub_community_status",
    "sub_community_domain",
    "sub_community_created_by",
    "sub_community_created_on",
    "sub_community_type",
    "asset_type",
    "building_id",
    "building_name",
    "building_status",
    "building_location",
    "building_site_code",
    "building_open_time",
    "building_close_time",
    "building_domain",
    "building_created_by",
    "building_created_on",
    "building_type",
    "spaces_id",
    "spaces_name",
    "spaces_layout",
    "spaces_status",
    "spaces_domain",
    "spaces_type",
    "data_point_id",
    "point_name",
    "point_display_name",
    "point_data_type",
    "remote_data_type",
    "access_type",
    "point_status",
    "point_symbol",
    "point_unit",
    "point_expression",
    "point_precedence",
    "point_type",
]


def read_subcommunity_ids(csv_file_path: Union[str, Path] = "subcommunityids.csv"):
    # type: (Union[str, Path]) -> List[str]
    """Read subcommunity identifiers from CSV file."""
    csv_path = Path(csv_file_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Subcommunity CSV file not found: {csv_path}")
    
    subcommunity_ids = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Handle different possible column names
            subcommunity_id = (
                row.get("subcommunity id") or 
                row.get("subcommunity_id") or 
                row.get("identifier") or
                row.get("id")
            )
            if subcommunity_id:
                subcommunity_id = subcommunity_id.strip()
                if subcommunity_id:  # Skip empty values
                    subcommunity_ids.append(subcommunity_id)
    
    logger.info("Read %s subcommunity IDs from %s", len(subcommunity_ids), csv_path)
    return subcommunity_ids


def get_total_count(session: Session, subcommunity_id: str):
    # type: (Session, str) -> int
    """Get the total count of records from Neo4j for a specific subcommunity."""
    logger.info("Fetching total count from Neo4j for subcommunity: %s", subcommunity_id)
    query = COUNT_QUERY_TEMPLATE.format(subcommunity_id=subcommunity_id)
    result = run_query(session, query)
    if not result:
        logger.warning("No count result returned for subcommunity: %s", subcommunity_id)
        return 0
    
    total_count = result[0].get("total_count", 0)
    logger.info("Total records for subcommunity %s: %s", subcommunity_id, total_count)
    return int(total_count)


def fetch_batch(session: Session, skip: int, limit: int, subcommunity_id: str):
    # type: (Session, int, int, str) -> List[Dict[str, Any]]
    """Fetch a batch of records from Neo4j for a specific subcommunity."""
    query = DATA_QUERY_TEMPLATE.format(subcommunity_id=subcommunity_id) + f"\nSKIP {skip} LIMIT {limit}"
    logger.debug("Fetching batch: skip=%s, limit=%s, subcommunity_id=%s", skip, limit, subcommunity_id)
    records = run_query(session, query)
    logger.info("Fetched %s records (skip=%s, limit=%s) for subcommunity %s", len(records), skip, limit, subcommunity_id)
    return records


def convert_value_to_string(value: Any) -> str:
    """Convert a value to a string for CSV output, handling None."""
    if value is None:
        return ""
    return str(value)


def save_to_csv(records, file_path: Path, write_header: bool = True):
    # type: (List[Dict[str, Any]], Path, bool) -> None
    """Save records to a CSV file. If write_header is False, append without header."""
    logger.info("Saving %s records to %s (header=%s)", len(records), file_path, write_header)
    
    mode = 'w' if write_header else 'a'
    with open(file_path, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        
        for record in records:
            # Ensure all columns are present, using empty string for missing values
            row = {col: convert_value_to_string(record.get(col, "")) for col in CSV_COLUMNS}
            writer.writerow(row)
    
    logger.info("✓ Saved %s records to %s", len(records), file_path)


def export_neo4j_to_csv(
    session: Session,
    _: _PGConnection,
    data_dir: Union[str, Path] = "data",
    batch_size: int = 5000,
    subcommunity_csv: Union[str, Path] = "subcommunityids.csv",
) -> None:
    """Export data from Neo4j to CSV files in batches for each subcommunity.
    
    Args:
        session: Neo4j session
        _: PostgreSQL connection (not used, but required for signature compatibility)
        data_dir: Directory to save CSV files (default: "data")
        batch_size: Number of records per batch (default: 5000)
        subcommunity_csv: Path to CSV file containing subcommunity IDs (default: "subcommunityids.csv")
    """
    logger.info("Starting Neo4j to CSV export with batch_size: %s", batch_size)
    print("\n" + "="*60)
    print("NEO4J TO CSV EXPORT")
    print("="*60)
    print(f"Batch size: {batch_size}")
    print(f"Subcommunity CSV: {subcommunity_csv}")
    
    # Read subcommunity IDs from CSV
    try:
        subcommunity_ids = read_subcommunity_ids(subcommunity_csv)
    except FileNotFoundError as e:
        logger.error("Failed to read subcommunity CSV: %s", str(e))
        print(f"✗ Error: {e}")
        raise
    
    if not subcommunity_ids:
        logger.warning("No subcommunity IDs found in CSV file")
        print("✗ No subcommunity IDs found in CSV file")
        return
    
    print(f"\nFound {len(subcommunity_ids)} subcommunity IDs to process\n")
    
    # Ensure data directory exists
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", data_path.absolute())
    
    # Track totals across all subcommunities
    total_exported = 0
    total_subcommunities = len(subcommunity_ids)
    processed_subcommunities = 0
    global_batch_counter = 0  # Global counter for file naming across all subcommunities
    
    # Process each subcommunity
    for subcommunity_idx, subcommunity_id in enumerate(subcommunity_ids, 1):
        print(f"\n{'='*60}")
        print(f"Processing Subcommunity {subcommunity_idx}/{total_subcommunities}: {subcommunity_id}")
        print(f"{'='*60}")
        
        try:
            # Get total count for this subcommunity
            total_count = get_total_count(session, subcommunity_id)
            
            if total_count == 0:
                logger.warning("No records found for subcommunity: %s", subcommunity_id)
                print(f"  → No records found for this subcommunity")
                continue
            
            print(f"  Total records: {total_count}")
            
            # Calculate number of batches for this subcommunity
            num_batches = (total_count + batch_size - 1) // batch_size
            print(f"  Number of batches: {num_batches}\n")
            
            # Export in batches
            subcommunity_exported = 0
            
            for batch_num in range(1, num_batches + 1):
                skip = (batch_num - 1) * batch_size
                limit = batch_size
                global_batch_counter += 1
                
                print(f"  Batch {batch_num}/{num_batches} (skip={skip}, limit={limit})...", end=" ")
                
                try:
                    # Fetch batch from Neo4j
                    records = fetch_batch(session, skip, limit, subcommunity_id)
                    
                    if not records:
                        logger.warning("No records returned for batch %s of subcommunity %s", batch_num, subcommunity_id)
                        print("No records")
                        break
                    
                    # Save to separate CSV file (data_1.csv, data_2.csv, etc.)
                    csv_file = data_path / f"data_{global_batch_counter}.csv"
                    save_to_csv(records, csv_file, write_header=True)
                    
                    subcommunity_exported += len(records)
                    total_exported += len(records)
                    print(f"✓ {len(records)} records → {csv_file.name}")
                    
                except Exception as e:
                    logger.error("Error processing batch %s for subcommunity %s: %s", batch_num, subcommunity_id, str(e), exc_info=True)
                    print(f"✗ Error: {e}")
                    raise
            
            processed_subcommunities += 1
            print(f"\n  ✓ Subcommunity completed: {subcommunity_exported} records exported")
            
        except Exception as e:
            logger.error("Error processing subcommunity %s: %s", subcommunity_id, str(e), exc_info=True)
            print(f"\n  ✗ Error processing subcommunity: {e}")
            # Continue with next subcommunity instead of failing completely
            continue
    
    print("\n" + "="*60)
    print(f"✓ Export completed successfully!")
    print(f"  Subcommunities processed: {processed_subcommunities}/{total_subcommunities}")
    print(f"  Total records exported: {total_exported}")
    print(f"  Total files created: {global_batch_counter}")
    print(f"  Output directory: {data_path.absolute()}")
    print("="*60 + "\n")
    
    logger.info("Neo4j to CSV export completed. Exported %s records from %s subcommunities to %s files", total_exported, processed_subcommunities, global_batch_counter)

