from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from db.neo4j_utils import run_query

logger = logging.getLogger(__name__)

# Neo4j query templates (domain will be injected)
COUNT_QUERY_TEMPLATE = """
MATCH (subCommunity:SubCommunity {{domain:'{domain}'}})-[:tags]->(building)
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
MATCH (subCommunity:SubCommunity {{domain:'{domain}'}})-[:tags]->(building)
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


def get_total_count(session: Session, domain: str) -> int:
    """Get the total count of records from Neo4j."""
    logger.info("Fetching total count from Neo4j for domain: %s", domain)
    query = COUNT_QUERY_TEMPLATE.format(domain=domain)
    result = run_query(session, query)
    if not result:
        logger.warning("No count result returned")
        return 0
    
    total_count = result[0].get("total_count", 0)
    logger.info("Total records to export: %s", total_count)
    return int(total_count)


def fetch_batch(session: Session, skip: int, limit: int, domain: str) -> list[dict[str, Any]]:
    """Fetch a batch of records from Neo4j."""
    query = DATA_QUERY_TEMPLATE.format(domain=domain) + f"\nSKIP {skip} LIMIT {limit}"
    logger.debug("Fetching batch: skip=%s, limit=%s, domain=%s", skip, limit, domain)
    records = run_query(session, query)
    logger.info("Fetched %s records (skip=%s, limit=%s)", len(records), skip, limit)
    return records


def convert_value_to_string(value: Any) -> str:
    """Convert a value to a string for CSV output, handling None."""
    if value is None:
        return ""
    return str(value)


def save_to_csv(records: list[dict[str, Any]], file_path: Path) -> None:
    """Save records to a CSV file."""
    logger.info("Saving %s records to %s", len(records), file_path)
    
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        
        for record in records:
            # Ensure all columns are present, using empty string for missing values
            row = {col: convert_value_to_string(record.get(col, "")) for col in CSV_COLUMNS}
            writer.writerow(row)
    
    logger.info("✓ Saved %s records to %s", len(records), file_path)


def export_neo4j_to_csv(
    session: Session,
    _: _PGConnection,
    data_dir: str | Path = "data",
    batch_size: int = 1000,
    domain: str = "ecd",
) -> None:
    """Export data from Neo4j to CSV files in batches.
    
    Args:
        session: Neo4j session
        _: PostgreSQL connection (not used, but required for signature compatibility)
        data_dir: Directory to save CSV files (default: "data")
        batch_size: Number of records per CSV file (default: 1000)
        domain: Domain filter for Neo4j query (default: "ecd")
    """
    logger.info("Starting Neo4j to CSV export with domain: %s, batch_size: %s", domain, batch_size)
    print("\n" + "="*60)
    print("NEO4J TO CSV EXPORT")
    print("="*60)
    print(f"Domain: {domain}")
    print(f"Batch size: {batch_size}")
    
    # Ensure data directory exists
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", data_path.absolute())
    
    # Get total count
    total_count = get_total_count(session, domain)
    if total_count == 0:
        logger.warning("No records found to export for domain: %s", domain)
        print(f"No records found to export for domain: {domain}")
        return
    
    print(f"\nTotal records to export: {total_count}")
    
    # Calculate number of batches
    num_batches = (total_count + batch_size - 1) // batch_size  # Ceiling division
    print(f"Number of files to create: {num_batches}\n")
    
    # Export in batches
    exported_count = 0
    for batch_num in range(1, num_batches + 1):
        skip = (batch_num - 1) * batch_size
        limit = batch_size
        
        print(f"Processing batch {batch_num}/{num_batches} (skip={skip}, limit={limit})...")
        
        try:
            # Fetch batch from Neo4j
            records = fetch_batch(session, skip, limit, domain)
            
            if not records:
                logger.warning("No records returned for batch %s", batch_num)
                print(f"  → No records in batch {batch_num}")
                break
            
            # Save to CSV
            csv_file = data_path / f"data_{batch_num}.csv"
            save_to_csv(records, csv_file)
            
            exported_count += len(records)
            print(f"  ✓ Saved {len(records)} records to {csv_file.name}")
            
        except Exception as e:
            logger.error("Error processing batch %s: %s", batch_num, str(e), exc_info=True)
            print(f"  ✗ Error processing batch {batch_num}: {e}")
            raise
    
    print("\n" + "="*60)
    print(f"✓ Export completed successfully!")
    print(f"  Total records exported: {exported_count}/{total_count}")
    print(f"  Files created: {num_batches}")
    print(f"  Output directory: {data_path.absolute()}")
    print("="*60 + "\n")
    
    logger.info("Neo4j to CSV export completed. Exported %s records to %s files", exported_count, num_batches)

