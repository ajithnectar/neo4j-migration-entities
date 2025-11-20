from __future__ import annotations

import argparse
import logging
from typing import Callable, Iterable

from neo4j import Session
from psycopg2.extensions import connection as _PGConnection

from app_config.settings import EnvName, get_config
from db.neo4j_utils import create_neo4j_driver, neo4j_session
from db.postgres_utils import pg_connection
from migrations.community_migration import migrate_communities
from migrations.client_migration import migrate_clients
from migrations.complete_migration import run_migration
from migrations.type_migration import migrate_types
from migrations.asset_type_migration import migrate_asset_types, fetch_asset_types_from_db
from migrations.neo4j_export import export_neo4j_to_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Type alias for migration function
MigrationFn = Callable[[Session, _PGConnection], None]

# Adapter to plug the CSV-based step-by-step flow into the standard signature
def run_step_by_step_migration(_: Session, conn: _PGConnection) -> None:
    """Run the interactive CSV migration that only needs PostgreSQL."""
    run_migration(conn)


# Adapter for type migration (CSV-based)
def run_type_migration(_: Session, conn: _PGConnection) -> None:
    """Run the type migration from CSV file."""
    migrate_types(_, conn)


# Adapter for asset type migration (CSV-based)
def run_asset_type_migration(_: Session, conn: _PGConnection) -> None:
    """Run the asset type migration from CSV file."""
    migrate_asset_types(_, conn)


# Adapter for fetching asset types from database
def run_fetch_asset_types(_: Session, conn: _PGConnection) -> None:
    """Fetch asset types from PostgreSQL database."""
    fetch_asset_types_from_db(_, conn)


# Factory functions that create migration functions with config values
def create_neo4j_export_fn(cfg) -> MigrationFn:
    """Create a Neo4j export function with config values."""
    def run_neo4j_export(session: Session, _: _PGConnection) -> None:
        """Export data from Neo4j to CSV files."""
        export_neo4j_to_csv(
            session, 
            _, 
            batch_size=cfg.neo4j_export_batch_size, 
            domain=cfg.community_domain
        )
    return run_neo4j_export


def create_community_migration_fn(cfg) -> MigrationFn:
    """Create a community migration function with config values."""
    def run_community_migration(session: Session, conn: _PGConnection) -> None:
        """Run the community migration with configurable domain."""
        migrate_communities(session, conn, domain=cfg.community_domain)
    return run_community_migration


# Migration configuration factory: (name, factory_function)
# Factory functions receive config and return the actual migration function
# Order matches the step-by-step migration process in README.md
MIGRATION_FACTORIES = [
    ("Type migration", lambda cfg: run_type_migration),
    ("Asset type migration", lambda cfg: run_asset_type_migration),
    ("Fetch asset types", lambda cfg: run_fetch_asset_types),
    ("Neo4j to CSV export", create_neo4j_export_fn),
    ("Client migration", lambda cfg: migrate_clients),
    ("Community migration", create_community_migration_fn),
    ("Step-by-step migration", lambda cfg: run_step_by_step_migration),
]

def build_migrations(cfg) -> list[tuple[str, MigrationFn]]:
    """Build migration list from factories with config."""
    return [
        (name, factory(cfg))
        for name, factory in MIGRATION_FACTORIES
    ]

MIGRATION_KEY_MAP_INDICES = {
    "type": 0,
    "asset-type": 1,
    "fetch-asset-types": 2,
    "export": 3,
    "neo4j-export": 3,
    "client": 4,
    "community": 5,
    "step": 6,
}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Neo4j → PostgreSQL data migration")
    parser.add_argument(
        "--env",
        type=str,
        default="local",
        choices=["local", "nec-ofc-stg", "nec-aws-stg", "nec-aws-prod", "emaar"],
        help="Environment to run migration against",
    )
    parser.add_argument(
        "--migration",
        choices=["export", "neo4j-export", "community", "client", "type", "asset-type", "fetch-asset-types", "step", "all"],
        help="Optional: run specific migration without interactive prompt",
    )
    return parser.parse_args()


def show_migration_menu(cfg) -> list[tuple[str, MigrationFn]]:
    """Display menu and return selected migrations."""
    migrations = build_migrations(cfg)
    print("\n" + "="*50)
    print("MIGRATION MENU")
    print("="*50)
    for idx, (name, _) in enumerate(migrations, start=1):
        print(f"{idx}. {name}")
    print(f"{len(migrations) + 1}. Run all migrations")
    print("="*50)

    while True:
        choice = input("\nEnter option number: ").strip()
        
        # Run all migrations
        if choice == str(len(migrations) + 1):
            return migrations
        
        # Run single migration
        if choice.isdigit():
            index = int(choice) - 1
            if 0 <= index < len(migrations):
                return [migrations[index]]
        
        print("Invalid choice. Please try again.")


def get_selected_migrations(arg_choice: str | None, cfg) -> list[tuple[str, MigrationFn]]:
    """Resolve which migrations to run based on argument or prompt."""
    migrations = build_migrations(cfg)
    
    if arg_choice is None:
        return show_migration_menu(cfg)
    
    # Map command line choices to migrations
    if arg_choice == "all":
        return migrations

    index = MIGRATION_KEY_MAP_INDICES.get(arg_choice)
    if index is not None and 0 <= index < len(migrations):
        return [migrations[index]]
    
    return migrations


def main() -> None:
    """Main migration entry point."""
    args = parse_args()
    env: EnvName = args.env
    cfg = get_config(env)

    # Get selected migrations (with config to build migration functions)
    selected_migrations = get_selected_migrations(args.migration, cfg)
    selected_names = ", ".join(name for name, _ in selected_migrations)
    
    logger.info("Environment: %s", env)
    logger.info("Selected migrations: %s", selected_names)
    print(f"\n→ Running: {selected_names}\n")

    # Create Neo4j driver
    driver = create_neo4j_driver(cfg.neo4j)
    
    try:
        with neo4j_session(driver) as nsession:
            for name, migration_fn in selected_migrations:
                logger.info("Running '%s'", name)
                print(f"\n{'='*50}")
                print(f"Running: {name}")
                print(f"{'='*50}\n")
                
                try:
                    with pg_connection(cfg.postgres) as conn:
                        migration_fn(nsession, conn)
                    logger.info("✓ %s completed successfully", name)
                    print(f"\n✓ {name} completed successfully\n")
                except Exception as e:
                    logger.error("✗ %s failed: %s", name, str(e))
                    print(f"\n✗ {name} failed: {str(e)}\n")
                    raise
    finally:
        driver.close()
        logger.info("Neo4j driver closed")

    logger.info("All migrations finished successfully for env=%s", env)
    print(f"\n{'='*50}")
    print("✓ ALL MIGRATIONS COMPLETED SUCCESSFULLY")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()