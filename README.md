## Neo4j → PostgreSQL Data Migration

This project migrates data from a Neo4j graph database into PostgreSQL.

### Structure

- `config/` – environment-specific configuration (Neo4j & PostgreSQL).
- `db/neo4j_utils.py` – helpers to connect to Neo4j and run queries.
- `db/postgres_utils.py` – helpers to connect to PostgreSQL and run inserts/updates.
- `migrations/` – scripts that extract from Neo4j and load into PostgreSQL.
- `main.py` – entrypoint to run a migration for a chosen environment.

### Setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

2. Configure environment variables or `.env` file (see `config/settings.py`).

3. Run migration:

```bash
python main.py --env local
```

### Migration Process

Follow these steps in order to migrate data:

1. **Create typeToMigrate.csv (types table) and import to program**
   - Ensure `typeToMigrate.csv` file exists in the project root
   - The CSV should contain columns: `parent_name`, `child_name`, `child_display_name`, `child_template_name`

2. **Run type migration - type will be inserted to psql**
   ```bash
   python main.py --env local --migration type
   ```
   Or select "Type migration" from the interactive menu

3. **Go to clients migration and change the query domain and save and run the client migration**
   - Edit `migrations/client_migration.py` and update the domain in the Cypher query
   - Run client migration:
   ```bash
   python main.py --env local --migration client
   ```

4. **Go to community migration and change the query domain and save and run the community migration**
   - Edit `migrations/community_migration.py` and update the domain in the Cypher query
   - Run community migration:
   ```bash
   python main.py --env local --migration community
   ```

5. **Run again and select Start from Subcommunity it will insert subcommunity, building, space, asset, points**
   ```bash
   python main.py --env local --migration step
   ```
   - Select option "1. Start from Subcommunity" from the menu
   - This will migrate: subcommunity → building → space → asset → points

### Available Migrations

- **Domain migration**: Migrates domains from Neo4j to PostgreSQL
- **Community migration**: Migrates communities from Neo4j to PostgreSQL
- **Client migration**: Migrates clients from Neo4j to PostgreSQL
- **Type migration**: Migrates types from `typeToMigrate.csv` to PostgreSQL
- **Step-by-step migration**: Interactive CSV-based migration for subcommunities, buildings, spaces, assets, and points


