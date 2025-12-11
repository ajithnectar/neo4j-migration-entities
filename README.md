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

Follow these steps **in order** to migrate data:

#### Step 1: Types Migration
Migrates type definitions from Neo4j to PostgreSQL.

**Note:** If `typeToMigrate.csv` doesn't exist, it will be automatically created by fetching data from Neo4j.

```bash
python main.py --env <environment> --migration type
```

Or select **"1. Type migration"** from the interactive menu.

This will:
- Fetch type data from Neo4j (if CSV doesn't exist) and create `typeToMigrate.csv`
- Insert types into PostgreSQL `public.types` table

---

#### Step 2: Client Migration
Migrates clients from Neo4j to PostgreSQL.

```bash
python main.py --env <environment> --migration client
```

Or select **"2. Client migration"** from the interactive menu.

**Note:** Update the domain in the Cypher query in `migrations/client_migration.py` if needed, or configure it via `app_config/settings.py`.

---

#### Step 3: Community Migration
Migrates communities from Neo4j to PostgreSQL.

```bash
python main.py --env <environment> --migration community
```

Or select **"3. Community migration"** from the interactive menu.

**Note:** Update the domain in the Cypher query in `migrations/community_migration.py` if needed, or configure it via `app_config/settings.py`.

---

#### Step 4: Asset Type Migration
Migrates asset type definitions from Neo4j to PostgreSQL.

**Note:** If `AssetTypeToMigrate.csv` doesn't exist, it will be automatically created by fetching data from Neo4j.

```bash
python main.py --env <environment> --migration asset-type
```

Or select **"4. Asset type migration"** from the interactive menu.

This will:
- Fetch asset type data from Neo4j (if CSV doesn't exist) and create `AssetTypeToMigrate.csv`
- Insert asset types into PostgreSQL `public.asset_type` table

---

#### Step 5: Fetch Asset Types (Optional)
Fetches all asset types from PostgreSQL and saves them to `assetType.csv`.

```bash
python main.py --env <environment> --migration fetch-asset-types
```

Or select **"5. Fetch asset types"** from the interactive menu.

This creates `assetType.csv` with all asset types from the database for reference.

---

#### Step 6: Neo4j to CSV Export
Exports data from Neo4j to CSV files for the complete migration.

```bash
python main.py --env <environment> --migration neo4j-export
```

Or select **"6. Neo4j to CSV export"** from the interactive menu.

This will:
- Query Neo4j for all assets, buildings, spaces, subcommunities, and points
- Export data to multiple CSV files in the `data/` directory (e.g., `data_1.csv`, `data_2.csv`, etc.)
- Files are created in batches (default: 1000 records per file)

**Note:** The domain filter is configured in `app_config/settings.py` (default: "ecd"). You can override it via environment variable `COMMUNITY_DOMAIN`.

---

**Note:** Update the domain in the Cypher query in `migrations/community_migration.py` if needed, or configure it via `app_config/settings.py`.

---

#### Step 7: Complete Migration (Step-by-Step)
Migrates subcommunities, buildings, spaces, assets, and points from CSV files to PostgreSQL.

```bash
python main.py --env <environment> --migration step
```

Or select **"7. Step-by-step migration"** from the interactive menu.

This will show an interactive menu:
```
==================================================
1. Start from Subcommunity
2. Start from Building
3. Start from Space
4. Start from Asset
5. Start from Point
6. Exit
==================================================
```

**Recommended:** Select **"1. Start from Subcommunity"** to migrate everything in the correct order:
- Subcommunities
- Buildings
- Spaces
- Assets
- Asset-Space relationships
- Points
- Asset-Point relationships
- Asset-Type-Point relationships

---

### Available Migrations

| Migration | Description | Command |
|-----------|-------------|---------|
| **Type migration** | Migrates types from Neo4j/CSV to PostgreSQL | `--migration type` |
| **Client migration** | Migrates clients from Neo4j to PostgreSQL | `--migration client` |
| **Community migration** | Migrates communities from Neo4j to PostgreSQL | `--migration community` |
| **Asset type migration** | Migrates asset types from Neo4j/CSV to PostgreSQL | `--migration asset-type` |
| **Fetch asset types** | Fetches asset types from PostgreSQL to CSV | `--migration fetch-asset-types` |
| **Neo4j to CSV export** | Exports data from Neo4j to CSV files | `--migration neo4j-export` |
| **Step-by-step migration** | Interactive CSV-based migration | `--migration step` |

### Environment Options

- `local` - Local development environment
- `nec-ofc-stg` - NEC Office staging environment
- `nec-aws-stg` - NEC AWS staging environment
- `nec-aws-prod` - NEC AWS production environment
- `emaar` - Emaar environment

Example:
```bash
python main.py --env emaar --migration type
```


