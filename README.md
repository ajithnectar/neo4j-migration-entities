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


