try:
    from dataclasses import dataclass
except ImportError:
    # For Python 3.6, use backport
    from dataclasses import dataclass

import os

from dotenv import load_dotenv

load_dotenv()

# Valid environment names
VALID_ENV_NAMES = ("local", "nec-ofc-stg", "nec-aws-stg", "nec-aws-prod", "emaar")
EnvName = str  # Type alias for environment name (Python 3.6 compatible)


@dataclass
class Neo4jConfig:
    uri: str
    username: str
    password: str
    mode: str = None


@dataclass
class PostgresConfig:
    host: str
    port: int
    dbname: str
    username: str
    password: str


@dataclass
class AppConfig:
    env: EnvName
    neo4j: Neo4jConfig
    postgres: PostgresConfig
    neo4j_export_batch_size: int = 1000
    client_domain: str = "ecd"
    community_domain: str = "ecd"


def _env_or_default(key, default=None):
    value = os.getenv(key)
    if value is None:
        if default is None:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return default
    return value


def get_config(env):
    """
    Build configuration for a given environment.

    You can override any of these via environment variables, e.g.:
    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_MODE,
    PG_HOST, PG_PORT, PG_DB, PG_USERNAME, PG_PASSWORD.
    For the nectar_new database override NECTAR_PG_HOST, NECTAR_PG_PORT,
    NECTAR_PG_DB, NECTAR_PG_USERNAME, NECTAR_PG_PASSWORD.
    """
    # Validate environment name
    if env not in VALID_ENV_NAMES:
        raise ValueError("Invalid environment name: {}. Valid options: {}".format(
            env, ", ".join(VALID_ENV_NAMES)
        ))

    if env == "local":
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "bolt://localhost:7687"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "test123"),
            mode=_env_or_default("NEO4J_MODE", "SINGLE"),
        )
        pg = PostgresConfig(
            host=_env_or_default("PG_HOST", "localhost"),
            port=int(_env_or_default("PG_PORT", "5432")),
            dbname=_env_or_default("PG_DB", "neo4j_migration"),
            username=_env_or_default("PG_USERNAME", "appuser"),
            password=_env_or_default("PG_PASSWORD", "NecOfc@123"),
        )
        neo4j_export_batch_size = 1000
        client_domain = "ecd"
        community_domain = "ecd"

    elif env == "nec-ofc-stg":
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "bolt://nec-ofc-dbc3:7687"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "NecOfc@123"),
            mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
        )
        pg = PostgresConfig(
            host=_env_or_default("NECTAR_PG_HOST", "nec-ofc-dbc1"),
            port=int(_env_or_default("NECTAR_PG_PORT", "5432")),
            dbname=_env_or_default("NECTAR_PG_DB", "accumulation"),
            username=_env_or_default("NECTAR_PG_USERNAME", "appuser"),
            password=_env_or_default("NECTAR_PG_PASSWORD", "NecOfc@123"),
        )
        neo4j_export_batch_size = 3000
        client_domain = "ecd"
        community_domain = "ecd"

    elif env == "emaar":
            neo4j = Neo4jConfig(
                uri=_env_or_default("NEO4J_URI", "bolt://emrbldbmsgdb2:7687"),
                username=_env_or_default("NEO4J_USERNAME", "datalkz"),
                password=_env_or_default("NEO4J_PASSWORD", "Datalkz123*"),
                mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
            )
            pg = PostgresConfig(
                host=_env_or_default("PG_HOST", "10.95.6.53"),
                port=int(_env_or_default("PG_PORT", "5432")),
                dbname=_env_or_default("PG_DB", "nectar"),
                username=_env_or_default("PG_USERNAME", "appuser"),
                password=_env_or_default("PG_PASSWORD", "Bmsapp@2435"),
            )
            neo4j_export_batch_size = 6000
            client_domain = "emaar"
            community_domain = "emaar"

    elif env == "nec-aws-stg":
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "http://nec-aws-stg-neo4j:7474"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "password"),
            mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
        )
        pg = PostgresConfig(
            host=_env_or_default("NECTAR_PG_HOST", "nec-aws-stg-pg"),
            port=int(_env_or_default("NECTAR_PG_PORT", "5432")),
            dbname=_env_or_default("NECTAR_PG_DB", "neo4jawesometicks"),
            username=_env_or_default("NECTAR_PG_USERNAME", "appuser"),
            password=_env_or_default("NECTAR_PG_PASSWORD", "password"),
        )
        neo4j_export_batch_size = 1000
        client_domain = "ecd"
        community_domain = "ecd"

    else:  # nec-aws-prod
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "http://nec-aws-prod-neo4j:7474"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "password"),
            mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
        )
        pg = PostgresConfig(
            host=_env_or_default("NECTAR_PG_HOST", "nec-aws-prod-pg"),
            port=int(_env_or_default("NECTAR_PG_PORT", "5432")),
            dbname=_env_or_default("NECTAR_PG_DB", "neo4jawesometicks"),
            username=_env_or_default("NECTAR_PG_USERNAME", "appuser"),
            password=_env_or_default("NECTAR_PG_PASSWORD", "password"),
        )
        neo4j_export_batch_size = 1000
        client_domain = "ecd"
        community_domain = "ecd"
    
    return AppConfig(
        env=env,
        neo4j=neo4j,
        postgres=pg,
        neo4j_export_batch_size=neo4j_export_batch_size,
        client_domain=client_domain,
        community_domain=community_domain,
    )


