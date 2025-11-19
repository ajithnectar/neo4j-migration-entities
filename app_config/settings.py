from dataclasses import dataclass
from typing import Dict, Literal, Optional
import os

from dotenv import load_dotenv

load_dotenv()

EnvName = Literal["local", "nec-ofc-stg", "nec-aws-stg", "nec-aws-prod"]


@dataclass
class Neo4jConfig:
    uri: str
    username: str
    password: str
    mode: Optional[str] = None


@dataclass
class PostgresConfig:
    host: str
    port: int
    dbname: str
    username: str
    password: str


PostgresTarget = Literal["accesscontrol", "nectar_new"]


@dataclass
class AppConfig:
    env: EnvName
    neo4j: Neo4jConfig
    postgres: Dict[PostgresTarget, PostgresConfig]

    def get_postgres(self, target: PostgresTarget) -> PostgresConfig:
        try:
            return self.postgres[target]
        except KeyError as exc:
            raise RuntimeError(
                f"No PostgreSQL configuration found for target '{target}'"
            ) from exc


def _env_or_default(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key)
    if value is None:
        if default is None:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return default
    return value


def get_config(env: EnvName) -> AppConfig:
    """
    Build configuration for a given environment.

    You can override any of these via environment variables, e.g.:
    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_MODE,
    PG_HOST, PG_PORT, PG_DB, PG_USERNAME, PG_PASSWORD.
    For the nectar_new database override NECTAR_PG_HOST, NECTAR_PG_PORT,
    NECTAR_PG_DB, NECTAR_PG_USERNAME, NECTAR_PG_PASSWORD.
    """

    if env == "local":
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "bolt://localhost:7687"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "test123"),
            mode=_env_or_default("NEO4J_MODE", "SINGLE"),
        )
        pg = {
            "accesscontrol": PostgresConfig(
                host=_env_or_default("PG_HOST", "localhost"),
                port=int(_env_or_default("PG_PORT", "5432")),
                dbname=_env_or_default("PG_DB", "neo4j_migration"),
                username=_env_or_default("PG_USERNAME", "appuser"),
                password=_env_or_default("PG_PASSWORD", "NecOfc@123"),
            ),
            "nectar_new": PostgresConfig(
                 host=_env_or_default("PG_HOST", "localhost"),
                port=int(_env_or_default("PG_PORT", "5432")),
                dbname=_env_or_default("PG_DB", "neo4j_migration"),
                username=_env_or_default("PG_USERNAME", "appuser"),
                password=_env_or_default("PG_PASSWORD", "NecOfc@123"),
            ),
        }

    elif env == "nec-ofc-stg":
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "bolt://nec-ofc-dbc3:7687"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "NecOfc@123"),
            mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
        )
        pg = {
            "accesscontrol": PostgresConfig(
                host=_env_or_default("PG_HOST", "nec-ofc-dbc1"),
                port=int(_env_or_default("PG_PORT", "5432")),
                dbname=_env_or_default("PG_DB", "neo4j_migration"),
                username=_env_or_default("PG_USERNAME", "appuser"),
                password=_env_or_default("PG_PASSWORD", "NecOfc@123"),
            ),
            "nectar_new": PostgresConfig(
                host=_env_or_default("NECTAR_PG_HOST", "nec-ofc-dbc1"),
                port=int(_env_or_default("NECTAR_PG_PORT", "5432")),
                dbname=_env_or_default("NECTAR_PG_DB", "neo4j_migration"),
                username=_env_or_default("NECTAR_PG_USERNAME", "appuser"),
                password=_env_or_default("NECTAR_PG_PASSWORD", "NecOfc@123"),
            ),
        }

    elif env == "nec-aws-stg":
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "http://nec-aws-stg-neo4j:7474"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "password"),
            mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
        )
        pg = {
            "accesscontrol": PostgresConfig(
                host=_env_or_default("PG_HOST", "nec-aws-stg-pg"),
                port=int(_env_or_default("PG_PORT", "5432")),
                dbname=_env_or_default("PG_DB", "neo4jmigrate"),
                username=_env_or_default("PG_USERNAME", "appuser"),
                password=_env_or_default("PG_PASSWORD", "password"),
            ),
            "nectar_new": PostgresConfig(
                host=_env_or_default("NECTAR_PG_HOST", "nec-aws-stg-pg"),
                port=int(_env_or_default("NECTAR_PG_PORT", "5432")),
                dbname=_env_or_default("NECTAR_PG_DB", "neo4jawesometicks"),
                username=_env_or_default("NECTAR_PG_USERNAME", "appuser"),
                password=_env_or_default("NECTAR_PG_PASSWORD", "password"),
            ),
        }

    else:  # nec-aws-prod
        neo4j = Neo4jConfig(
            uri=_env_or_default("NEO4J_URI", "http://nec-aws-prod-neo4j:7474"),
            username=_env_or_default("NEO4J_USERNAME", "neo4j"),
            password=_env_or_default("NEO4J_PASSWORD", "password"),
            mode=_env_or_default("NEO4J_MODE", "CLUSTER"),
        )
        pg = {
            "accesscontrol": PostgresConfig(
                host=_env_or_default("PG_HOST", "nec-aws-prod-pg"),
                port=int(_env_or_default("PG_PORT", "5432")),
                dbname=_env_or_default("PG_DB", "neo4jmigrate"),
                username=_env_or_default("PG_USERNAME", "appuser"),
                password=_env_or_default("PG_PASSWORD", "password"),
            ),
            "nectar_new": PostgresConfig(
                host=_env_or_default("NECTAR_PG_HOST", "nec-aws-prod-pg"),
                port=int(_env_or_default("NECTAR_PG_PORT", "5432")),
                dbname=_env_or_default("NECTAR_PG_DB", "neo4jawesometicks"),
                username=_env_or_default("NECTAR_PG_USERNAME", "appuser"),
                password=_env_or_default("NECTAR_PG_PASSWORD", "password"),
            ),
        }

    return AppConfig(env=env, neo4j=neo4j, postgres=pg)


