"""Utility functions for Databricks operations.

Extracts common DB logic from CLI for reuse and testability.
"""

from typing import Optional

from ucmt.config import Config
from ucmt.schema.models import Schema


def build_config_and_validate(
    *,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    profile: Optional[str] = None,
) -> Config:
    """Load config from ~/.databrickscfg/env and validate for DB operations.

    Args:
        catalog: Catalog name (overrides env/config)
        schema: Schema name (overrides env/config)
        profile: Databricks config profile name

    Returns:
        Config: Validated configuration.

    Raises:
        ConfigError: If required configuration is missing.
    """
    config = Config.from_env(catalog=catalog, schema=schema, profile=profile)
    config.validate_for_db_ops()
    return config


def get_online_schema(config: Config) -> Schema:
    """Get current schema from database via introspection.

    Args:
        config: Validated configuration with DB connection info.

    Returns:
        Schema: Current database schema.

    Raises:
        ConfigError: If DB config is invalid.
        Exception: If introspection fails.
    """
    from ucmt.databricks.client import DatabricksClient
    from ucmt.schema.introspect import SchemaIntrospector

    config.validate_for_db_ops()

    with DatabricksClient(
        host=config.databricks_host,
        token=config.databricks_token,
        http_path=config.databricks_http_path,
    ) as client:
        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        return introspector.introspect_schema()


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL text into individual statements.

    Handles:
    - Semicolon-separated statements
    - Comment-only segments starting with -- (skipped)
    - Empty statements (skipped)

    Limitations:
    - Does NOT handle semicolons inside string literals
    - Drops segments that START with -- (including multi-line segments
      where the first line is a comment but later lines have SQL)
    - For complex cases, consider single-statement migrations

    Args:
        sql: SQL text potentially containing multiple statements.

    Returns:
        List of non-empty SQL statements (without trailing semicolons).
    """
    statements = []
    for part in sql.split(";"):
        stmt = part.strip()
        if stmt and not stmt.startswith("--"):
            statements.append(stmt)
    return statements
