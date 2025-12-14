"""Configuration management for ucmt."""

import os
from dataclasses import dataclass
from typing import Optional

from ucmt.exceptions import ConfigError


@dataclass
class Config:
    """Configuration for ucmt."""

    catalog: Optional[str] = None
    schema: Optional[str] = None
    schema_dir: str = "schema"
    migrations_dir: str = "sql/migrations"
    state_table: str = "_ucmt_migrations"
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_http_path: Optional[str] = None
    databricks_warehouse_id: Optional[str] = None

    @classmethod
    def from_env(
        cls,
        *,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        schema_dir: Optional[str] = None,
        migrations_dir: Optional[str] = None,
        state_table: Optional[str] = None,
        databricks_host: Optional[str] = None,
        databricks_token: Optional[str] = None,
        databricks_http_path: Optional[str] = None,
        databricks_warehouse_id: Optional[str] = None,
    ) -> "Config":
        """Load configuration from environment variables with optional CLI overrides."""
        return cls(
            catalog=catalog if catalog is not None else os.environ.get("UCMT_CATALOG"),
            schema=schema if schema is not None else os.environ.get("UCMT_SCHEMA"),
            schema_dir=schema_dir
            if schema_dir is not None
            else os.environ.get("UCMT_SCHEMA_DIR", "schema"),
            migrations_dir=migrations_dir
            if migrations_dir is not None
            else os.environ.get("UCMT_MIGRATIONS_DIR", "sql/migrations"),
            state_table=state_table
            if state_table is not None
            else os.environ.get("UCMT_STATE_TABLE", "_ucmt_migrations"),
            databricks_host=databricks_host
            if databricks_host is not None
            else os.environ.get("DATABRICKS_HOST"),
            databricks_token=databricks_token
            if databricks_token is not None
            else os.environ.get("DATABRICKS_TOKEN"),
            databricks_http_path=databricks_http_path
            if databricks_http_path is not None
            else os.environ.get("DATABRICKS_HTTP_PATH"),
            databricks_warehouse_id=databricks_warehouse_id
            if databricks_warehouse_id is not None
            else os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        )

    def validate_for_db_ops(self) -> None:
        """Validate that all required fields for database operations are present.

        Raises:
            ConfigError: If catalog, schema, or connection info is missing.
        """
        missing = []
        if not self.catalog:
            missing.append("catalog")
        if not self.schema:
            missing.append("schema")
        if not self.databricks_host:
            missing.append("databricks_host")
        if not self.databricks_token:
            missing.append("databricks_token")

        if missing:
            raise ConfigError(f"Missing required configuration: {', '.join(missing)}")
