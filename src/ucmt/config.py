"""Configuration management for ucmt."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Config:
    """Configuration for ucmt."""

    catalog: str
    schema: str
    server_hostname: Optional[str] = None
    http_path: Optional[str] = None
    access_token: Optional[str] = None
    schema_path: Path = Path("schema/tables")
    migrations_path: Path = Path("sql/migrations")
    state_table: str = "_ucmt_migrations"

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        import os

        return cls(
            catalog=os.environ.get("DATABRICKS_CATALOG", ""),
            schema=os.environ.get("DATABRICKS_SCHEMA", ""),
            server_hostname=os.environ.get("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.environ.get("DATABRICKS_HTTP_PATH"),
            access_token=os.environ.get("DATABRICKS_ACCESS_TOKEN"),
        )
