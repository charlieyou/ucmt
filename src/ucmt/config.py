"""Configuration management for ucmt."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Configuration for ucmt."""

    catalog: str
    schema: str
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
        )
