"""Migration state tracking."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


@dataclass
class MigrationRecord:
    """Record of an applied migration."""

    version: str
    description: str
    applied_at: datetime
    checksum: str
    success: bool
    error_message: Optional[str] = None


class MigrationState:
    """Track applied migrations in a state table."""

    def __init__(
        self, client: Any, catalog: str, schema: str, table: str = "_ucmt_migrations"
    ):
        self.client = client
        self.fqn = f"{catalog}.{schema}.{table}"

    def ensure_table(self) -> None:
        """Create migrations state table if not exists."""
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.fqn} (
                version STRING NOT NULL,
                description STRING,
                applied_at TIMESTAMP NOT NULL,
                checksum STRING NOT NULL,
                success BOOLEAN NOT NULL,
                error_message STRING
            ) USING DELTA
        """
        self.client.execute(sql)

    def get_applied_versions(self) -> set[str]:
        """Get set of applied migration versions."""
        rows = self.client.execute(
            f"SELECT version FROM {self.fqn} WHERE success = true"
        )
        return {row["version"] for row in rows}

    def record_migration(self, record: MigrationRecord) -> None:
        """Record a migration execution."""
        version = _escape_sql_string(record.version)
        description = _escape_sql_string(record.description)
        checksum = _escape_sql_string(record.checksum)
        error_msg = (
            f"'{_escape_sql_string(record.error_message)}'"
            if record.error_message
            else "NULL"
        )
        applied_at = record.applied_at.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
            INSERT INTO {self.fqn}
            (version, description, applied_at, checksum, success, error_message)
            VALUES ('{version}', '{description}',
                    '{applied_at}', '{checksum}',
                    {str(record.success).lower()},
                    {error_msg})
        """
        self.client.execute(sql)
