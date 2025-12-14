import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Protocol, runtime_checkable

from ucmt.databricks.client import DatabricksClient
from ucmt.exceptions import ConfigError, MigrationStateConflictError

if TYPE_CHECKING:
    from ucmt.config import Config


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, kind: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise ConfigError(f"Invalid {kind} identifier: {value!r}")
    return value


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


@dataclass
class AppliedMigration:
    """Record of a migration that has been executed (successfully or not)."""

    version: int
    name: str
    checksum: str
    applied_at: datetime
    success: bool
    error: Optional[str] = None


@runtime_checkable
class MigrationStateStore(Protocol):
    """
    Stores the state of executed migrations.

    Semantics:
    - "applied" means "this version has been executed at least once",
      regardless of success or failure.
    - Implementations must be idempotent: re-recording the same version
      with the same checksum MUST NOT create duplicates or change state.
    """

    def list_applied(self) -> list[AppliedMigration]:
        """Return all recorded migrations in ascending version order."""
        ...

    def get_last_applied(self) -> Optional[AppliedMigration]:
        """Return the migration with the highest version, or None if empty."""
        ...

    def record_applied(
        self,
        version: int,
        name: str,
        checksum: str,
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        """Record that a migration has been run (successfully or not)."""
        ...

    def has_applied(self, version: int) -> bool:
        """Return True if this version has ever been recorded."""
        ...


class InMemoryMigrationStateStore:
    def __init__(self) -> None:
        self._applied: dict[int, AppliedMigration] = {}

    def list_applied(self) -> list[AppliedMigration]:
        return sorted(self._applied.values(), key=lambda m: m.version)

    def get_last_applied(self) -> Optional[AppliedMigration]:
        if not self._applied:
            return None
        return max(self._applied.values(), key=lambda m: m.version)

    def record_applied(
        self,
        version: int,
        name: str,
        checksum: str,
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        if version in self._applied:
            existing = self._applied[version]
            if existing.checksum != checksum:
                raise MigrationStateConflictError(
                    f"Migration {version} already recorded with checksum {existing.checksum}, "
                    f"but attempted to record with checksum {checksum}"
                )
            return

        self._applied[version] = AppliedMigration(
            version=version,
            name=name,
            checksum=checksum,
            applied_at=datetime.now(),
            success=success,
            error=error,
        )

    def has_applied(self, version: int) -> bool:
        return version in self._applied


class DatabricksMigrationStateStore:
    """Stores migration state in a Databricks Delta table."""

    _CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS {table} (
            version INT,
            name STRING,
            checksum STRING,
            applied_at TIMESTAMP,
            success BOOLEAN,
            error STRING
        )
    """

    def __init__(self, config: "Config") -> None:
        self._catalog = _validate_identifier(config.catalog, "catalog")
        self._schema = _validate_identifier(config.schema, "schema")
        self._state_table = _validate_identifier(config.state_table, "state_table")
        self._client = DatabricksClient(
            host=config.databricks_host,
            token=config.databricks_token,
            http_path=config.databricks_http_path,
        )
        self._client.connect()
        self._ensure_state_table()

    def __enter__(self) -> "DatabricksMigrationStateStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the database connection."""
        self._client.close()

    @property
    def state_table_fqn(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._state_table}"

    def _ensure_state_table(self) -> None:
        self._client.execute(self._CREATE_TABLE_SQL.format(table=self.state_table_fqn))

    def list_applied(self) -> list[AppliedMigration]:
        rows = self._client.fetchall(
            f"SELECT version, name, checksum, applied_at, success, error "
            f"FROM {self.state_table_fqn} ORDER BY version ASC"
        )
        return [
            AppliedMigration(
                version=row["version"],
                name=row["name"],
                checksum=row["checksum"],
                applied_at=row["applied_at"],
                success=row["success"],
                error=row["error"],
            )
            for row in rows
        ]

    def get_last_applied(self) -> Optional[AppliedMigration]:
        rows = self._client.fetchall(
            f"SELECT version, name, checksum, applied_at, success, error "
            f"FROM {self.state_table_fqn} ORDER BY version DESC LIMIT 1"
        )
        if not rows:
            return None
        row = rows[0]
        return AppliedMigration(
            version=row["version"],
            name=row["name"],
            checksum=row["checksum"],
            applied_at=row["applied_at"],
            success=row["success"],
            error=row["error"],
        )

    def record_applied(
        self,
        version: int,
        name: str,
        checksum: str,
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        existing = self._get_by_version(version)
        if existing is not None:
            if existing.checksum != checksum:
                raise MigrationStateConflictError(
                    f"Migration {version} already recorded with checksum {existing.checksum}, "
                    f"but attempted to record with checksum {checksum}"
                )
            return

        def q(v: str) -> str:
            return "'" + _escape_sql_string(v) + "'"

        error_value = q(error) if error is not None else "NULL"
        success_value = "true" if success else "false"
        self._client.execute(
            f"INSERT INTO {self.state_table_fqn} "
            f"(version, name, checksum, applied_at, success, error) VALUES "
            f"({version}, {q(name)}, {q(checksum)}, current_timestamp(), {success_value}, {error_value})"
        )

    def has_applied(self, version: int) -> bool:
        rows = self._client.fetchall(
            f"SELECT version FROM {self.state_table_fqn} WHERE version = {version}"
        )
        return len(rows) > 0

    def _get_by_version(self, version: int) -> Optional[AppliedMigration]:
        rows = self._client.fetchall(
            f"SELECT version, name, checksum, applied_at, success, error "
            f"FROM {self.state_table_fqn} WHERE version = {version}"
        )
        if not rows:
            return None
        row = rows[0]
        return AppliedMigration(
            version=row["version"],
            name=row["name"],
            checksum=row["checksum"],
            applied_at=row["applied_at"],
            success=row["success"],
            error=row["error"],
        )
