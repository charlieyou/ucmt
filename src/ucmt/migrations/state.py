import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Protocol, runtime_checkable

from databricks import sql

from ucmt.exceptions import ConfigError, MigrationStateConflictError

if TYPE_CHECKING:
    from ucmt.config import Config


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, kind: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise ConfigError(f"Invalid {kind} identifier: {value!r}")
    return value


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
            # Idempotent: same version & checksum -> do not overwrite existing state.
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
        self._connection = sql.connect(
            server_hostname=config.databricks_host,
            http_path=config.databricks_http_path,
            access_token=config.databricks_token,
        )
        self._ensure_state_table()

    @property
    def state_table_fqn(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._state_table}"

    def _ensure_state_table(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(self._CREATE_TABLE_SQL.format(table=self.state_table_fqn))

    def list_applied(self) -> list[AppliedMigration]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT version, name, checksum, applied_at, success, error "
                f"FROM {self.state_table_fqn} ORDER BY version ASC"
            )
            rows = cursor.fetchall()
        return [
            AppliedMigration(
                version=row[0],
                name=row[1],
                checksum=row[2],
                applied_at=row[3],
                success=row[4],
                error=row[5],
            )
            for row in rows
        ]

    def get_last_applied(self) -> Optional[AppliedMigration]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT version, name, checksum, applied_at, success, error "
                f"FROM {self.state_table_fqn} ORDER BY version DESC LIMIT 1"
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return AppliedMigration(
            version=row[0],
            name=row[1],
            checksum=row[2],
            applied_at=row[3],
            success=row[4],
            error=row[5],
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

        with self._connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {self.state_table_fqn} "
                f"(version, name, checksum, applied_at, success, error) VALUES "
                f"(?, ?, ?, current_timestamp(), ?, ?)",
                [version, name, checksum, success, error],
            )

    def has_applied(self, version: int) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT version FROM {self.state_table_fqn} WHERE version = ?",
                [version],
            )
            return cursor.fetchone() is not None

    def _get_by_version(self, version: int) -> Optional[AppliedMigration]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT version, name, checksum, applied_at, success, error "
                f"FROM {self.state_table_fqn} WHERE version = ?",
                [version],
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return AppliedMigration(
            version=row[0],
            name=row[1],
            checksum=row[2],
            applied_at=row[3],
            success=row[4],
            error=row[5],
        )
