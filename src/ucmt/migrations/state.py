from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol, runtime_checkable

from ucmt.exceptions import MigrationStateConflictError


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
