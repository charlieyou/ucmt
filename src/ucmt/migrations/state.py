from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

from ucmt.exceptions import MigrationStateConflictError


@dataclass
class AppliedMigration:
    version: int
    name: str
    checksum: str
    applied_at: datetime
    success: bool
    error: Optional[str] = None


class MigrationStateStore(Protocol):
    def list_applied(self) -> list[AppliedMigration]: ...

    def get_last_applied(self) -> Optional[AppliedMigration]: ...

    def record_applied(
        self,
        version: int,
        name: str,
        checksum: str,
        success: bool,
        error: Optional[str] = None,
    ) -> None: ...

    def has_applied(self, version: int) -> bool: ...


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
