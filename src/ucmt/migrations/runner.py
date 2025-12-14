"""Migration runner: plan and apply migrations."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ucmt.exceptions import MigrationChecksumMismatchError
from ucmt.migrations.parser import MigrationFile
from ucmt.migrations.state import MigrationStateStore

__all__ = ["PendingMigration", "plan", "Runner"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PendingMigration:
    version: int
    name: str
    path: Path
    checksum: str
    sql: str


def plan(
    migrations: list[MigrationFile], state_store: MigrationStateStore
) -> list[PendingMigration]:
    """
    Pure function: determine which migrations are pending.

    Args:
        migrations: List of migration files from the migrations directory.
        state_store: State store to check which migrations have been applied.

    Returns:
        List of pending migrations sorted by version (ascending).
    """
    pending: list[PendingMigration] = []

    for mf in sorted(migrations, key=lambda m: m.version):
        if not state_store.has_applied(mf.version):
            pending.append(
                PendingMigration(
                    version=mf.version,
                    name=mf.name,
                    path=mf.path,
                    checksum=mf.checksum,
                    sql=mf.sql,
                )
            )

    return pending


Executor = Callable[[str, int], None]


class Runner:
    """
    Applies migrations in version order, recording state after each execution.

    Variable substitution: ${catalog} and ${schema} are replaced with Config values.
    """

    def __init__(
        self,
        state_store: MigrationStateStore,
        executor: Executor,
        catalog: str,
        schema: str,
    ) -> None:
        self._state_store = state_store
        self._executor = executor
        self._catalog = catalog
        self._schema = schema

    def _substitute_variables(self, sql: str) -> str:
        return sql.replace("${catalog}", self._catalog).replace(
            "${schema}", self._schema
        )

    def _check_checksums(self, migrations: list[MigrationFile]) -> None:
        applied = {a.version: a for a in self._state_store.list_applied()}

        for mf in migrations:
            if mf.version in applied:
                recorded = applied[mf.version]
                if recorded.checksum != mf.checksum:
                    raise MigrationChecksumMismatchError(
                        f"Migration V{mf.version}__{mf.name} checksum mismatch: "
                        f"recorded={recorded.checksum}, file={mf.checksum}"
                    )

    def apply(
        self,
        migrations: list[MigrationFile],
        dry_run: bool = False,
    ) -> None:
        """
        Apply pending migrations in version order.

        Args:
            migrations: All migration files from the migrations directory.
            dry_run: If True, log what would be executed but don't execute.
        """
        self._check_checksums(migrations)

        pending = plan(migrations, self._state_store)

        if not pending:
            logger.info("Schema is up to date. No pending migrations.")
            return

        for pm in pending:
            migration_label = f"V{pm.version}__{pm.name}"

            if dry_run:
                logger.info(f"[DRY RUN] Would apply {migration_label}")
                continue

            logger.info(f"Applying {migration_label}...")

            sql = self._substitute_variables(pm.sql)
            try:
                self._executor(sql, pm.version)
                self._state_store.record_applied(
                    version=pm.version,
                    name=pm.name,
                    checksum=pm.checksum,
                    success=True,
                )
                logger.info(f"Applied {migration_label}")
            except Exception as exc:
                self._state_store.record_applied(
                    version=pm.version,
                    name=pm.name,
                    checksum=pm.checksum,
                    success=False,
                    error=str(exc),
                )
                raise
