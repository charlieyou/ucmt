"""Tests for migration runner."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pytest

from ucmt.exceptions import MigrationChecksumMismatchError
from ucmt.migrations.parser import MigrationFile
from ucmt.migrations.runner import PendingMigration, plan, Runner
from ucmt.migrations.state import AppliedMigration, InMemoryMigrationStateStore
from datetime import datetime


def make_migration_file(
    version: int, name: str = "test", sql: str = "SELECT 1;", checksum: str = "abc123"
) -> MigrationFile:
    return MigrationFile(
        version=version,
        name=name,
        path=Path(f"/migrations/V{version}__{name}.sql"),
        checksum=checksum,
        sql=sql,
    )


def make_applied(
    version: int,
    name: str = "test",
    checksum: str = "abc123",
    success: bool = True,
    error: Optional[str] = None,
) -> AppliedMigration:
    return AppliedMigration(
        version=version,
        name=name,
        checksum=checksum,
        applied_at=datetime.now(),
        success=success,
        error=error,
    )


class TestPlan:
    def test_plan_returns_pending_only(self) -> None:
        files = [make_migration_file(1), make_migration_file(2), make_migration_file(3)]
        store = InMemoryMigrationStateStore()
        store.record_applied(1, "test", "abc123", success=True)

        pending = plan(files, store)

        assert len(pending) == 2
        assert pending[0].version == 2
        assert pending[1].version == 3

    def test_plan_skips_already_applied(self) -> None:
        files = [make_migration_file(1), make_migration_file(2)]
        store = InMemoryMigrationStateStore()
        store.record_applied(1, "test", "abc123", success=True)
        store.record_applied(2, "test", "abc123", success=True)

        pending = plan(files, store)

        assert pending == []

    def test_plan_is_pure_function(self) -> None:
        files = [make_migration_file(1), make_migration_file(2)]
        store = InMemoryMigrationStateStore()
        store.record_applied(1, "test", "abc123", success=True)

        pending1 = plan(files, store)
        pending2 = plan(files, store)

        assert pending1 == pending2
        assert len(store.list_applied()) == 1


class TestRunner:
    def test_apply_executes_in_version_order(self) -> None:
        files = [make_migration_file(3), make_migration_file(1), make_migration_file(2)]
        store = InMemoryMigrationStateStore()
        executed: list[int] = []

        def executor(sql: str, version: int) -> None:
            executed.append(version)

        runner = Runner(store, executor, catalog="cat", schema="sch")
        runner.apply(files)

        assert executed == [1, 2, 3]

    def test_apply_records_each_success(self) -> None:
        files = [make_migration_file(1), make_migration_file(2)]
        store = InMemoryMigrationStateStore()

        def executor(sql: str, version: int) -> None:
            pass

        runner = Runner(store, executor, catalog="cat", schema="sch")
        runner.apply(files)

        applied = store.list_applied()
        assert len(applied) == 2
        assert all(a.success for a in applied)

    def test_apply_stops_on_first_failure_and_records_error(self) -> None:
        files = [make_migration_file(1), make_migration_file(2), make_migration_file(3)]
        store = InMemoryMigrationStateStore()

        def executor(sql: str, version: int) -> None:
            if version == 2:
                raise RuntimeError("DB connection failed")

        runner = Runner(store, executor, catalog="cat", schema="sch")
        with pytest.raises(RuntimeError, match="DB connection failed"):
            runner.apply(files)

        applied = store.list_applied()
        assert len(applied) == 2
        assert applied[0].version == 1
        assert applied[0].success is True
        assert applied[1].version == 2
        assert applied[1].success is False
        assert "DB connection failed" in (applied[1].error or "")

    def test_apply_checksum_mismatch_raises_MigrationChecksumMismatchError_before_execution(
        self,
    ) -> None:
        files = [make_migration_file(1, checksum="new_checksum")]
        store = InMemoryMigrationStateStore()
        store.record_applied(1, "test", "old_checksum", success=True)
        executed: list[int] = []

        def executor(sql: str, version: int) -> None:
            executed.append(version)

        runner = Runner(store, executor, catalog="cat", schema="sch")
        with pytest.raises(MigrationChecksumMismatchError):
            runner.apply(files)

        assert executed == []

    def test_apply_logs_each_migration(self, caplog: pytest.LogCaptureFixture) -> None:
        files = [make_migration_file(1, name="create_users")]
        store = InMemoryMigrationStateStore()

        def executor(sql: str, version: int) -> None:
            pass

        runner = Runner(store, executor, catalog="cat", schema="sch")
        with caplog.at_level(logging.INFO):
            runner.apply(files)

        assert any("V1__create_users" in record.message for record in caplog.records)

    def test_apply_no_pending_logs_up_to_date(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        files = [make_migration_file(1)]
        store = InMemoryMigrationStateStore()
        store.record_applied(1, "test", "abc123", success=True)

        def executor(sql: str, version: int) -> None:
            pass

        runner = Runner(store, executor, catalog="cat", schema="sch")
        with caplog.at_level(logging.INFO):
            runner.apply(files)

        assert any("up to date" in record.message.lower() for record in caplog.records)

    def test_dry_run_executes_nothing(self) -> None:
        files = [make_migration_file(1), make_migration_file(2)]
        store = InMemoryMigrationStateStore()
        executed: list[int] = []

        def executor(sql: str, version: int) -> None:
            executed.append(version)

        runner = Runner(store, executor, catalog="cat", schema="sch")
        runner.apply(files, dry_run=True)

        assert executed == []
        assert store.list_applied() == []


class TestVariableSubstitution:
    def test_substitutes_catalog_and_schema(self) -> None:
        sql = "CREATE TABLE ${catalog}.${schema}.users (id INT);"
        files = [make_migration_file(1, sql=sql)]
        store = InMemoryMigrationStateStore()
        executed_sql: list[str] = []

        def executor(sql: str, version: int) -> None:
            executed_sql.append(sql)

        runner = Runner(store, executor, catalog="my_catalog", schema="my_schema")
        runner.apply(files)

        assert executed_sql == ["CREATE TABLE my_catalog.my_schema.users (id INT);"]


class TestPendingMigration:
    def test_dataclass_fields(self) -> None:
        pm = PendingMigration(
            version=1,
            name="test",
            path=Path("/tmp/V1__test.sql"),
            checksum="abc",
            sql="SELECT 1;",
        )
        assert pm.version == 1
        assert pm.name == "test"
        assert pm.path == Path("/tmp/V1__test.sql")
        assert pm.checksum == "abc"
        assert pm.sql == "SELECT 1;"
