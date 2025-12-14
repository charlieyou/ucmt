from datetime import datetime

import pytest

from ucmt.exceptions import MigrationStateConflictError
from ucmt.migrations.state import AppliedMigration, InMemoryMigrationStateStore


class TestAppliedMigration:
    def test_applied_migration_dataclass(self):
        now = datetime.now()
        m = AppliedMigration(
            version=1,
            name="create_users",
            checksum="abc123",
            applied_at=now,
            success=True,
            error=None,
        )
        assert m.version == 1
        assert m.name == "create_users"
        assert m.checksum == "abc123"
        assert m.applied_at == now
        assert m.success is True
        assert m.error is None


class TestInMemoryMigrationStateStore:
    def test_list_applied_returns_ascending_order(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(version=3, name="third", checksum="c", success=True)
        store.record_applied(version=1, name="first", checksum="a", success=True)
        store.record_applied(version=2, name="second", checksum="b", success=True)

        applied = store.list_applied()
        versions = [m.version for m in applied]
        assert versions == [1, 2, 3]

    def test_record_applied_success(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=True
        )

        applied = store.list_applied()
        assert len(applied) == 1
        assert applied[0].version == 1
        assert applied[0].name == "create_users"
        assert applied[0].checksum == "abc"
        assert applied[0].success is True
        assert applied[0].error is None

    def test_record_applied_failure_with_error(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1,
            name="create_users",
            checksum="abc",
            success=False,
            error="syntax error",
        )

        applied = store.list_applied()
        assert len(applied) == 1
        assert applied[0].success is False
        assert applied[0].error == "syntax error"

    def test_get_last_applied(self):
        store = InMemoryMigrationStateStore()
        assert store.get_last_applied() is None

        store.record_applied(version=1, name="first", checksum="a", success=True)
        store.record_applied(version=3, name="third", checksum="c", success=True)
        store.record_applied(version=2, name="second", checksum="b", success=True)

        last = store.get_last_applied()
        assert last is not None
        assert last.version == 3

    def test_has_applied(self):
        store = InMemoryMigrationStateStore()
        assert store.has_applied(1) is False

        store.record_applied(version=1, name="first", checksum="a", success=True)
        assert store.has_applied(1) is True
        assert store.has_applied(2) is False

    def test_record_same_version_twice_is_idempotent(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=True
        )
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=True
        )

        applied = store.list_applied()
        assert len(applied) == 1

    def test_record_different_checksum_same_version_raises_MigrationStateConflictError(
        self,
    ):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=True
        )

        with pytest.raises(MigrationStateConflictError):
            store.record_applied(
                version=1, name="create_users", checksum="different", success=True
            )

    def test_has_applied_returns_true_for_failed_migration(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=False, error="boom"
        )

        assert store.has_applied(1) is True

    def test_record_failed_migration_twice_is_idempotent(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=False, error="boom"
        )
        store.record_applied(
            version=1,
            name="create_users",
            checksum="abc",
            success=False,
            error="still boom",
        )

        applied = store.list_applied()
        assert len(applied) == 1
        assert applied[0].error == "boom"
        assert applied[0].success is False

    def test_record_same_checksum_different_success_does_not_overwrite(self):
        store = InMemoryMigrationStateStore()
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=False, error="boom"
        )
        store.record_applied(
            version=1, name="create_users", checksum="abc", success=True, error=None
        )

        applied = store.list_applied()
        assert len(applied) == 1
        assert applied[0].success is False
        assert applied[0].error == "boom"
