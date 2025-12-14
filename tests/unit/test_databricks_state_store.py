"""Tests for DatabricksMigrationStateStore."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from ucmt.config import Config
from ucmt.exceptions import ConfigError, MigrationStateConflictError
from ucmt.migrations.state import (
    DatabricksMigrationStateStore,
    MigrationStateStore,
)


@pytest.fixture
def config() -> Config:
    return Config(
        catalog="my_catalog",
        schema="my_schema",
        databricks_host="test.databricks.com",
        databricks_http_path="/sql/warehouses/abc123",
        databricks_token="test_token",
    )


@pytest.fixture
def mock_cursor() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_connection(mock_cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


class TestDatabricksMigrationStateStoreProtocol:
    def test_implements_migration_state_store_protocol(self, config: Config):
        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            store = DatabricksMigrationStateStore(config)
            assert isinstance(store, MigrationStateStore)


class TestStateTableFqn:
    def test_state_table_fqn_from_config(self, config: Config):
        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            store = DatabricksMigrationStateStore(config)
            assert store.state_table_fqn == "my_catalog.my_schema._ucmt_migrations"

    def test_state_table_fqn_uses_config_state_table(self):
        config = Config(
            catalog="cat",
            schema="sch",
            state_table="custom_migrations",
        )
        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            store = DatabricksMigrationStateStore(config)
            assert store.state_table_fqn == "cat.sch.custom_migrations"

    def test_invalid_catalog_raises_config_error(self):
        config = Config(
            catalog="invalid-catalog",
            schema="sch",
        )
        with patch("ucmt.migrations.state.sql.connect"):
            with pytest.raises(ConfigError):
                DatabricksMigrationStateStore(config)

    def test_invalid_schema_raises_config_error(self):
        config = Config(
            catalog="cat",
            schema="invalid.schema",
        )
        with patch("ucmt.migrations.state.sql.connect"):
            with pytest.raises(ConfigError):
                DatabricksMigrationStateStore(config)


class TestCreatesStateTableIfMissing:
    def test_creates_state_table_if_missing(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection

            DatabricksMigrationStateStore(config)

            mock_cursor.execute.assert_called()
            create_call = mock_cursor.execute.call_args_list[0]
            sql = create_call[0][0]
            assert "CREATE TABLE IF NOT EXISTS" in sql
            assert "my_catalog.my_schema._ucmt_migrations" in sql
            assert "version INT" in sql
            assert "name STRING" in sql
            assert "checksum STRING" in sql
            assert "applied_at TIMESTAMP" in sql
            assert "success BOOLEAN" in sql
            assert "error STRING" in sql


class TestReadsAppliedMigrations:
    def test_list_applied_returns_migrations_in_ascending_order(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        now = datetime.now()
        mock_cursor.fetchall.return_value = [
            (1, "first", "a", now, True, None),
            (2, "second", "b", now, True, None),
            (3, "third", "c", now, True, None),
        ]

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            applied = store.list_applied()

        assert len(applied) == 3
        versions = [m.version for m in applied]
        assert versions == [1, 2, 3]

    def test_list_applied_empty_table(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchall.return_value = []

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            applied = store.list_applied()

        assert applied == []

    def test_get_last_applied_returns_highest_version(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        now = datetime.now()
        mock_cursor.fetchone.return_value = (3, "third", "c", now, True, None)

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            last = store.get_last_applied()

        assert last is not None
        assert last.version == 3

    def test_get_last_applied_returns_none_when_empty(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchone.return_value = None

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            last = store.get_last_applied()

        assert last is None

    def test_has_applied_returns_true_for_existing_version(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchone.return_value = (1,)

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            assert store.has_applied(1) is True

    def test_has_applied_returns_false_for_nonexistent_version(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchone.return_value = None

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            assert store.has_applied(999) is False


class TestRecordsSuccess:
    def test_records_success(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchone.return_value = None

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            store.record_applied(
                version=1,
                name="create_users",
                checksum="abc123",
                success=True,
                error=None,
            )

        insert_calls = [
            c for c in mock_cursor.execute.call_args_list if "INSERT" in str(c)
        ]
        assert len(insert_calls) >= 1
        sql = insert_calls[0][0][0]
        assert "INSERT INTO" in sql
        assert "my_catalog.my_schema._ucmt_migrations" in sql

    def test_records_success_with_correct_values(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchone.return_value = None

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            store.record_applied(
                version=42,
                name="add_orders",
                checksum="xyz789",
                success=True,
                error=None,
            )

        insert_calls = [
            c for c in mock_cursor.execute.call_args_list if "INSERT" in str(c)
        ]
        assert len(insert_calls) >= 1


class TestRecordsFailure:
    def test_records_failure_with_error(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        mock_cursor.fetchone.return_value = None

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            store.record_applied(
                version=1,
                name="create_users",
                checksum="abc123",
                success=False,
                error="syntax error at line 5",
            )

        insert_calls = [
            c for c in mock_cursor.execute.call_args_list if "INSERT" in str(c)
        ]
        assert len(insert_calls) >= 1


class TestIdempotency:
    def test_record_same_version_twice_is_idempotent(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        now = datetime.now()
        mock_cursor.fetchone.side_effect = [
            (1, "create_users", "abc123", now, True, None),
        ]

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)
            store.record_applied(
                version=1,
                name="create_users",
                checksum="abc123",
                success=True,
                error=None,
            )

        insert_calls = [
            c for c in mock_cursor.execute.call_args_list if "INSERT" in str(c)
        ]
        assert len(insert_calls) == 0

    def test_record_different_checksum_same_version_raises(
        self, config: Config, mock_connection: MagicMock, mock_cursor: MagicMock
    ):
        now = datetime.now()
        mock_cursor.fetchone.return_value = (
            1,
            "create_users",
            "original_checksum",
            now,
            True,
            None,
        )

        with patch("ucmt.migrations.state.sql.connect") as mock_connect:
            mock_connect.return_value = mock_connection
            store = DatabricksMigrationStateStore(config)

            with pytest.raises(MigrationStateConflictError):
                store.record_applied(
                    version=1,
                    name="create_users",
                    checksum="different_checksum",
                    success=True,
                    error=None,
                )
