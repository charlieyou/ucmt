"""Tests for CLI commands including --online flag."""

import argparse
from unittest.mock import patch

from ucmt.cli import cmd_diff, cmd_generate
from ucmt.schema.models import Schema


class TestCmdDiffOffline:
    """Test cmd_diff in offline mode (default)."""

    def test_diff_offline_compares_against_empty_schema(self, tmp_path):
        """Offline mode should compare declared schema against empty schema."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(schema_path=schema_dir, online=False)

        with patch("builtins.print") as mock_print:
            result = cmd_diff(args)

        assert result == 0
        mock_print.assert_any_call("Found 1 changes (offline mode):")

    def test_diff_offline_no_changes_when_empty_schema(self, tmp_path):
        """Offline mode with empty schema dir should show no changes."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()

        args = argparse.Namespace(schema_path=schema_dir, online=False)

        with patch("builtins.print") as mock_print:
            result = cmd_diff(args)

        assert result == 0
        mock_print.assert_called_with("No changes detected")


class TestCmdDiffOnline:
    """Test cmd_diff in online mode."""

    def test_diff_online_requires_db_config(self, tmp_path):
        """Online mode should fail if DB config is missing."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(schema_path=schema_dir, online=True)

        with patch.dict("os.environ", {}, clear=True):
            with patch("builtins.print"):
                result = cmd_diff(args)

        assert result == 1

    def test_diff_online_uses_get_current_schema_online(self, tmp_path):
        """Online mode should use _get_current_schema_online helper."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(schema_path=schema_dir, online=True)

        mock_schema = Schema(tables={})

        with (
            patch(
                "ucmt.cli._get_current_schema_online", return_value=mock_schema
            ) as mock_get,
            patch("builtins.print") as mock_print,
        ):
            result = cmd_diff(args)

        mock_get.assert_called_once()
        assert result == 0
        mock_print.assert_any_call("Found 1 changes (online mode):")

    def test_diff_online_returns_1_when_introspection_fails(self, tmp_path):
        """Online mode should return 1 if _get_current_schema_online returns None."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(schema_path=schema_dir, online=True)

        with patch("ucmt.cli._get_current_schema_online", return_value=None):
            result = cmd_diff(args)

        assert result == 1


class TestCmdGenerateOffline:
    """Test cmd_generate in offline mode."""

    def test_generate_offline_compares_against_empty_schema(self, tmp_path):
        """Offline mode should generate migration comparing against empty schema."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(
            schema_path=schema_dir, online=False, description="Add users table"
        )

        with patch("builtins.print") as mock_print:
            result = cmd_generate(args)

        assert result == 0
        printed_output = " ".join(str(call) for call in mock_print.call_args_list)
        assert "CREATE TABLE" in printed_output


class TestCmdGenerateOnline:
    """Test cmd_generate in online mode."""

    def test_generate_online_requires_db_config(self, tmp_path):
        """Online mode should fail if DB config is missing."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(
            schema_path=schema_dir, online=True, description="Add users table"
        )

        with patch.dict("os.environ", {}, clear=True):
            with patch("builtins.print"):
                result = cmd_generate(args)

        assert result == 1

    def test_generate_online_uses_get_current_schema_online(self, tmp_path):
        """Online mode should use _get_current_schema_online helper."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "users.yaml").write_text(
            """
table: users
columns:
  - name: id
    type: BIGINT
"""
        )

        args = argparse.Namespace(
            schema_path=schema_dir, online=True, description="Add users table"
        )

        mock_schema = Schema(tables={})

        with (
            patch(
                "ucmt.cli._get_current_schema_online", return_value=mock_schema
            ) as mock_get,
            patch("builtins.print") as mock_print,
        ):
            result = cmd_generate(args)

        mock_get.assert_called_once()
        assert result == 0
        printed_output = " ".join(str(call) for call in mock_print.call_args_list)
        assert "CREATE TABLE" in printed_output


class TestCmdStatus:
    """Test cmd_status displays failed migrations correctly."""

    def test_status_shows_failed_migrations(self, tmp_path):
        """Status should show ✗ failed for migrations with success=False."""
        from datetime import datetime
        from unittest.mock import MagicMock

        from ucmt.cli import cmd_status
        from ucmt.migrations.state import AppliedMigration

        migrations_dir = tmp_path / "sql" / "migrations"
        migrations_dir.mkdir(parents=True)
        (migrations_dir / "V001__create_users.sql").write_text(
            "CREATE TABLE users (id INT);"
        )
        (migrations_dir / "V002__add_email.sql").write_text(
            "ALTER TABLE users ADD email STRING;"
        )

        args = argparse.Namespace(migrations_path=migrations_dir)

        mock_state_store = MagicMock()
        mock_state_store.__enter__ = MagicMock(return_value=mock_state_store)
        mock_state_store.__exit__ = MagicMock(return_value=False)
        mock_state_store.list_applied.return_value = [
            AppliedMigration(
                version=1,
                name="create_users",
                checksum="abc",
                applied_at=datetime.now(),
                success=True,
            ),
            AppliedMigration(
                version=2,
                name="add_email",
                checksum="def",
                applied_at=datetime.now(),
                success=False,
                error="syntax error",
            ),
        ]

        with (
            patch(
                "ucmt.migrations.state.DatabricksMigrationStateStore",
                return_value=mock_state_store,
            ),
            patch("ucmt.cli.Config.from_env"),
            patch("ucmt.cli._validate_db_config", return_value=True),
            patch("builtins.print") as mock_print,
        ):
            result = cmd_status(args)

        assert result == 0
        printed_calls = [str(call) for call in mock_print.call_args_list]
        printed_output = " ".join(printed_calls)
        assert "V2" in printed_output and "✗ failed" in printed_output
