"""TDD tests for CLI module - tests written FIRST per ucmt-fy5."""

import argparse
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from ucmt.exceptions import ConfigError
from ucmt.migrations.state import AppliedMigration


class TestCliHelp:
    """Test CLI help and command discovery."""

    def test_cli_help_shows_all_commands(self, capsys):
        """Help output should list all available commands."""
        from ucmt.cli import main

        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["ucmt", "--help"]):
                main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        output = captured.out

        assert "validate" in output
        assert "generate" in output
        assert "diff" in output
        assert "status" in output
        assert "run" in output or "apply" in output


class TestCliApply:
    """Test migrations apply command."""

    def test_cli_apply_invokes_runner(self, tmp_path):
        """Apply command should invoke the migration runner."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        (migrations_dir / "V001__init.sql").write_text("CREATE TABLE test (id INT);")

        from ucmt.cli import cmd_run

        mock_state_store = MagicMock()
        mock_state_store.has_applied.return_value = False
        mock_state_store.list_applied.return_value = []

        args = argparse.Namespace(
            migrations_path=migrations_dir,
            dry_run=False,
            allow_destructive=False,
        )

        with (
            patch("ucmt.cli._validate_db_config", return_value=True),
            patch("ucmt.cli.Config.from_env") as mock_config,
            patch(
                "ucmt.migrations.state.DatabricksMigrationStateStore",
                return_value=mock_state_store,
            ),
            patch("ucmt.cli.DatabricksClient") as mock_client_cls,
        ):
            mock_config.return_value.catalog = "test_catalog"
            mock_config.return_value.schema = "test_schema"
            mock_config.return_value.databricks_host = "host"
            mock_config.return_value.databricks_token = "token"
            mock_config.return_value.databricks_http_path = "/sql/1.0"

            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = cmd_run(args)

        assert result == 0
        mock_client.execute.assert_called()


class TestCliGenerate:
    """Test migrations generate command."""

    def test_cli_generate_creates_migration_file(self, tmp_path, capsys):
        """Generate command should output migration SQL."""
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

        from ucmt.cli import cmd_generate

        args = argparse.Namespace(
            schema_path=schema_dir,
            online=False,
            description="add_users_table",
            output=None,
            allow_destructive=False,
        )

        result = cmd_generate(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "CREATE TABLE" in captured.out


class TestCliValidate:
    """Test schema validate command."""

    def test_cli_validate_exits_nonzero_on_drift(self, tmp_path):
        """Validate should exit non-zero when schema has errors."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "invalid.yaml").write_text("not: valid: yaml: {{")

        from ucmt.cli import cmd_validate

        args = argparse.Namespace(schema_path=schema_dir)
        result = cmd_validate(args)

        assert result == 1


class TestCliDiff:
    """Test diff command."""

    def test_cli_diff_prints_changes_no_execution(self, tmp_path, capsys):
        """Diff should print changes without executing anything."""
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

        from ucmt.cli import cmd_diff

        args = argparse.Namespace(schema_path=schema_dir, online=False)
        result = cmd_diff(args)

        assert result == 0
        captured = capsys.readouterr()
        assert (
            "changes" in captured.out.lower() or "create_table" in captured.out.lower()
        )


class TestCliPlan:
    """Test migrations plan command."""

    def test_cli_plan_lists_pending(self, tmp_path, capsys):
        """Plan should list pending migrations."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        (migrations_dir / "V001__init.sql").write_text("CREATE TABLE t (id INT);")
        (migrations_dir / "V002__add_col.sql").write_text("ALTER TABLE t ADD col INT;")

        from ucmt.cli import cmd_plan

        mock_state_store = MagicMock()
        mock_state_store.list_applied.return_value = []
        mock_state_store.has_applied.return_value = False

        args = argparse.Namespace(migrations_path=migrations_dir)

        with (
            patch("ucmt.cli._validate_db_config", return_value=True),
            patch("ucmt.cli.Config.from_env"),
            patch(
                "ucmt.migrations.state.DatabricksMigrationStateStore",
                return_value=mock_state_store,
            ),
        ):
            result = cmd_plan(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "V1" in captured.out or "001" in captured.out
        assert "V2" in captured.out or "002" in captured.out


class TestCliStatus:
    """Test status command."""

    def test_cli_status_shows_applied_and_pending(self, tmp_path, capsys):
        """Status should show both applied and pending migrations."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        (migrations_dir / "V001__init.sql").write_text("CREATE TABLE t (id INT);")
        (migrations_dir / "V002__add_col.sql").write_text("ALTER TABLE t ADD col INT;")

        from ucmt.cli import cmd_status

        mock_state_store = MagicMock()
        mock_state_store.list_applied.return_value = [
            AppliedMigration(
                version=1,
                name="init",
                checksum="abc",
                applied_at=datetime.now(),
                success=True,
            )
        ]

        args = argparse.Namespace(migrations_path=migrations_dir)

        with (
            patch("ucmt.cli._validate_db_config", return_value=True),
            patch("ucmt.cli.Config.from_env"),
            patch(
                "ucmt.migrations.state.DatabricksMigrationStateStore",
                return_value=mock_state_store,
            ),
        ):
            result = cmd_status(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "applied" in captured.out.lower()
        assert "pending" in captured.out.lower()


class TestCliExitCodes:
    """Test CLI exit codes."""

    def test_cli_exits_0_on_success(self, tmp_path):
        """CLI should exit 0 on successful operations."""
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

        from ucmt.cli import cmd_validate

        args = argparse.Namespace(schema_path=schema_dir)
        result = cmd_validate(args)

        assert result == 0

    def test_cli_exits_1_on_error(self, tmp_path):
        """CLI should exit 1 on runtime errors."""
        schema_dir = tmp_path / "nonexistent"

        from ucmt.cli import cmd_validate

        args = argparse.Namespace(schema_path=schema_dir)
        result = cmd_validate(args)

        assert result == 1

    def test_cli_config_error_exits_2(self, tmp_path):
        """CLI should exit 2 on configuration errors."""
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        (migrations_dir / "V001__init.sql").write_text("CREATE TABLE t (id INT);")

        from ucmt.cli import cmd_run

        args = argparse.Namespace(
            migrations_path=migrations_dir,
            dry_run=False,
            allow_destructive=False,
        )

        with patch("ucmt.cli.Config.from_env") as mock_config:
            mock_config.side_effect = ConfigError("Missing catalog")
            result = cmd_run(args)

        assert result == 2

    def test_cli_validation_failure_exits_1(self, tmp_path):
        """CLI should exit 1 on validation failures."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()
        (schema_dir / "bad.yaml").write_text("invalid: yaml: {{{")

        from ucmt.cli import cmd_validate

        args = argparse.Namespace(schema_path=schema_dir)
        result = cmd_validate(args)

        assert result == 1


class TestCliConfigPrecedence:
    """Test configuration precedence."""

    def test_cli_respects_config_precedence(self):
        """CLI args should override environment variables."""
        from ucmt.config import Config

        with patch.dict(
            "os.environ",
            {"UCMT_CATALOG": "env_catalog", "UCMT_SCHEMA": "env_schema"},
        ):
            config = Config.from_env(catalog="cli_catalog")

        assert config.catalog == "cli_catalog"
        assert config.schema == "env_schema"


class TestCliDestructiveOperations:
    """Test destructive operation protection."""

    def test_cli_requires_allow_destructive_for_drops(self, tmp_path, capsys):
        """CLI should require --allow-destructive for DROP operations."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()

        from ucmt.cli import cmd_generate
        from ucmt.schema.diff import SchemaChange
        from ucmt.schema.models import Schema
        from ucmt.types import ChangeType

        args = argparse.Namespace(
            schema_path=schema_dir,
            online=False,
            description="drop_users_table",
            output=None,
            allow_destructive=False,
        )

        drop_change = SchemaChange(
            change_type=ChangeType.DROP_TABLE,
            table_name="users",
            is_destructive=True,
        )

        with (
            patch("ucmt.cli.load_schema", return_value=Schema(tables={})),
            patch("ucmt.cli.SchemaDiffer") as mock_differ_cls,
        ):
            mock_differ = MagicMock()
            mock_differ.diff.return_value = [drop_change]
            mock_differ_cls.return_value = mock_differ

            result = cmd_generate(args)

        assert result == 1
        captured = capsys.readouterr()
        assert (
            "destructive" in captured.err.lower()
            or "allow-destructive" in captured.err.lower()
        )

    def test_cli_allows_destructive_with_flag(self, tmp_path, capsys):
        """CLI should allow DROP operations with --allow-destructive flag."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()

        from ucmt.cli import cmd_generate
        from ucmt.schema.diff import SchemaChange
        from ucmt.schema.models import Schema
        from ucmt.types import ChangeType

        args = argparse.Namespace(
            schema_path=schema_dir,
            online=False,
            description="drop_users_table",
            output=None,
            allow_destructive=True,
        )

        drop_change = SchemaChange(
            change_type=ChangeType.DROP_TABLE,
            table_name="users",
            is_destructive=True,
        )

        with (
            patch("ucmt.cli.load_schema", return_value=Schema(tables={})),
            patch("ucmt.cli.SchemaDiffer") as mock_differ_cls,
            patch("ucmt.cli.MigrationGenerator") as mock_gen_cls,
            patch("ucmt.cli.Config.from_env"),
        ):
            mock_differ = MagicMock()
            mock_differ.diff.return_value = [drop_change]
            mock_differ_cls.return_value = mock_differ

            mock_gen = MagicMock()
            mock_gen.generate.return_value = "DROP TABLE users;"
            mock_gen_cls.return_value = mock_gen

            result = cmd_generate(args)

        assert result == 0

    def test_cli_detects_drop_column_as_destructive(self, tmp_path, capsys):
        """CLI should detect DROP COLUMN in SQL as destructive."""
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()

        from ucmt.cli import cmd_generate
        from ucmt.schema.diff import SchemaChange
        from ucmt.schema.models import Schema
        from ucmt.types import ChangeType

        args = argparse.Namespace(
            schema_path=schema_dir,
            online=False,
            description="drop_column",
            output=None,
            allow_destructive=False,
        )

        drop_col_change = SchemaChange(
            change_type=ChangeType.DROP_COLUMN,
            table_name="users",
            details={"column_name": "email"},
            is_destructive=True,
        )

        with (
            patch("ucmt.cli.load_schema", return_value=Schema(tables={})),
            patch("ucmt.cli.SchemaDiffer") as mock_differ_cls,
        ):
            mock_differ = MagicMock()
            mock_differ.diff.return_value = [drop_col_change]
            mock_differ_cls.return_value = mock_differ

            result = cmd_generate(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "destructive" in captured.err.lower()
