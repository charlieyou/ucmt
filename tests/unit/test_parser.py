"""Tests for migration file parser."""

import hashlib
from pathlib import Path

import pytest

from ucmt.exceptions import MigrationParseError
from ucmt.migrations.parser import (
    MigrationFile,
    parse_migration_file,
    parse_migrations_dir,
)


def _sha256(content: str) -> str:
    """Compute SHA256 of content with normalized line endings."""
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class TestParseFilename:
    def test_parser_filename_extracts_version_and_name(self, tmp_path: Path):
        """Test that V###__name.sql extracts version as int and name."""
        sql_file = tmp_path / "V001__create_users.sql"
        sql_file.write_text("CREATE TABLE users (id BIGINT);")

        migration = parse_migration_file(sql_file)

        assert migration.version == 1
        assert migration.name == "create_users"
        assert migration.path == sql_file
        assert migration.sql == "CREATE TABLE users (id BIGINT);"

    def test_parser_unpadded_version_allowed(self, tmp_path: Path):
        """Test that unpadded versions like V1 are allowed."""
        sql_file = tmp_path / "V1__initial.sql"
        sql_file.write_text("SELECT 1;")

        migration = parse_migration_file(sql_file)

        assert migration.version == 1
        assert migration.name == "initial"

    def test_parser_large_version_number(self, tmp_path: Path):
        """Test that large version numbers work."""
        sql_file = tmp_path / "V9999__big_version.sql"
        sql_file.write_text("SELECT 1;")

        migration = parse_migration_file(sql_file)

        assert migration.version == 9999
        assert migration.name == "big_version"

    def test_parser_invalid_filename_raises_MigrationParseError(self, tmp_path: Path):
        """Test that invalid filenames raise MigrationParseError."""
        # Missing V prefix
        sql_file = tmp_path / "001__create_users.sql"
        sql_file.write_text("SELECT 1;")

        with pytest.raises(MigrationParseError, match="(?i)invalid.*filename"):
            parse_migration_file(sql_file)

    def test_parser_invalid_filename_no_double_underscore(self, tmp_path: Path):
        """Test that missing double underscore raises MigrationParseError."""
        sql_file = tmp_path / "V001_create_users.sql"
        sql_file.write_text("SELECT 1;")

        with pytest.raises(MigrationParseError, match="(?i)invalid.*filename"):
            parse_migration_file(sql_file)

    def test_parser_invalid_filename_no_version(self, tmp_path: Path):
        """Test that missing version number raises MigrationParseError."""
        sql_file = tmp_path / "V__create_users.sql"
        sql_file.write_text("SELECT 1;")

        with pytest.raises(MigrationParseError, match="(?i)invalid.*filename"):
            parse_migration_file(sql_file)

    def test_parser_invalid_filename_no_name(self, tmp_path: Path):
        """Test that missing name raises MigrationParseError."""
        sql_file = tmp_path / "V001__.sql"
        sql_file.write_text("SELECT 1;")

        with pytest.raises(MigrationParseError, match="(?i)invalid.*filename"):
            parse_migration_file(sql_file)


class TestChecksum:
    def test_parser_checksum_sha256_deterministic(self, tmp_path: Path):
        """Test that checksum is SHA256 and deterministic."""
        sql_content = "CREATE TABLE users (id BIGINT);"
        sql_file = tmp_path / "V001__create_users.sql"
        sql_file.write_text(sql_content)

        migration = parse_migration_file(sql_file)

        expected_checksum = _sha256(sql_content)
        assert migration.checksum == expected_checksum

    def test_parser_checksum_normalizes_line_endings(self, tmp_path: Path):
        """Test that checksum normalizes CRLF to LF."""
        sql_content_lf = "SELECT 1;\nSELECT 2;\n"
        sql_content_crlf = "SELECT 1;\r\nSELECT 2;\r\n"

        file_lf = tmp_path / "V001__lf.sql"
        file_crlf = tmp_path / "V002__crlf.sql"

        file_lf.write_bytes(sql_content_lf.encode("utf-8"))
        file_crlf.write_bytes(sql_content_crlf.encode("utf-8"))

        migration_lf = parse_migration_file(file_lf)
        migration_crlf = parse_migration_file(file_crlf)

        assert migration_lf.checksum == migration_crlf.checksum


class TestSqlContent:
    def test_parser_multiple_statements_preserved_as_raw_text(self, tmp_path: Path):
        """Test that multiple SQL statements are preserved as raw text."""
        sql_content = """CREATE TABLE users (id BIGINT);
CREATE TABLE orders (id BIGINT);
INSERT INTO users VALUES (1);"""
        sql_file = tmp_path / "V001__multi.sql"
        sql_file.write_text(sql_content)

        migration = parse_migration_file(sql_file)

        assert migration.sql == sql_content

    def test_parser_preserves_variable_placeholders(self, tmp_path: Path):
        """Test that variable placeholders like ${var} are preserved."""
        sql_content = "CREATE TABLE ${schema}.users (id BIGINT);"
        sql_file = tmp_path / "V001__with_vars.sql"
        sql_file.write_text(sql_content)

        migration = parse_migration_file(sql_file)

        assert "${schema}" in migration.sql
        assert migration.sql == sql_content

    def test_parser_empty_file_raises_MigrationParseError(self, tmp_path: Path):
        """Test that empty files raise MigrationParseError."""
        sql_file = tmp_path / "V001__empty.sql"
        sql_file.write_text("")

        with pytest.raises(MigrationParseError, match="(?i)empty"):
            parse_migration_file(sql_file)

    def test_parser_whitespace_only_file_raises_MigrationParseError(
        self, tmp_path: Path
    ):
        """Test that whitespace-only files raise MigrationParseError."""
        sql_file = tmp_path / "V001__whitespace.sql"
        sql_file.write_text("   \n\t\n  ")

        with pytest.raises(MigrationParseError, match="(?i)empty"):
            parse_migration_file(sql_file)


class TestParseDirectory:
    def test_parser_sorts_by_numeric_version(self, tmp_path: Path):
        """Test that migrations are sorted by numeric version."""
        (tmp_path / "V010__tenth.sql").write_text("SELECT 10;")
        (tmp_path / "V2__second.sql").write_text("SELECT 2;")
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")

        migrations = parse_migrations_dir(tmp_path)

        assert len(migrations) == 3
        assert migrations[0].version == 1
        assert migrations[1].version == 2
        assert migrations[2].version == 10

    def test_parser_rejects_duplicate_versions_raises_MigrationParseError(
        self, tmp_path: Path
    ):
        """Test that duplicate versions raise MigrationParseError."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V001__duplicate.sql").write_text("SELECT 2;")

        with pytest.raises(MigrationParseError, match="(?i)duplicate.*version"):
            parse_migrations_dir(tmp_path)

    def test_parser_missing_version_gap_allowed(self, tmp_path: Path):
        """Test that gaps in version numbers are allowed."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V005__fifth.sql").write_text("SELECT 5;")
        (tmp_path / "V010__tenth.sql").write_text("SELECT 10;")

        migrations = parse_migrations_dir(tmp_path)

        assert len(migrations) == 3
        assert migrations[0].version == 1
        assert migrations[1].version == 5
        assert migrations[2].version == 10

    def test_parser_empty_directory_returns_empty_list(self, tmp_path: Path):
        """Test that empty directory returns empty list."""
        migrations = parse_migrations_dir(tmp_path)

        assert migrations == []

    def test_parser_ignores_non_sql_files(self, tmp_path: Path):
        """Test that non-.sql files are ignored."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "README.md").write_text("# Migrations")
        (tmp_path / "V002__second.txt").write_text("not sql")

        migrations = parse_migrations_dir(tmp_path)

        assert len(migrations) == 1
        assert migrations[0].version == 1


class TestMigrationFileDataclass:
    def test_parser_rejects_string_version(self):
        """Test that passing string version to MigrationFile raises TypeError."""
        with pytest.raises(TypeError):
            MigrationFile(
                version="1",  # type: ignore
                name="test",
                path=Path("/tmp/test.sql"),
                checksum="abc123",
                sql="SELECT 1;",
            )

    def test_migration_file_immutable(self, tmp_path: Path):
        """Test that MigrationFile is immutable (frozen dataclass)."""
        sql_file = tmp_path / "V001__test.sql"
        sql_file.write_text("SELECT 1;")

        migration = parse_migration_file(sql_file)

        with pytest.raises(AttributeError):
            migration.version = 2  # type: ignore
