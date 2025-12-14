"""Tests for SQL migration code generation."""

from datetime import datetime

import pytest

from ucmt.exceptions import UnsupportedSchemaChangeError
from ucmt.schema.codegen import MigrationGenerator
from ucmt.schema.diff import SchemaChange
from ucmt.schema.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    PrimaryKey,
    Table,
)
from ucmt.types import ChangeType


@pytest.fixture
def generator() -> MigrationGenerator:
    """Create a generator with test catalog/schema."""
    return MigrationGenerator(catalog="test_catalog", schema="test_schema")


@pytest.fixture
def users_table() -> Table:
    """Create a sample users table."""
    return Table(
        name="users",
        columns=[
            Column(
                name="id",
                type="BIGINT",
                nullable=False,
                generated="ALWAYS AS IDENTITY",
            ),
            Column(name="email", type="STRING", nullable=False),
            Column(
                name="status",
                type="STRING",
                nullable=False,
                default="'active'",
                comment="User's current status",
            ),
            Column(name="created_at", type="TIMESTAMP", nullable=False),
        ],
        primary_key=PrimaryKey(columns=["id"], rely=True),
        liquid_clustering=["status", "created_at"],
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "delta.columnMapping.mode": "name",
        },
        comment="Core user accounts table",
    )


class TestCodegenCreateTable:
    """Tests for CREATE TABLE generation."""

    def test_codegen_create_table_golden(
        self, generator: MigrationGenerator, users_table: Table
    ):
        """Test CREATE TABLE generates expected SQL structure."""
        change = SchemaChange(
            change_type=ChangeType.CREATE_TABLE,
            table_name="users",
            details={"table": users_table},
        )

        result = generator.generate([change], "Create users table")

        # Check header
        assert "-- Migration: Auto-generated" in result
        assert "-- Description: Create users table" in result
        assert "-- Generated:" in result

        # Check table structure
        assert "CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.users" in result
        assert "id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL" in result
        assert "email STRING NOT NULL" in result
        assert (
            "status STRING NOT NULL DEFAULT 'active' COMMENT 'User''s current status'"
            in result
        )
        assert "created_at TIMESTAMP NOT NULL" in result
        assert "USING DELTA" in result

        # Check primary key
        assert "CONSTRAINT pk_users PRIMARY KEY (id) RELY" in result

        # Check clustering
        assert "CLUSTER BY (status, created_at)" in result

        # Check properties
        assert "TBLPROPERTIES" in result
        assert "'delta.enableChangeDataFeed' = 'true'" in result
        assert "'delta.columnMapping.mode' = 'name'" in result

        # Check comment
        assert "COMMENT 'Core user accounts table'" in result

    def test_codegen_primary_key_syntax(self, generator: MigrationGenerator):
        """Test primary key SQL syntax with RELY/NORELY."""
        table_rely = Table(
            name="t1",
            columns=[Column(name="id", type="BIGINT", nullable=False)],
            primary_key=PrimaryKey(columns=["id", "code"], rely=True),
        )
        table_norely = Table(
            name="t2",
            columns=[Column(name="id", type="BIGINT", nullable=False)],
            primary_key=PrimaryKey(columns=["id"], rely=False),
        )

        change_rely = SchemaChange(
            change_type=ChangeType.CREATE_TABLE,
            table_name="t1",
            details={"table": table_rely},
        )
        change_norely = SchemaChange(
            change_type=ChangeType.CREATE_TABLE,
            table_name="t2",
            details={"table": table_norely},
        )

        result_rely = generator.generate([change_rely], "test")
        result_norely = generator.generate([change_norely], "test")

        assert "PRIMARY KEY (id, code) RELY" in result_rely
        assert "PRIMARY KEY (id) NORELY" in result_norely


class TestCodegenColumnOperations:
    """Tests for ADD/DROP/ALTER column generation."""

    def test_codegen_add_column(self, generator: MigrationGenerator):
        """Test ADD COLUMN generates correct SQL."""
        column = Column(
            name="phone",
            type="STRING",
            nullable=True,
            comment="User phone number",
        )
        change = SchemaChange(
            change_type=ChangeType.ADD_COLUMN,
            table_name="users",
            details={"column": column},
        )

        result = generator.generate([change], "Add phone column")

        assert (
            "ALTER TABLE ${catalog}.${schema}.users ADD COLUMN IF NOT EXISTS "
            "phone STRING COMMENT 'User phone number';"
        ) in result

    def test_codegen_drop_column_marks_destructive(self, generator: MigrationGenerator):
        """Test DROP COLUMN includes destructive warning."""
        change = SchemaChange(
            change_type=ChangeType.DROP_COLUMN,
            table_name="users",
            details={"column_name": "phone"},
            is_destructive=True,
            requires_column_mapping=True,
        )

        result = generator.generate([change], "Drop phone column")

        assert "-- WARNING: This migration contains destructive changes:" in result
        assert "drop_column: users" in result
        assert "-- Requires: delta.columnMapping.mode = 'name'" in result
        assert (
            "ALTER TABLE ${catalog}.${schema}.users DROP COLUMN IF EXISTS phone;"
            in result
        )

    def test_codegen_alter_column_type(self, generator: MigrationGenerator):
        """Test ALTER COLUMN TYPE generates correct SQL."""
        change = SchemaChange(
            change_type=ChangeType.ALTER_COLUMN_TYPE,
            table_name="users",
            details={
                "column_name": "amount",
                "from_type": "INT",
                "to_type": "BIGINT",
            },
        )

        result = generator.generate([change], "Widen amount column")

        assert (
            "ALTER TABLE ${catalog}.${schema}.users ALTER COLUMN amount TYPE BIGINT;"
            in result
        )


class TestCodegenTableProperties:
    """Tests for table property operations."""

    def test_codegen_alter_table_properties(self, generator: MigrationGenerator):
        """Test ALTER TABLE SET TBLPROPERTIES generates correct SQL."""
        change = SchemaChange(
            change_type=ChangeType.ALTER_TABLE_PROPERTIES,
            table_name="users",
            details={
                "properties": {
                    "delta.enableChangeDataFeed": "true",
                    "custom.prop": "value",
                }
            },
        )

        result = generator.generate([change], "Update table properties")

        assert "ALTER TABLE ${catalog}.${schema}.users SET TBLPROPERTIES" in result
        assert "'delta.enableChangeDataFeed' = 'true'" in result
        assert "'custom.prop' = 'value'" in result


class TestCodegenConstraints:
    """Tests for constraint generation."""

    def test_codegen_add_check_constraint(self, generator: MigrationGenerator):
        """Test ADD CHECK constraint generates correct SQL."""
        constraint = CheckConstraint(
            name="chk_status",
            expression="status IN ('active', 'suspended', 'deleted')",
        )
        change = SchemaChange(
            change_type=ChangeType.ADD_CHECK_CONSTRAINT,
            table_name="users",
            details={"constraint": constraint},
        )

        result = generator.generate([change], "Add status check")

        assert (
            "ALTER TABLE ${catalog}.${schema}.users ADD CONSTRAINT chk_status "
            "CHECK (status IN ('active', 'suspended', 'deleted'));"
        ) in result

    def test_codegen_drop_check_constraint(self, generator: MigrationGenerator):
        """Test DROP CHECK constraint generates correct SQL."""
        change = SchemaChange(
            change_type=ChangeType.DROP_CHECK_CONSTRAINT,
            table_name="users",
            details={"constraint_name": "chk_status"},
        )

        result = generator.generate([change], "Drop status check")

        assert (
            "ALTER TABLE ${catalog}.${schema}.users DROP CONSTRAINT IF EXISTS chk_status;"
            in result
        )


class TestCodegenFQN:
    """Tests for fully-qualified name generation."""

    def test_codegen_fqn_uses_dollar_braces(self, generator: MigrationGenerator):
        """Test FQN uses ${catalog}.${schema} variable syntax."""
        column = Column(name="test_col", type="STRING", nullable=True)
        change = SchemaChange(
            change_type=ChangeType.ADD_COLUMN,
            table_name="users",
            details={"column": column},
        )

        result = generator.generate([change], "test")

        assert "${catalog}.${schema}.users" in result
        # Ensure no literal catalog/schema values
        assert "test_catalog.test_schema" not in result


class TestCodegenDestructiveWarning:
    """Tests for destructive change warnings."""

    def test_codegen_destructive_warning_header(self, generator: MigrationGenerator):
        """Test destructive changes include warning header."""
        changes = [
            SchemaChange(
                change_type=ChangeType.DROP_TABLE,
                table_name="old_table",
                is_destructive=True,
            ),
            SchemaChange(
                change_type=ChangeType.DROP_COLUMN,
                table_name="users",
                details={"column_name": "legacy_field"},
                is_destructive=True,
            ),
        ]

        result = generator.generate(changes, "Cleanup")

        assert "-- WARNING: This migration contains destructive changes:" in result
        assert "drop_table: old_table" in result
        assert "drop_column: users" in result


class TestCodegenUnsupported:
    """Tests for unsupported change handling."""

    def test_codegen_unsupported_change_raises_UnsupportedSchemaChangeError(
        self, generator: MigrationGenerator
    ):
        """Test unsupported changes raise UnsupportedSchemaChangeError."""
        change = SchemaChange(
            change_type=ChangeType.ALTER_PARTITIONING,
            table_name="users",
            details={
                "from_columns": ["date"],
                "to_columns": ["region"],
            },
            is_unsupported=True,
            error_message="Cannot change partitioning",
        )

        with pytest.raises(UnsupportedSchemaChangeError):
            generator.generate([change], "Change partition")


class TestCodegenEmptyChanges:
    """Tests for empty change list."""

    def test_codegen_empty_changes_returns_header_only(
        self, generator: MigrationGenerator
    ):
        """Test empty changes list returns header only."""
        result = generator.generate([], "No changes")

        assert "-- Migration: Auto-generated" in result
        assert "-- Description: No changes" in result
        assert "-- Generated:" in result
        # No SQL statements
        assert "ALTER" not in result
        assert "CREATE" not in result
        assert "DROP" not in result


class TestCodegenEscaping:
    """Tests for SQL string escaping."""

    def test_codegen_escapes_single_quotes(self, generator: MigrationGenerator):
        """Test single quotes in strings are properly escaped."""
        table = Table(
            name="test_table",
            columns=[
                Column(
                    name="description",
                    type="STRING",
                    nullable=True,
                    comment="User's personal O'Brien note",
                )
            ],
            comment="Table for user's data with O'Brien quotes",
        )
        change = SchemaChange(
            change_type=ChangeType.CREATE_TABLE,
            table_name="test_table",
            details={"table": table},
        )

        result = generator.generate([change], "test")

        # SQL escapes single quotes by doubling them
        assert "User''s personal O''Brien note" in result
        assert "user''s data with O''Brien quotes" in result


class TestCodegenTableComment:
    """Tests for table comment handling."""

    def test_codegen_uses_table_comment_not_description(
        self, generator: MigrationGenerator
    ):
        """Test codegen uses Table.comment field, not description."""
        table = Table(
            name="test_table",
            columns=[Column(name="id", type="BIGINT", nullable=False)],
            comment="This is the SQL comment",
        )
        change = SchemaChange(
            change_type=ChangeType.CREATE_TABLE,
            table_name="test_table",
            details={"table": table},
        )

        result = generator.generate([change], "test")

        assert "COMMENT 'This is the SQL comment'" in result


class TestCodegenForeignKey:
    """Tests for foreign key handling."""

    def test_codegen_no_foreign_key_sql(self, generator: MigrationGenerator):
        """Test foreign key columns don't generate FK constraint SQL."""
        table = Table(
            name="orders",
            columns=[
                Column(name="id", type="BIGINT", nullable=False),
                Column(
                    name="user_id",
                    type="BIGINT",
                    nullable=False,
                    foreign_key=ForeignKey(table="users", column="id"),
                ),
            ],
        )
        change = SchemaChange(
            change_type=ChangeType.CREATE_TABLE,
            table_name="orders",
            details={"table": table},
        )

        result = generator.generate([change], "Create orders")

        # FK info is NOT in SQL - it's informational only
        assert "FOREIGN KEY" not in result
        assert "REFERENCES" not in result
        # Column itself is still created
        assert "user_id BIGINT NOT NULL" in result


class TestCodegenTimestamp:
    """Tests for timestamp line format."""

    def test_codegen_timestamp_line_format(self, generator: MigrationGenerator):
        """Test timestamp line starts with '-- Generated:'."""
        change = SchemaChange(
            change_type=ChangeType.ADD_COLUMN,
            table_name="users",
            details={"column": Column(name="x", type="INT", nullable=True)},
        )

        result = generator.generate([change], "test")

        # Find the Generated line
        lines = result.split("\n")
        generated_lines = [line for line in lines if line.startswith("-- Generated:")]
        assert len(generated_lines) == 1
        # Should be ISO format datetime after the prefix
        timestamp_part = generated_lines[0].replace("-- Generated: ", "")
        # Verify it can be parsed as ISO datetime
        datetime.fromisoformat(timestamp_part)
