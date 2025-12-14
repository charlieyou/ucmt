"""End-to-end workflow validation tests.

These tests validate the complete UCMT workflow:
- YAML schema definition → Diff → Generate SQL → Apply migrations
- All tests run without real Databricks using mocked clients.
"""

from pathlib import Path

import pytest

from tests.helpers import (
    build_db_state_from_schema,
    make_mock_client,
    make_test_config,
    strip_timestamp_line,
)
from ucmt.config import Config
from ucmt.exceptions import UnsupportedSchemaChangeError
from ucmt.migrations.runner import Runner
from ucmt.migrations.state import InMemoryMigrationStateStore
from ucmt.schema.codegen import MigrationGenerator
from ucmt.schema.diff import SchemaDiffer
from ucmt.schema.introspect import SchemaIntrospector
from ucmt.schema.loader import load_schema
from ucmt.schema.models import Column, PrimaryKey, Schema, Table
from ucmt.types import ChangeType


FIXTURES_PATH = Path(__file__).parent.parent / "fixtures" / "schema" / "tables"
GOLDEN_SQL_PATH = Path(__file__).parent.parent / "fixtures" / "golden_sql"


class TestRoundtripYamlToDbProducesNoDiff:
    """Test that YAML -> DB -> YAML roundtrip produces no diff."""

    def test_roundtrip_yaml_to_db_produces_no_diff(self):
        """When DB state matches YAML exactly, diff should be empty."""
        yaml_schema = load_schema(FIXTURES_PATH)

        db_state = build_db_state_from_schema(yaml_schema)
        client = make_mock_client(*db_state)

        config = make_test_config()
        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 0, f"Expected no changes, got: {changes}"

    def test_roundtrip_single_table_produces_no_diff(self):
        """Single table roundtrip produces no diff."""
        yaml_schema = Schema(
            tables={
                "simple": Table(
                    name="simple",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="name", type="STRING", nullable=True),
                    ],
                    primary_key=PrimaryKey(columns=["id"], rely=True),
                )
            }
        )

        db_state = build_db_state_from_schema(yaml_schema)
        client = make_mock_client(*db_state)

        config = make_test_config()
        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 0


class TestGenerateThenApplyWorkflow:
    """Test the generate → apply workflow."""

    def test_generate_then_apply_workflow(self):
        """Complete workflow: diff → generate SQL → apply migration."""
        state_store = InMemoryMigrationStateStore()
        executed_sql: list[str] = []

        def mock_executor(sql: str, version: int):
            executed_sql.append(sql)

        config = make_test_config()

        client = make_mock_client(tables_data=[])
        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=False),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)
        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.CREATE_TABLE

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Create users table")

        assert "CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.users" in sql

        from ucmt.migrations.parser import MigrationFile

        migration = MigrationFile(
            version=1,
            name="create_users",
            path=Path("V1__create_users.sql"),
            checksum="test_checksum",
            sql=sql,
        )

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )

        runner.apply([migration])

        assert len(executed_sql) == 1
        assert "test_catalog.test_schema.users" in executed_sql[0]
        assert state_store.has_applied(1)


class TestFullLifecycleCreateTable:
    """Test full lifecycle for creating a new table."""

    def test_full_lifecycle_create_table(self):
        """Full lifecycle: empty DB → create table → verify state."""
        state_store = InMemoryMigrationStateStore()
        executed_sql: list[str] = []
        config = make_test_config()

        db_schema = Schema(tables={})

        yaml_schema = Schema(
            tables={
                "products": Table(
                    name="products",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="name", type="STRING", nullable=False),
                        Column(name="price", type="DECIMAL(10,2)", nullable=True),
                    ],
                    primary_key=PrimaryKey(columns=["id"], rely=True),
                    liquid_clustering=["name"],
                    table_properties={"delta.columnMapping.mode": "name"},
                    comment="Product catalog",
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.CREATE_TABLE

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Create products table")

        assert "CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.products" in sql
        assert "id BIGINT NOT NULL" in sql
        assert "name STRING NOT NULL" in sql
        assert "price DECIMAL(10,2)" in sql
        assert "PRIMARY KEY (id)" in sql
        assert "CLUSTER BY (name)" in sql
        assert "'delta.columnMapping.mode' = 'name'" in sql
        assert "COMMENT 'Product catalog'" in sql

        from ucmt.migrations.parser import MigrationFile

        migration = MigrationFile(
            version=1,
            name="create_products",
            path=Path("V1__create_products.sql"),
            checksum="abc123",
            sql=sql,
        )

        def mock_executor(sql: str, version: int):
            executed_sql.append(sql)

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )
        runner.apply([migration])

        assert state_store.has_applied(1)
        applied = state_store.get_last_applied()
        assert applied is not None
        assert applied.success is True


class TestFullLifecycleAddColumn:
    """Test full lifecycle for adding a column."""

    def test_full_lifecycle_add_column(self):
        """Full lifecycle: existing table → add column."""
        state_store = InMemoryMigrationStateStore()
        executed_sql: list[str] = []
        config = make_test_config()

        db_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=False),
                    ],
                )
            }
        )

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=False),
                        Column(
                            name="phone",
                            type="STRING",
                            nullable=True,
                            comment="User phone",
                        ),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ADD_COLUMN
        assert changes[0].details["column_name"] == "phone"

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Add phone column")

        assert (
            "ALTER TABLE ${catalog}.${schema}.users ADD COLUMN IF NOT EXISTS phone STRING"
            in sql
        )
        assert "COMMENT 'User phone'" in sql

        from ucmt.migrations.parser import MigrationFile

        migration = MigrationFile(
            version=1,
            name="add_phone",
            path=Path("V1__add_phone.sql"),
            checksum="def456",
            sql=sql,
        )

        def mock_executor(sql: str, version: int):
            executed_sql.append(sql)

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )
        runner.apply([migration])

        assert state_store.has_applied(1)
        assert "test_catalog.test_schema.users ADD COLUMN" in executed_sql[0]


class TestFullLifecycleUnsupportedChangeBlocksApply:
    """Test that unsupported changes block migration generation."""

    def test_full_lifecycle_unsupported_change_blocks_apply(self):
        """Unsupported changes (type narrowing) should raise during generation."""
        config = make_test_config()

        db_schema = Schema(
            tables={
                "metrics": Table(
                    name="metrics",
                    columns=[Column(name="count", type="BIGINT", nullable=True)],
                )
            }
        )

        yaml_schema = Schema(
            tables={
                "metrics": Table(
                    name="metrics",
                    columns=[Column(name="count", type="INT", nullable=True)],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_COLUMN_TYPE
        assert changes[0].is_unsupported is True

        generator = MigrationGenerator(config.catalog, config.schema)

        with pytest.raises(UnsupportedSchemaChangeError):
            generator.generate(changes, "Narrow count column")

    def test_partition_change_blocked(self):
        """Partition changes are unsupported and should raise."""
        config = make_test_config()

        db_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                    partitioned_by=["region"],
                )
            }
        )

        yaml_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                    partitioned_by=["country"],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert any(c.is_unsupported for c in changes)

        generator = MigrationGenerator(config.catalog, config.schema)
        with pytest.raises(UnsupportedSchemaChangeError):
            generator.generate(changes, "Change partitioning")


class TestLocalAndDatabricksProduceSameSql:
    """Test that local (offline) and Databricks (mocked online) produce same SQL."""

    def test_local_and_databricks_produce_same_sql(self):
        """Offline and online modes should produce identical SQL."""
        config = make_test_config()

        empty_db_schema = Schema(tables={})
        yaml_schema = load_schema(FIXTURES_PATH / "users.yaml")

        differ = SchemaDiffer()
        offline_changes = differ.diff(empty_db_schema, yaml_schema)

        client = make_mock_client(tables_data=[])
        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        online_db_schema = introspector.introspect_schema()

        online_changes = differ.diff(online_db_schema, yaml_schema)

        assert len(offline_changes) == len(online_changes)

        for off, on in zip(offline_changes, online_changes):
            assert off.change_type == on.change_type
            assert off.table_name == on.table_name

        generator = MigrationGenerator(config.catalog, config.schema)
        offline_sql = generator.generate(offline_changes, "offline")
        online_sql = generator.generate(online_changes, "online")

        offline_lines = [
            line
            for line in offline_sql.split("\n")
            if not line.startswith("-- Generated:")
            and not line.startswith("-- Description:")
        ]
        online_lines = [
            line
            for line in online_sql.split("\n")
            if not line.startswith("-- Generated:")
            and not line.startswith("-- Description:")
        ]

        assert offline_lines == online_lines


class TestVariableSubstitutionWorks:
    """Test that ${catalog} and ${schema} variables are substituted correctly."""

    def test_variable_substitution_works(self):
        """Variables should be substituted during migration execution."""
        config = Config(catalog="prod_catalog", schema="prod_schema")
        executed_sql: list[str] = []
        state_store = InMemoryMigrationStateStore()

        def mock_executor(sql: str, version: int):
            executed_sql.append(sql)

        sql_with_vars = """-- Migration
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.users (
    id BIGINT NOT NULL
);
"""

        from ucmt.migrations.parser import MigrationFile

        migration = MigrationFile(
            version=1,
            name="create_users",
            path=Path("V1__create_users.sql"),
            checksum="xyz",
            sql=sql_with_vars,
        )

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )
        runner.apply([migration])

        assert len(executed_sql) == 1
        assert "prod_catalog.prod_schema.users" in executed_sql[0]
        assert "${catalog}" not in executed_sql[0]
        assert "${schema}" not in executed_sql[0]

    def test_variable_substitution_with_multiple_tables(self):
        """Variables should be substituted for all table references."""
        config = Config(catalog="my_cat", schema="my_sch")
        executed_sql: list[str] = []
        state_store = InMemoryMigrationStateStore()

        def mock_executor(sql: str, version: int):
            executed_sql.append(sql)

        sql_with_vars = """
ALTER TABLE ${catalog}.${schema}.orders ADD COLUMN status STRING;
ALTER TABLE ${catalog}.${schema}.users ADD COLUMN verified BOOLEAN;
"""

        from ucmt.migrations.parser import MigrationFile

        migration = MigrationFile(
            version=1,
            name="add_columns",
            path=Path("V1__add_columns.sql"),
            checksum="multi",
            sql=sql_with_vars,
        )

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )
        runner.apply([migration])

        executed = executed_sql[0]
        assert "my_cat.my_sch.orders" in executed
        assert "my_cat.my_sch.users" in executed
        assert "${catalog}" not in executed
        assert "${schema}" not in executed


class TestGoldenSqlMatches:
    """Test that generated SQL matches golden files (except timestamp)."""

    def test_create_table_golden_sql(self):
        """Generated CREATE TABLE SQL should match golden file."""
        config = make_test_config()

        yaml_schema = Schema(
            tables={
                "test_table": Table(
                    name="test_table",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="name", type="STRING", nullable=True),
                    ],
                    primary_key=PrimaryKey(columns=["id"], rely=True),
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(Schema(tables={}), yaml_schema)

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Create test_table")

        golden_path = GOLDEN_SQL_PATH / "create_test_table.sql"
        expected_sql = golden_path.read_text()

        assert strip_timestamp_line(sql) == strip_timestamp_line(expected_sql)

    def test_add_column_golden_sql(self):
        """Generated ADD COLUMN SQL should match golden file."""
        config = make_test_config()

        db_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=False),
                    ],
                )
            }
        )

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=False),
                        Column(
                            name="phone",
                            type="STRING",
                            nullable=True,
                            comment="User phone",
                        ),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Add phone column")

        golden_path = GOLDEN_SQL_PATH / "add_phone_column.sql"
        expected_sql = golden_path.read_text()

        assert strip_timestamp_line(sql) == strip_timestamp_line(expected_sql)


class TestMultipleMigrationsInSequence:
    """Test applying multiple migrations in sequence."""

    def test_multiple_migrations_in_sequence(self):
        """Multiple migrations should be applied in version order."""
        state_store = InMemoryMigrationStateStore()
        executed_sql: list[tuple[str, int]] = []
        config = make_test_config()

        def mock_executor(sql: str, version: int):
            executed_sql.append((sql, version))

        from ucmt.migrations.parser import MigrationFile

        migrations = [
            MigrationFile(
                version=3,
                name="third",
                path=Path("V3__third.sql"),
                checksum="c3",
                sql="-- Migration 3",
            ),
            MigrationFile(
                version=1,
                name="first",
                path=Path("V1__first.sql"),
                checksum="c1",
                sql="-- Migration 1",
            ),
            MigrationFile(
                version=2,
                name="second",
                path=Path("V2__second.sql"),
                checksum="c2",
                sql="-- Migration 2",
            ),
        ]

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )
        runner.apply(migrations)

        assert len(executed_sql) == 3
        versions_executed = [v for _, v in executed_sql]
        assert versions_executed == [1, 2, 3]

        assert state_store.has_applied(1)
        assert state_store.has_applied(2)
        assert state_store.has_applied(3)


class TestIdempotentMigrationApplication:
    """Test that migrations are idempotent."""

    def test_already_applied_migrations_skipped(self):
        """Already applied migrations should not be re-executed."""
        state_store = InMemoryMigrationStateStore()
        executed_sql: list[str] = []
        config = make_test_config()

        state_store.record_applied(
            version=1,
            name="first",
            checksum="c1",
            success=True,
        )

        def mock_executor(sql: str, version: int):
            executed_sql.append(sql)

        from ucmt.migrations.parser import MigrationFile

        migrations = [
            MigrationFile(
                version=1,
                name="first",
                path=Path("V1__first.sql"),
                checksum="c1",
                sql="-- Migration 1",
            ),
            MigrationFile(
                version=2,
                name="second",
                path=Path("V2__second.sql"),
                checksum="c2",
                sql="-- Migration 2",
            ),
        ]

        runner = Runner(
            state_store=state_store,
            executor=mock_executor,
            catalog=config.catalog,
            schema=config.schema,
        )
        runner.apply(migrations)

        assert len(executed_sql) == 1
        assert "Migration 2" in executed_sql[0]
