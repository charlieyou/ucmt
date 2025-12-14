"""Integration test for full online diff flow.

Exercises: Config → DatabricksClient → SchemaIntrospector → SchemaDiffer → MigrationGenerator
Uses mocked fetchall responses for information_schema queries.
Verifies end-to-end correctness without real DB.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ucmt.config import Config
from ucmt.exceptions import CodegenError, UnsupportedSchemaChangeError
from ucmt.schema.codegen import MigrationGenerator
from ucmt.schema.diff import SchemaDiffer
from ucmt.schema.introspect import SchemaIntrospector
from ucmt.schema.loader import load_schema
from ucmt.schema.models import CheckConstraint, Column, PrimaryKey, Schema, Table
from ucmt.types import ChangeType


class FakeRow:
    """Mock row from DatabricksClient.fetchall()."""

    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def asDict(self):
        return self._data


def make_test_config(
    catalog: str = "test_catalog",
    schema: str = "test_schema",
    **kwargs,
) -> Config:
    """Create a Config for tests with sensible defaults."""
    return Config(catalog=catalog, schema=schema, **kwargs)


def make_mock_client(
    tables_data: list[dict] | None = None,
    columns_data: dict[str, list[dict]] | None = None,
    pk_constraints_data: dict[str, list[dict]] | None = None,
    check_constraints_data: dict[str, list[dict]] | None = None,
    table_properties_data: dict[str, list[dict]] | None = None,
) -> MagicMock:
    """Create a mock DatabricksClient with test data.

    Args:
        tables_data: List of table metadata dicts
        columns_data: Dict mapping table_name -> list of column dicts
        pk_constraints_data: Dict mapping table_name -> list of PK constraint dicts
        check_constraints_data: Dict mapping table_name -> list of CHECK constraint dicts
        table_properties_data: Dict mapping table_name -> list of property dicts

    Note:
        The mock handles both single-table queries (for introspect_table) and
        all-table queries (for introspect_schema) by checking if a specific
        table name appears in the SQL.
    """
    client = MagicMock()
    columns_data = columns_data or {}
    pk_constraints_data = pk_constraints_data or {}
    check_constraints_data = check_constraints_data or {}
    table_properties_data = table_properties_data or {}

    def fetchall_side_effect(sql: str):
        sql_lower = sql.lower()

        if "information_schema.tables" in sql_lower:
            for table in tables_data or []:
                if f"'{table['table_name']}'" in sql_lower:
                    return [FakeRow(table)]
            return [FakeRow(t) for t in (tables_data or [])]

        if "information_schema.columns" in sql_lower:
            for table_name, cols in columns_data.items():
                if f"'{table_name}'" in sql_lower:
                    return [FakeRow(c) for c in cols]
            return []

        if "constraint_column_usage" in sql_lower:
            for table_name, pks in pk_constraints_data.items():
                if f"'{table_name}'" in sql_lower:
                    return [FakeRow(pk) for pk in pks]
            return []

        if "table_constraints" in sql_lower and "check" in sql_lower:
            for table_name, checks in check_constraints_data.items():
                if f"'{table_name}'" in sql_lower:
                    return [FakeRow(c) for c in checks]
            return []

        if "tblproperties" in sql_lower:
            for table_name, props in table_properties_data.items():
                if f"`{table_name}`" in sql_lower:
                    return [FakeRow(p) for p in props]
            return []

        return []

    client.fetchall.side_effect = fetchall_side_effect
    return client


class TestFullOnlineDiffFlow:
    """Test the complete online diff flow end-to-end."""

    def test_new_table_creates_migration(self):
        """When DB has no tables and YAML declares one, generate CREATE TABLE."""
        config = make_test_config(
            databricks_host="test.databricks.com",
            databricks_token="test_token",
            databricks_http_path="/sql/1.0/warehouses/abc",
        )

        client = make_mock_client(tables_data=[])
        introspector = SchemaIntrospector(client, config.catalog, config.schema)

        db_schema = introspector.introspect_schema()
        assert len(db_schema.tables) == 0

        yaml_schema = load_schema(
            Path(__file__).parent.parent
            / "fixtures"
            / "schema"
            / "tables"
            / "users.yaml"
        )
        assert "users" in yaml_schema.tables

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.CREATE_TABLE
        assert changes[0].table_name == "users"

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Add users table")

        assert "CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.users" in sql
        assert "id BIGINT" in sql
        assert "email STRING NOT NULL" in sql

    def test_add_column_generates_alter(self):
        """When DB is missing a column declared in YAML, generate ADD COLUMN."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "users": [
                    {
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                    {
                        "table_name": "users",
                        "column_name": "email",
                        "data_type": "STRING",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=False),
                        Column(name="phone", type="STRING", nullable=True),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        add_column_changes = [
            c for c in changes if c.change_type == ChangeType.ADD_COLUMN
        ]
        assert len(add_column_changes) == 1
        assert add_column_changes[0].details["column_name"] == "phone"

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Add phone column")

        assert (
            "ALTER TABLE ${catalog}.${schema}.users ADD COLUMN IF NOT EXISTS phone STRING"
            in sql
        )

    def test_no_changes_when_schemas_match(self):
        """When DB matches YAML exactly, no changes generated."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "simple",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "simple": [
                    {
                        "table_name": "simple",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "simple": Table(
                    name="simple",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        assert len(changes) == 0

    def test_type_widening_generates_alter(self):
        """Widening INT to BIGINT should generate valid ALTER COLUMN TYPE."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "metrics",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "metrics": [
                    {
                        "table_name": "metrics",
                        "column_name": "count",
                        "data_type": "INT",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "metrics": Table(
                    name="metrics",
                    columns=[
                        Column(name="count", type="BIGINT", nullable=True),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        type_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_COLUMN_TYPE
        ]
        assert len(type_changes) == 1
        assert not type_changes[0].is_unsupported

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Widen count to BIGINT")

        assert (
            "ALTER TABLE ${catalog}.${schema}.metrics ALTER COLUMN count TYPE BIGINT"
            in sql
        )

    def test_unsupported_type_change_raises(self):
        """Narrowing BIGINT to INT should fail with unsupported error."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "metrics",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "metrics": [
                    {
                        "table_name": "metrics",
                        "column_name": "count",
                        "data_type": "BIGINT",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "metrics": Table(
                    name="metrics",
                    columns=[
                        Column(name="count", type="INT", nullable=True),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        type_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_COLUMN_TYPE
        ]
        assert len(type_changes) == 1
        assert type_changes[0].is_unsupported
        assert type_changes[0].error_message is not None

        generator = MigrationGenerator(config.catalog, config.schema)

        with pytest.raises(UnsupportedSchemaChangeError):
            generator.generate(changes, "Invalid narrowing")

    def test_multiple_tables_diff(self):
        """Diff with multiple tables creates correct changes for each."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                },
            ],
            columns_data={
                "users": [
                    {
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", nullable=True),
                    ],
                ),
                "orders": Table(
                    name="orders",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="user_id", type="BIGINT", nullable=False),
                    ],
                ),
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        create_changes = [
            c for c in changes if c.change_type == ChangeType.CREATE_TABLE
        ]
        add_col_changes = [c for c in changes if c.change_type == ChangeType.ADD_COLUMN]

        assert len(create_changes) == 1
        assert create_changes[0].table_name == "orders"

        assert len(add_col_changes) == 1
        assert add_col_changes[0].table_name == "users"
        assert add_col_changes[0].details["column_name"] == "email"

    def test_primary_key_diff(self):
        """Adding a primary key generates SET PRIMARY KEY change."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "users": [
                    {
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
            pk_constraints_data={},
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                    ],
                    primary_key=PrimaryKey(columns=["id"], rely=True),
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        pk_changes = [c for c in changes if c.change_type == ChangeType.SET_PRIMARY_KEY]
        assert len(pk_changes) == 1

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Add primary key")

        assert "ADD CONSTRAINT pk_users PRIMARY KEY (id)" in sql
        assert " RELY" in sql

    def test_liquid_clustering_diff(self):
        """Changing liquid clustering generates ALTER CLUSTERING."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "events",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": "user_id",
                }
            ],
            columns_data={
                "events": [
                    {
                        "table_name": "events",
                        "column_name": "user_id",
                        "data_type": "BIGINT",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                    {
                        "table_name": "events",
                        "column_name": "event_date",
                        "data_type": "DATE",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[
                        Column(name="user_id", type="BIGINT", nullable=True),
                        Column(name="event_date", type="DATE", nullable=True),
                    ],
                    liquid_clustering=["user_id", "event_date"],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        clustering_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_CLUSTERING
        ]
        assert len(clustering_changes) == 1

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Update clustering")

        assert "CLUSTER BY (user_id, event_date)" in sql

    def test_check_constraint_diff(self):
        """Adding a check constraint generates ADD CONSTRAINT."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "products",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "products": [
                    {
                        "table_name": "products",
                        "column_name": "price",
                        "data_type": "DECIMAL(10,2)",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
            check_constraints_data={},
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "products": Table(
                    name="products",
                    columns=[
                        Column(name="price", type="DECIMAL(10,2)", nullable=True),
                    ],
                    check_constraints=[
                        CheckConstraint(name="positive_price", expression="price > 0")
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        check_changes = [
            c for c in changes if c.change_type == ChangeType.ADD_CHECK_CONSTRAINT
        ]
        assert len(check_changes) == 1

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Add price check")

        assert "ADD CONSTRAINT positive_price CHECK (price > 0)" in sql

    def test_table_properties_diff(self):
        """Changing table properties generates ALTER TABLE SET TBLPROPERTIES."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "events",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "events": [
                    {
                        "table_name": "events",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
            table_properties_data={
                "events": [
                    {"key": "delta.enableChangeDataFeed", "value": "false"},
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=True),
                    ],
                    table_properties={"delta.enableChangeDataFeed": "true"},
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        prop_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_TABLE_PROPERTIES
        ]
        assert len(prop_changes) == 1

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Enable CDC")

        assert "SET TBLPROPERTIES" in sql
        assert "'delta.enableChangeDataFeed' = 'true'" in sql


class TestExtraDbTablesIgnored:
    """Test that extra DB tables (not in YAML) are ignored, not dropped."""

    def test_extra_db_tables_not_dropped(self):
        """Extra tables in DB but not in YAML should not generate DROP_TABLE."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                },
                {
                    "table_name": "legacy_data",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                },
            ],
            columns_data={
                "users": [
                    {
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                ],
                "legacy_data": [
                    {
                        "table_name": "legacy_data",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ],
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()
        assert len(db_schema.tables) == 2

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        drop_changes = [c for c in changes if c.change_type == ChangeType.DROP_TABLE]
        assert len(drop_changes) == 0
        assert len(changes) == 0


class TestUnsupportedChanges:
    """Test that unsupported schema changes are properly flagged."""

    def test_unsupported_partitioning_change_raises(self):
        """Changing partitioning should fail with unsupported error."""
        config = make_test_config()

        source_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                    partitioned_by=["region"],
                )
            }
        )

        target_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                    partitioned_by=["country"],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(source_schema, target_schema)

        partition_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_PARTITIONING
        ]
        assert len(partition_changes) == 1
        assert partition_changes[0].is_unsupported
        assert "partitioning" in partition_changes[0].error_message.lower()

        generator = MigrationGenerator(config.catalog, config.schema)

        with pytest.raises(UnsupportedSchemaChangeError):
            generator.generate(changes, "Change partitioning")

    def test_codegen_error_not_null_without_default(self):
        """Adding NOT NULL column without default should raise CodegenError."""
        config = make_test_config()

        source_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                )
            }
        )

        target_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="required_field", type="STRING", nullable=False),
                    ],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(source_schema, target_schema)

        generator = MigrationGenerator(config.catalog, config.schema)

        with pytest.raises(CodegenError) as exc_info:
            generator.generate(changes, "Add required column")

        assert "required_field" in str(exc_info.value)


class TestDropConstraints:
    """Test dropping primary keys and check constraints."""

    def test_drop_primary_key(self):
        """Removing a primary key generates DROP PRIMARY KEY."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "users": [
                    {
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "BIGINT",
                        "is_nullable": "NO",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
            pk_constraints_data={
                "users": [
                    {
                        "constraint_name": "pk_users",
                        "constraint_type": "PRIMARY KEY",
                        "column_name": "id",
                        "rely": True,
                    }
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                    primary_key=None,
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        drop_pk_changes = [
            c for c in changes if c.change_type == ChangeType.DROP_PRIMARY_KEY
        ]
        assert len(drop_pk_changes) == 1

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Drop primary key")

        assert "DROP PRIMARY KEY IF EXISTS" in sql

    def test_drop_check_constraint(self):
        """Removing a check constraint generates DROP CONSTRAINT."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "products",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": None,
                }
            ],
            columns_data={
                "products": [
                    {
                        "table_name": "products",
                        "column_name": "price",
                        "data_type": "DECIMAL(10,2)",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
            check_constraints_data={
                "products": [
                    {
                        "constraint_name": "positive_price",
                        "constraint_type": "CHECK",
                        "check_clause": "price > 0",
                    }
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "products": Table(
                    name="products",
                    columns=[Column(name="price", type="DECIMAL(10,2)", nullable=True)],
                    check_constraints=[],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        drop_check_changes = [
            c for c in changes if c.change_type == ChangeType.DROP_CHECK_CONSTRAINT
        ]
        assert len(drop_check_changes) == 1
        assert drop_check_changes[0].details["constraint_name"] == "positive_price"

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Drop check constraint")

        assert "DROP CONSTRAINT IF EXISTS positive_price" in sql


class TestRemoveClustering:
    """Test removing liquid clustering."""

    def test_remove_clustering_generates_cluster_by_none(self):
        """Removing clustering should generate CLUSTER BY NONE."""
        config = make_test_config()

        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "events",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": "user_id",
                }
            ],
            columns_data={
                "events": [
                    {
                        "table_name": "events",
                        "column_name": "user_id",
                        "data_type": "BIGINT",
                        "is_nullable": "YES",
                        "column_default": None,
                        "comment": None,
                    },
                ]
            },
        )

        introspector = SchemaIntrospector(client, config.catalog, config.schema)
        db_schema = introspector.introspect_schema()

        yaml_schema = Schema(
            tables={
                "events": Table(
                    name="events",
                    columns=[Column(name="user_id", type="BIGINT", nullable=True)],
                    liquid_clustering=[],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(db_schema, yaml_schema)

        clustering_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_CLUSTERING
        ]
        assert len(clustering_changes) == 1
        assert clustering_changes[0].details["to_columns"] == []

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Remove clustering")

        assert "CLUSTER BY NONE" in sql
        assert "OPTIMIZE" in sql


class TestConfigToIntrospectorFlow:
    """Test Config -> DatabricksClient -> Introspector flow."""

    def test_config_provides_correct_params(self):
        """Config should provide catalog/schema for introspector.

        Note: We intentionally access private attributes here to verify
        the introspector was initialized with the correct values.
        """
        config = Config.from_env(
            catalog="my_catalog",
            schema="my_schema",
            databricks_host="host.databricks.com",
            databricks_token="token123",
            databricks_http_path="/sql/1.0/warehouses/xyz",
        )

        config.validate_for_db_ops()

        client = make_mock_client(tables_data=[])
        introspector = SchemaIntrospector(client, config.catalog, config.schema)

        assert introspector._catalog == "my_catalog"
        assert introspector._schema == "my_schema"

    def test_variable_substitution_in_generated_sql(self):
        """Generated SQL should use ${catalog} and ${schema} placeholders."""
        config = make_test_config(catalog="prod_catalog", schema="prod_schema")

        yaml_schema = Schema(
            tables={
                "test_table": Table(
                    name="test_table",
                    columns=[Column(name="id", type="BIGINT", nullable=False)],
                )
            }
        )

        differ = SchemaDiffer()
        changes = differ.diff(Schema(tables={}), yaml_schema)

        generator = MigrationGenerator(config.catalog, config.schema)
        sql = generator.generate(changes, "Create test table")

        assert "${catalog}.${schema}.test_table" in sql
        assert "prod_catalog" not in sql
        assert "prod_schema" not in sql
