"""Tests for schema exporter."""

import yaml

from ucmt.schema.exporter import (
    export_schema_to_directory,
    export_table_yaml,
    table_to_dict,
)
from ucmt.schema.loader import load_schema
from ucmt.schema.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    PrimaryKey,
    Schema,
    Table,
)


class TestTableToDict:
    def test_minimal_table(self):
        table = Table(
            name="users",
            columns=[Column(name="id", type="BIGINT")],
        )
        result = table_to_dict(table)
        assert result == {
            "table": "users",
            "columns": [{"name": "id", "type": "BIGINT"}],
        }

    def test_table_with_comment(self):
        table = Table(
            name="users",
            columns=[Column(name="id", type="BIGINT")],
            comment="User accounts",
        )
        result = table_to_dict(table)
        assert result["comment"] == "User accounts"

    def test_column_with_all_fields(self):
        table = Table(
            name="users",
            columns=[
                Column(
                    name="email",
                    type="STRING",
                    nullable=False,
                    default="'unknown'",
                    comment="User email",
                )
            ],
        )
        result = table_to_dict(table)
        col = result["columns"][0]
        assert col["name"] == "email"
        assert col["type"] == "STRING"
        assert col["nullable"] is False
        assert col["default"] == "'unknown'"
        assert col["comment"] == "User email"

    def test_column_with_foreign_key(self):
        table = Table(
            name="orders",
            columns=[
                Column(
                    name="user_id",
                    type="BIGINT",
                    foreign_key=ForeignKey(table="users", column="id"),
                )
            ],
        )
        result = table_to_dict(table)
        col = result["columns"][0]
        assert col["foreign_key"] == {"table": "users", "column": "id"}

    def test_primary_key(self):
        table = Table(
            name="users",
            columns=[Column(name="id", type="BIGINT")],
            primary_key=PrimaryKey(columns=["id"], rely=False),
        )
        result = table_to_dict(table)
        assert result["primary_key"] == {"columns": ["id"]}

    def test_primary_key_with_rely(self):
        table = Table(
            name="users",
            columns=[Column(name="id", type="BIGINT")],
            primary_key=PrimaryKey(columns=["id"], rely=True),
        )
        result = table_to_dict(table)
        assert result["primary_key"] == {"columns": ["id"], "rely": True}

    def test_check_constraints(self):
        table = Table(
            name="users",
            columns=[Column(name="age", type="INT")],
            check_constraints=[
                CheckConstraint(name="age_positive", expression="age > 0")
            ],
        )
        result = table_to_dict(table)
        assert result["check_constraints"] == [
            {"name": "age_positive", "expression": "age > 0"}
        ]

    def test_liquid_clustering(self):
        table = Table(
            name="events",
            columns=[Column(name="event_date", type="DATE")],
            liquid_clustering=["event_date"],
        )
        result = table_to_dict(table)
        assert result["liquid_clustering"] == ["event_date"]

    def test_partitioned_by(self):
        table = Table(
            name="events",
            columns=[Column(name="event_date", type="DATE")],
            partitioned_by=["event_date"],
        )
        result = table_to_dict(table)
        assert result["partitioned_by"] == ["event_date"]

    def test_table_properties(self):
        table = Table(
            name="users",
            columns=[Column(name="id", type="BIGINT")],
            table_properties={"delta.columnMapping.mode": "name"},
        )
        result = table_to_dict(table)
        assert result["table_properties"] == {"delta.columnMapping.mode": "name"}


class TestExportTableYaml:
    def test_roundtrip_minimal(self):
        table = Table(
            name="users",
            columns=[Column(name="id", type="BIGINT")],
        )
        yaml_str = export_table_yaml(table)
        parsed = yaml.safe_load(yaml_str)
        assert parsed["table"] == "users"
        assert parsed["columns"] == [{"name": "id", "type": "BIGINT"}]

    def test_yaml_is_valid(self):
        table = Table(
            name="users",
            columns=[
                Column(name="id", type="BIGINT", nullable=False),
                Column(name="email", type="STRING", comment="User email"),
            ],
            primary_key=PrimaryKey(columns=["id"]),
            comment="User accounts table",
        )
        yaml_str = export_table_yaml(table)
        parsed = yaml.safe_load(yaml_str)
        assert parsed is not None
        assert "table" in parsed
        assert "columns" in parsed


class TestExportSchemaToDirectory:
    def test_creates_files_for_each_table(self, tmp_path):
        schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[Column(name="id", type="BIGINT")],
                ),
                "orders": Table(
                    name="orders",
                    columns=[Column(name="id", type="BIGINT")],
                ),
            }
        )
        output_dir = tmp_path / "schema" / "tables"
        created_files = export_schema_to_directory(schema, output_dir)

        assert len(created_files) == 2
        assert (output_dir / "orders.yaml").exists()
        assert (output_dir / "users.yaml").exists()

    def test_creates_output_directory(self, tmp_path):
        schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[Column(name="id", type="BIGINT")],
                ),
            }
        )
        output_dir = tmp_path / "nested" / "path" / "schema"
        export_schema_to_directory(schema, output_dir)

        assert output_dir.exists()

    def test_files_contain_valid_yaml(self, tmp_path):
        schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING"),
                    ],
                    primary_key=PrimaryKey(columns=["id"]),
                ),
            }
        )
        output_dir = tmp_path / "schema"
        export_schema_to_directory(schema, output_dir)

        users_yaml = (output_dir / "users.yaml").read_text()
        parsed = yaml.safe_load(users_yaml)

        assert parsed["table"] == "users"
        assert len(parsed["columns"]) == 2
        assert parsed["primary_key"]["columns"] == ["id"]

    def test_roundtrip_export_then_load(self, tmp_path):
        """Exported YAML can be loaded back with identical schema."""
        original_schema = Schema(
            tables={
                "users": Table(
                    name="users",
                    columns=[
                        Column(name="id", type="BIGINT", nullable=False),
                        Column(name="email", type="STRING", comment="User email"),
                        Column(name="status", type="STRING", default="'active'"),
                    ],
                    primary_key=PrimaryKey(columns=["id"], rely=True),
                    check_constraints=[
                        CheckConstraint(
                            name="email_not_empty", expression="email != ''"
                        )
                    ],
                    liquid_clustering=["id"],
                    table_properties={"delta.columnMapping.mode": "name"},
                    comment="User accounts table",
                ),
            }
        )
        output_dir = tmp_path / "schema"
        export_schema_to_directory(original_schema, output_dir)

        loaded_schema = load_schema(output_dir)

        assert loaded_schema.table_names() == original_schema.table_names()
        original_table = original_schema.get_table("users")
        loaded_table = loaded_schema.get_table("users")
        assert loaded_table.name == original_table.name
        assert len(loaded_table.columns) == len(original_table.columns)
        assert loaded_table.primary_key == original_table.primary_key
        assert loaded_table.liquid_clustering == original_table.liquid_clustering
        assert loaded_table.table_properties == original_table.table_properties

    def test_liquid_clustering_truncates_over_4(self, tmp_path, caplog):
        """Liquid clustering > 4 columns is truncated with warning."""
        table = Table(
            name="events",
            columns=[Column(name="id", type="BIGINT")],
            liquid_clustering=["a", "b", "c", "d", "e"],
        )
        result = table_to_dict(table)

        assert result["liquid_clustering"] == ["a", "b", "c", "d"]
        assert "Truncating to first 4" in caplog.text
