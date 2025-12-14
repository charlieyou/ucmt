"""Tests for schema loader."""

import tempfile
from pathlib import Path

import pytest

from ucmt.exceptions import SchemaLoadError
from ucmt.schema.loader import load_schema


FIXTURES_PATH = Path(__file__).parent.parent / "fixtures" / "schema" / "tables"


def test_loader_users_yaml_golden():
    """Test users.yaml loads correctly with all fields."""
    schema = load_schema(FIXTURES_PATH)
    users = schema.get_table("users")

    assert users.name == "users"
    assert len(users.columns) == 7
    assert users.primary_key is not None
    assert users.primary_key.columns == ["id"]
    assert users.primary_key.rely is True
    assert users.liquid_clustering == ["status", "created_at"]
    assert users.table_properties["delta.columnMapping.mode"] == "name"
    assert users.comment == "Core user accounts table"

    id_col = users.get_column("id")
    assert id_col.type == "BIGINT"
    assert id_col.nullable is False
    assert id_col.generated == "ALWAYS AS IDENTITY"

    status_col = users.get_column("status")
    assert status_col.default == "'active'"
    assert status_col.check == "status IN ('active', 'suspended', 'deleted')"


def test_loader_transactions_yaml_golden():
    """Test transactions.yaml loads correctly with foreign key."""
    schema = load_schema(FIXTURES_PATH)
    transactions = schema.get_table("transactions")

    assert transactions.name == "transactions"
    assert len(transactions.columns) == 7
    assert transactions.liquid_clustering == ["user_id", "tx_date"]

    user_id_col = transactions.get_column("user_id")
    assert user_id_col is not None
    assert user_id_col.foreign_key is not None
    assert user_id_col.foreign_key.table == "users"
    assert user_id_col.foreign_key.column == "id"


def test_loader_schema_dir_multiple_tables():
    """Test loading schema from directory with multiple YAML files."""
    schema = load_schema(FIXTURES_PATH)

    assert len(schema.tables) == 2
    assert "users" in schema.tables
    assert "transactions" in schema.tables


def test_loader_missing_table_field_raises_SchemaLoadError():
    """Test that missing 'table' field raises SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "bad.yaml"
        yaml_file.write_text("""
columns:
  - name: id
    type: BIGINT
""")
        with pytest.raises(SchemaLoadError, match="missing 'table' field"):
            load_schema(yaml_file)


def test_loader_missing_column_name_raises_SchemaLoadError():
    """Test that missing column name raises SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "bad.yaml"
        yaml_file.write_text("""
table: test_table
columns:
  - type: BIGINT
""")
        with pytest.raises(SchemaLoadError, match="missing 'name'"):
            load_schema(yaml_file)


def test_loader_missing_column_type_raises_SchemaLoadError():
    """Test that missing column type raises SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "bad.yaml"
        yaml_file.write_text("""
table: test_table
columns:
  - name: id
""")
        with pytest.raises(SchemaLoadError, match="missing 'type'"):
            load_schema(yaml_file)


def test_loader_unknown_field_raises_SchemaLoadError():
    """Test that unknown fields raise SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "bad.yaml"
        yaml_file.write_text("""
table: test_table
unknown_field: some_value
columns:
  - name: id
    type: BIGINT
""")
        with pytest.raises(SchemaLoadError, match="unknown.*field"):
            load_schema(yaml_file)


def test_loader_validates_liquid_clustering_max_4_cols():
    """Test that liquid_clustering with more than 4 columns raises SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "bad.yaml"
        yaml_file.write_text("""
table: test_table
columns:
  - name: a
    type: STRING
  - name: b
    type: STRING
  - name: c
    type: STRING
  - name: d
    type: STRING
  - name: e
    type: STRING
liquid_clustering: [a, b, c, d, e]
""")
        with pytest.raises(SchemaLoadError, match="(?i)liquid.*clustering.*4"):
            load_schema(yaml_file)


def test_loader_duplicate_column_names_raises_SchemaLoadError():
    """Test that duplicate column names raise SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "bad.yaml"
        yaml_file.write_text("""
table: test_table
columns:
  - name: id
    type: BIGINT
  - name: id
    type: STRING
""")
        with pytest.raises(SchemaLoadError, match="(?i)duplicate.*column"):
            load_schema(yaml_file)


def test_loader_duplicate_table_names_raises_SchemaLoadError():
    """Test that duplicate table names in a directory raise SchemaLoadError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        (Path(tmpdir) / "table1.yaml").write_text("""
table: users
columns:
  - name: id
    type: BIGINT
""")
        (Path(tmpdir) / "table2.yaml").write_text("""
table: users
columns:
  - name: email
    type: STRING
""")
        with pytest.raises(SchemaLoadError, match="(?i)duplicate.*table"):
            load_schema(Path(tmpdir))


def test_loader_defaults():
    """Test that default values are applied correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_file = Path(tmpdir) / "minimal.yaml"
        yaml_file.write_text("""
table: minimal
columns:
  - name: id
    type: BIGINT
""")
        schema = load_schema(yaml_file)
        table = schema.get_table("minimal")

        assert table.primary_key is None
        assert table.check_constraints == []
        assert table.liquid_clustering == []
        assert table.partitioned_by == []
        assert table.table_properties == {}
        assert table.comment is None

        col = table.get_column("id")
        assert col.nullable is True
        assert col.default is None
        assert col.generated is None
        assert col.check is None
        assert col.foreign_key is None
        assert col.comment is None


def test_loader_parses_foreign_key_to_model():
    """Test that foreign keys are parsed into Column.foreign_key."""
    schema = load_schema(FIXTURES_PATH)
    transactions = schema.get_table("transactions")
    user_id_col = transactions.get_column("user_id")

    assert user_id_col.foreign_key is not None
    assert user_id_col.foreign_key.table == "users"
    assert user_id_col.foreign_key.column == "id"
