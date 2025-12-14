"""Tests for schema loader."""

from pathlib import Path


from ucmt.schema.loader import load_schema


FIXTURES_PATH = Path(__file__).parent.parent / "fixtures" / "schema" / "tables"


def test_load_schema_from_directory():
    """Test loading schema from directory of YAML files."""
    schema = load_schema(FIXTURES_PATH)

    assert len(schema.tables) == 2
    assert "users" in schema.tables
    assert "transactions" in schema.tables


def test_load_users_table():
    """Test users table is loaded correctly."""
    schema = load_schema(FIXTURES_PATH)
    users = schema.get_table("users")

    assert users.name == "users"
    assert len(users.columns) == 7
    assert users.primary_key is not None
    assert users.primary_key.columns == ["id"]
    assert users.primary_key.rely is True
    assert users.liquid_clustering == ["status", "created_at"]


def test_load_transactions_table():
    """Test transactions table is loaded correctly."""
    schema = load_schema(FIXTURES_PATH)
    transactions = schema.get_table("transactions")

    assert transactions.name == "transactions"
    assert len(transactions.columns) == 7

    user_id_col = transactions.get_column("user_id")
    assert user_id_col is not None
    assert user_id_col.foreign_key is not None
    assert user_id_col.foreign_key.table == "users"
    assert user_id_col.foreign_key.column == "id"


def test_load_column_properties():
    """Test column properties are loaded correctly."""
    schema = load_schema(FIXTURES_PATH)
    users = schema.get_table("users")

    id_col = users.get_column("id")
    assert id_col.type == "BIGINT"
    assert id_col.nullable is False
    assert id_col.generated == "ALWAYS AS IDENTITY"

    status_col = users.get_column("status")
    assert status_col.default == "'active'"
    assert status_col.check == "status IN ('active', 'suspended', 'deleted')"
