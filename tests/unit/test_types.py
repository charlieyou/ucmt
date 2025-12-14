"""Tests for ucmt.types module."""

from ucmt.schema.diff import SchemaChange
from ucmt.types import ChangeType


class TestSchemaChange:
    """Tests for SchemaChange dataclass."""

    def test_schema_change_fields(self):
        """SchemaChange has all required fields with correct types."""
        change = SchemaChange(
            change_type=ChangeType.ADD_COLUMN,
            table_name="users",
            details={"column_name": "email", "data_type": "STRING"},
        )
        assert change.change_type == ChangeType.ADD_COLUMN
        assert change.table_name == "users"
        assert change.details == {"column_name": "email", "data_type": "STRING"}
        assert change.is_destructive is False
        assert change.is_unsupported is False
        assert change.error_message is None
        assert change.requires_column_mapping is False

    def test_schema_change_with_optional_fields(self):
        """SchemaChange accepts optional fields."""
        change = SchemaChange(
            change_type=ChangeType.DROP_COLUMN,
            table_name="users",
            details={"column_name": "legacy_field"},
            is_destructive=True,
            is_unsupported=False,
            error_message="Dropping column will lose data",
            requires_column_mapping=True,
        )
        assert change.is_destructive is True
        assert change.error_message == "Dropping column will lose data"
        assert change.requires_column_mapping is True

    def test_schema_change_unsupported_change(self):
        """SchemaChange can represent unsupported changes."""
        change = SchemaChange(
            change_type=ChangeType.ALTER_COLUMN_TYPE,
            table_name="orders",
            details={"column_name": "amount", "from_type": "INT", "to_type": "STRING"},
            is_unsupported=True,
            error_message="Databricks does not support type narrowing",
        )
        assert change.is_unsupported is True
        assert change.error_message is not None
