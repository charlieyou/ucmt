"""Core type definitions for ucmt."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeAlias

TableName: TypeAlias = str
ColumnName: TypeAlias = str
CatalogName: TypeAlias = str
SchemaName: TypeAlias = str

__all__ = [
    "TableName",
    "ColumnName",
    "CatalogName",
    "SchemaName",
    "ChangeType",
    "SchemaChange",
]


class ChangeType(Enum):
    """Types of schema changes that can be detected and applied."""

    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    ALTER_COLUMN_TYPE = "alter_column_type"
    ALTER_COLUMN_NULLABILITY = "alter_column_nullability"
    ALTER_COLUMN_DEFAULT = "alter_column_default"
    SET_PRIMARY_KEY = "set_primary_key"
    DROP_PRIMARY_KEY = "drop_primary_key"
    ADD_FOREIGN_KEY = "add_foreign_key"
    DROP_FOREIGN_KEY = "drop_foreign_key"
    ADD_CHECK_CONSTRAINT = "add_check_constraint"
    DROP_CHECK_CONSTRAINT = "drop_check_constraint"
    ALTER_CLUSTERING = "alter_clustering"
    ALTER_PARTITIONING = "alter_partitioning"
    ALTER_TABLE_PROPERTIES = "alter_table_properties"


@dataclass
class SchemaChange:
    """Represents a single schema change detected by the differ."""

    change_type: ChangeType
    table_name: TableName
    details: dict[str, Any] = field(default_factory=dict)
    is_destructive: bool = False
    is_unsupported: bool = False
    error_message: str | None = None
    requires_column_mapping: bool = False
