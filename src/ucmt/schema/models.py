"""Schema representation classes."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ForeignKey:
    """
    Foreign key reference.

    WARNING: Foreign keys in Databricks are INFORMATIONAL ONLY.
    They are NOT enforced - no referential integrity checks occur.
    Use for: query hints, BI tool integration, documentation.
    """

    table: str
    column: str


@dataclass
class PrimaryKey:
    """
    Primary key definition.

    WARNING: Primary keys in Databricks are INFORMATIONAL ONLY.
    Uniqueness is NOT enforced. Use MERGE with dedup logic in pipelines.

    Args:
        columns: List of column names in the primary key
        rely: If True, query optimizer can use this constraint for optimizations
    """

    columns: list[str]
    rely: bool = False


@dataclass
class CheckConstraint:
    """
    CHECK constraint - these ARE enforced by Databricks.
    Transactions will fail if constraint is violated.
    """

    name: str
    expression: str


@dataclass
class Column:
    """Column definition."""

    name: str
    type: str
    nullable: bool = True
    default: Optional[str] = None
    generated: Optional[str] = None
    check: Optional[str] = None
    foreign_key: Optional[ForeignKey] = None
    comment: Optional[str] = None


@dataclass
class Table:
    """Table definition."""

    name: str
    columns: list[Column]
    primary_key: Optional[PrimaryKey] = None
    check_constraints: list[CheckConstraint] = field(default_factory=list)
    liquid_clustering: list[str] = field(default_factory=list)
    partitioned_by: list[str] = field(default_factory=list)
    table_properties: dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None

    def has_column_mapping(self) -> bool:
        """Check if column mapping mode is enabled (required for DROP/RENAME)."""
        return self.table_properties.get("delta.columnMapping.mode") == "name"

    def get_column(self, name: str) -> Optional[Column]:
        """Get a column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


@dataclass
class Schema:
    """Complete schema definition."""

    tables: dict[str, Table]

    def get_table(self, name: str) -> Optional[Table]:
        """Get a table by name."""
        return self.tables.get(name)

    def table_names(self) -> set[str]:
        """Get all table names."""
        return set(self.tables.keys())
