"""Schema representation classes."""

from dataclasses import dataclass, field
from typing import Optional
import re


class DeltaTypeRules:
    """Rules for Delta Lake type changes - widening, narrowing, and unsupported changes."""

    WIDENING_PATHS: dict[str, list[str]] = {
        "TINYINT": ["SMALLINT", "INT", "BIGINT"],
        "SMALLINT": ["INT", "BIGINT"],
        "INT": ["BIGINT"],
        "FLOAT": ["DOUBLE"],
    }

    @classmethod
    def is_widening(cls, from_type: str, to_type: str) -> bool:
        """Check if from_type -> to_type is a valid widening operation."""
        from_upper = from_type.upper()
        to_upper = to_type.upper()
        if from_upper == to_upper:
            return False
        allowed = cls.WIDENING_PATHS.get(from_upper, [])
        return to_upper in allowed

    @classmethod
    def is_decimal_change_unsupported(cls, from_type: str, to_type: str) -> bool:
        """Check if a DECIMAL precision/scale change is unsupported in v1."""
        from_upper = from_type.upper()
        to_upper = to_type.upper()
        decimal_pattern = re.compile(r"DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)")
        from_match = decimal_pattern.match(from_upper)
        to_match = decimal_pattern.match(to_upper)
        if not from_match or not to_match:
            return False
        from_prec, from_scale = from_match.groups()
        to_prec, to_scale = to_match.groups()
        return (from_prec, from_scale) != (to_prec, to_scale)


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

    def __post_init__(self) -> None:
        """Strip whitespace from type. Use normalized_type for comparisons."""
        self.type = self.type.strip()

    @property
    def normalized_type(self) -> str:
        """Return uppercase base type for case-insensitive comparisons."""
        return self.type.upper()


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

    def __eq__(self, other: object) -> bool:
        """Compare tables - column order, clustering order, partitioning order not significant."""
        if not isinstance(other, Table):
            return NotImplemented
        if self.name != other.name:
            return False
        if self.primary_key != other.primary_key:
            return False
        self_constraints = {c.name: c for c in self.check_constraints}
        other_constraints = {c.name: c for c in other.check_constraints}
        if self_constraints != other_constraints:
            return False
        if set(self.liquid_clustering) != set(other.liquid_clustering):
            return False
        if set(self.partitioned_by) != set(other.partitioned_by):
            return False
        if self.table_properties != other.table_properties:
            return False
        if self.comment != other.comment:
            return False
        self_cols = {col.name: col for col in self.columns}
        other_cols = {col.name: col for col in other.columns}
        return self_cols == other_cols

    def __hash__(self) -> int:
        """Hash based on name only for dict/set usage."""
        return hash(self.name)

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
