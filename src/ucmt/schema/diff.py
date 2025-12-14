"""Compare schemas and generate changes."""

from dataclasses import dataclass, field
from typing import Any, Optional

from ucmt.schema.models import Column, Schema, Table
from ucmt.types import ChangeType


@dataclass
class SchemaChange:
    """Represents a single schema change."""

    change_type: ChangeType
    table_name: str
    details: dict[str, Any] = field(default_factory=dict)
    is_destructive: bool = False
    is_unsupported: bool = False
    requires_column_mapping: bool = False
    error_message: Optional[str] = None


class SchemaDiffer:
    """Compare two schemas and generate list of changes."""

    def diff(self, source: Schema, target: Schema) -> list[SchemaChange]:
        """Compare source (current DB) to target (declared) and return changes."""
        changes: list[SchemaChange] = []

        source_tables = source.table_names()
        target_tables = target.table_names()

        for table_name in target_tables - source_tables:
            table = target.get_table(table_name)
            changes.append(
                SchemaChange(
                    change_type=ChangeType.CREATE_TABLE,
                    table_name=table_name,
                    details={"table": table},
                )
            )

        for table_name in source_tables - target_tables:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.DROP_TABLE,
                    table_name=table_name,
                    is_destructive=True,
                )
            )

        for table_name in source_tables & target_tables:
            source_table = source.get_table(table_name)
            target_table = target.get_table(table_name)
            changes.extend(self._diff_table(source_table, target_table))

        return self._order_changes(changes)

    def _diff_table(self, source: Table, target: Table) -> list[SchemaChange]:
        """Compare two tables and return changes."""
        changes: list[SchemaChange] = []

        changes.extend(self._diff_columns(source, target))
        changes.extend(self._diff_constraints(source, target))
        changes.extend(self._diff_clustering(source, target))
        changes.extend(self._diff_partitioning(source, target))
        changes.extend(self._diff_properties(source, target))

        return changes

    def _diff_columns(self, source: Table, target: Table) -> list[SchemaChange]:
        """Compare columns between tables."""
        changes: list[SchemaChange] = []
        table_name = source.name

        source_cols = {c.name: c for c in source.columns}
        target_cols = {c.name: c for c in target.columns}

        for col_name in set(target_cols) - set(source_cols):
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ADD_COLUMN,
                    table_name=table_name,
                    details={"column": target_cols[col_name]},
                )
            )

        for col_name in set(source_cols) - set(target_cols):
            changes.append(
                SchemaChange(
                    change_type=ChangeType.DROP_COLUMN,
                    table_name=table_name,
                    details={"column_name": col_name},
                    is_destructive=True,
                    requires_column_mapping=True,
                )
            )

        for col_name in set(source_cols) & set(target_cols):
            changes.extend(
                self._diff_column(
                    table_name, source_cols[col_name], target_cols[col_name]
                )
            )

        return changes

    def _diff_column(
        self, table_name: str, source: Column, target: Column
    ) -> list[SchemaChange]:
        """Compare two columns and return changes."""
        changes: list[SchemaChange] = []

        if source.type.upper() != target.type.upper():
            valid, error = self._validate_type_change(source.type, target.type)
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ALTER_COLUMN_TYPE,
                    table_name=table_name,
                    details={
                        "column_name": source.name,
                        "from_type": source.type,
                        "to_type": target.type,
                    },
                    is_unsupported=not valid,
                    error_message=error,
                )
            )

        if source.nullable != target.nullable:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ALTER_COLUMN_NULLABILITY,
                    table_name=table_name,
                    details={
                        "column_name": source.name,
                        "from_nullable": source.nullable,
                        "to_nullable": target.nullable,
                    },
                )
            )

        if source.default != target.default:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ALTER_COLUMN_DEFAULT,
                    table_name=table_name,
                    details={
                        "column_name": source.name,
                        "from_default": source.default,
                        "to_default": target.default,
                    },
                )
            )

        return changes

    def _validate_type_change(
        self, from_type: str, to_type: str
    ) -> tuple[bool, Optional[str]]:
        """Validate if a type change is supported in Delta Lake."""
        widening_allowed = {
            ("INT", "BIGINT"),
            ("SMALLINT", "INT"),
            ("SMALLINT", "BIGINT"),
            ("TINYINT", "SMALLINT"),
            ("TINYINT", "INT"),
            ("TINYINT", "BIGINT"),
            ("FLOAT", "DOUBLE"),
        }

        from_upper = from_type.upper().split("(")[0]
        to_upper = to_type.upper().split("(")[0]

        if (from_upper, to_upper) in widening_allowed:
            return True, None

        if from_upper == to_upper:
            return True, None

        return False, (
            f"Type change from {from_type} to {to_type} is not supported. "
            "Only widening conversions are allowed."
        )

    def _diff_clustering(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff liquid clustering configuration."""
        changes: list[SchemaChange] = []

        if set(source.liquid_clustering) != set(target.liquid_clustering):
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ALTER_CLUSTERING,
                    table_name=source.name,
                    details={
                        "from_columns": source.liquid_clustering,
                        "to_columns": target.liquid_clustering,
                    },
                )
            )

        return changes

    def _diff_partitioning(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff partitioning - generates ERROR if different."""
        changes: list[SchemaChange] = []

        if set(source.partitioned_by) != set(target.partitioned_by):
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ALTER_PARTITIONING,
                    table_name=source.name,
                    details={
                        "from_columns": source.partitioned_by,
                        "to_columns": target.partitioned_by,
                    },
                    is_unsupported=True,
                    error_message=(
                        f"Cannot change partitioning for table '{source.name}'. "
                        f"Current: {source.partitioned_by}, Desired: {target.partitioned_by}. "
                        "Delta Lake does not support changing partition columns. "
                        "You must recreate the table."
                    ),
                )
            )

        return changes

    def _diff_constraints(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff constraints between tables."""
        changes: list[SchemaChange] = []

        if source.primary_key != target.primary_key:
            if source.primary_key:
                changes.append(
                    SchemaChange(
                        change_type=ChangeType.DROP_PRIMARY_KEY,
                        table_name=source.name,
                        details={"constraint": source.primary_key},
                    )
                )
            if target.primary_key:
                changes.append(
                    SchemaChange(
                        change_type=ChangeType.SET_PRIMARY_KEY,
                        table_name=source.name,
                        details={"constraint": target.primary_key},
                    )
                )

        source_checks = {c.name: c for c in source.check_constraints}
        target_checks = {c.name: c for c in target.check_constraints}

        for name in set(target_checks) - set(source_checks):
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ADD_CHECK_CONSTRAINT,
                    table_name=source.name,
                    details={"constraint": target_checks[name]},
                )
            )

        for name in set(source_checks) - set(target_checks):
            changes.append(
                SchemaChange(
                    change_type=ChangeType.DROP_CHECK_CONSTRAINT,
                    table_name=source.name,
                    details={"constraint_name": name},
                )
            )

        return changes

    def _diff_properties(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff table properties."""
        changes: list[SchemaChange] = []

        changed_props = {}
        for key, value in target.table_properties.items():
            if source.table_properties.get(key) != value:
                changed_props[key] = value

        if changed_props:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ALTER_TABLE_PROPERTIES,
                    table_name=source.name,
                    details={"properties": changed_props},
                )
            )

        return changes

    def _order_changes(self, changes: list[SchemaChange]) -> list[SchemaChange]:
        """Order changes by dependency (creates first, drops last)."""
        order = {
            ChangeType.CREATE_TABLE: 0,
            ChangeType.ADD_COLUMN: 1,
            ChangeType.ALTER_COLUMN_TYPE: 2,
            ChangeType.ALTER_COLUMN_NULLABILITY: 2,
            ChangeType.ALTER_COLUMN_DEFAULT: 2,
            ChangeType.SET_PRIMARY_KEY: 3,
            ChangeType.ADD_FOREIGN_KEY: 3,
            ChangeType.ADD_CHECK_CONSTRAINT: 3,
            ChangeType.ALTER_CLUSTERING: 4,
            ChangeType.ALTER_TABLE_PROPERTIES: 4,
            ChangeType.DROP_CHECK_CONSTRAINT: 5,
            ChangeType.DROP_FOREIGN_KEY: 5,
            ChangeType.DROP_PRIMARY_KEY: 5,
            ChangeType.DROP_COLUMN: 6,
            ChangeType.DROP_TABLE: 7,
        }
        return sorted(changes, key=lambda c: order.get(c.change_type, 99))
