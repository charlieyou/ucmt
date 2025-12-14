"""Schema validation: compare DB state against YAML schema."""

from dataclasses import dataclass
from typing import Literal, Optional

from ucmt.schema.models import Schema


@dataclass
class ValidationIssue:
    """A single validation issue found during schema comparison."""

    table: Optional[str]
    column: Optional[str]
    kind: Literal[
        "missing_table", "missing_column", "type_mismatch", "constraint_mismatch"
    ]
    message: str


@dataclass
class ValidationResult:
    """Result of schema validation.

    ok is True iff issues is empty. CLI uses this flag for exit code (0 if ok, 1 otherwise).
    """

    ok: bool
    issues: list[ValidationIssue]


class SchemaValidator:
    """Validate DB schema matches YAML schema definition.

    Only YAML-defined tables and columns are validated. Extra tables/columns
    present in the DB but not in YAML are ignored (they do not cause failure).
    """

    def validate(self, yaml_schema: Schema, db_schema: Schema) -> ValidationResult:
        """Compare YAML schema against DB schema and return validation result.

        Args:
            yaml_schema: The expected schema from YAML definitions
            db_schema: The actual schema introspected from database

        Returns:
            ValidationResult with ok=True if schemas match, issues list otherwise
        """
        issues: list[ValidationIssue] = []

        for table_name, yaml_table in yaml_schema.tables.items():
            db_table = db_schema.get_table(table_name)

            if db_table is None:
                issues.append(
                    ValidationIssue(
                        table=table_name,
                        column=None,
                        kind="missing_table",
                        message=f"Table '{table_name}' not found in database",
                    )
                )
                continue

            yaml_cols = {col.name: col for col in yaml_table.columns}
            db_cols = {col.name: col for col in db_table.columns}

            for col_name, yaml_col in yaml_cols.items():
                db_col = db_cols.get(col_name)

                if db_col is None:
                    issues.append(
                        ValidationIssue(
                            table=table_name,
                            column=col_name,
                            kind="missing_column",
                            message=f"Column '{col_name}' missing from table '{table_name}'",
                        )
                    )
                    continue

                if yaml_col.normalized_type != db_col.normalized_type:
                    issues.append(
                        ValidationIssue(
                            table=table_name,
                            column=col_name,
                            kind="type_mismatch",
                            message=f"Column '{col_name}' type mismatch: expected {yaml_col.type}, got {db_col.type}",
                        )
                    )
                    continue

                if yaml_col.nullable != db_col.nullable:
                    expected = "nullable" if yaml_col.nullable else "NOT NULL"
                    actual = "nullable" if db_col.nullable else "NOT NULL"
                    issues.append(
                        ValidationIssue(
                            table=table_name,
                            column=col_name,
                            kind="constraint_mismatch",
                            message=f"Column '{col_name}' nullable mismatch: expected {expected}, got {actual}",
                        )
                    )

        return ValidationResult(ok=len(issues) == 0, issues=issues)
