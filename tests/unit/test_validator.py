"""TDD tests for SchemaValidator - written FIRST before implementation."""

from ucmt.schema.models import (
    CheckConstraint,
    Column,
    PrimaryKey,
    Schema,
    Table,
)
from ucmt.schema.validator import (
    SchemaValidator,
    ValidationIssue,
    ValidationResult,
)


def make_table(
    name: str,
    columns: list[Column] | None = None,
    primary_key: PrimaryKey | None = None,
    check_constraints: list[CheckConstraint] | None = None,
    comment: str | None = None,
) -> Table:
    """Helper to create a Table with defaults."""
    return Table(
        name=name,
        columns=columns or [],
        primary_key=primary_key,
        check_constraints=check_constraints or [],
        comment=comment,
    )


def make_column(
    name: str,
    col_type: str = "STRING",
    nullable: bool = True,
    comment: str | None = None,
) -> Column:
    """Helper to create a Column with defaults."""
    return Column(name=name, type=col_type, nullable=nullable, comment=comment)


class TestValidatePassesWhenDbMatchesYaml:
    """Test validation passes when DB matches YAML schema."""

    def test_validate_passes_when_db_matches_yaml(self):
        """Validation should pass when DB schema matches YAML exactly."""
        yaml_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[
                        make_column("id", "BIGINT", nullable=False),
                        make_column("email", "STRING", nullable=False),
                    ],
                )
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[
                        make_column("id", "BIGINT", nullable=False),
                        make_column("email", "STRING", nullable=False),
                    ],
                )
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is True
        assert result.issues == []


class TestValidateFailsOnMissingColumn:
    """Test validation fails when DB is missing a column from YAML."""

    def test_validate_fails_on_missing_column(self):
        """Validation should fail when DB is missing a column defined in YAML."""
        yaml_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[
                        make_column("id", "BIGINT"),
                        make_column("email", "STRING"),
                        make_column("name", "STRING"),
                    ],
                )
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[
                        make_column("id", "BIGINT"),
                        make_column("email", "STRING"),
                    ],
                )
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is False
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.table == "users"
        assert issue.column == "name"
        assert issue.kind == "missing_column"


class TestValidateFailsOnTypeMismatch:
    """Test validation fails when column types don't match."""

    def test_validate_fails_on_type_mismatch(self):
        """Validation should fail when DB column type differs from YAML."""
        yaml_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[
                        make_column("id", "BIGINT"),
                        make_column("age", "INT"),
                    ],
                )
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[
                        make_column("id", "BIGINT"),
                        make_column("age", "STRING"),
                    ],
                )
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is False
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.table == "users"
        assert issue.column == "age"
        assert issue.kind == "type_mismatch"
        assert "INT" in issue.message and "STRING" in issue.message


class TestValidateFailsOnMissingTable:
    """Test validation fails when DB is missing a table from YAML."""

    def test_validate_fails_on_missing_table(self):
        """Validation should fail when DB is missing a table defined in YAML."""
        yaml_schema = Schema(
            tables={
                "users": make_table("users", columns=[make_column("id", "BIGINT")]),
                "orders": make_table("orders", columns=[make_column("id", "BIGINT")]),
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table("users", columns=[make_column("id", "BIGINT")]),
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is False
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.table == "orders"
        assert issue.column is None
        assert issue.kind == "missing_table"


class TestValidateIgnoresExtraSystemTables:
    """Test validation ignores extra tables in DB not in YAML."""

    def test_validate_ignores_extra_system_tables(self):
        """Extra tables in DB (like system tables) should not cause validation failure."""
        yaml_schema = Schema(
            tables={
                "users": make_table("users", columns=[make_column("id", "BIGINT")]),
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table("users", columns=[make_column("id", "BIGINT")]),
                "_ucmt_migrations": make_table(
                    "_ucmt_migrations", columns=[make_column("version", "STRING")]
                ),
                "__apply_changes_storage": make_table(
                    "__apply_changes_storage", columns=[make_column("data", "STRING")]
                ),
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is True
        assert result.issues == []


class TestValidateIgnoresCommentDifferences:
    """Test validation ignores comment differences."""

    def test_validate_ignores_comment_differences(self):
        """Comment differences should not cause validation failure."""
        yaml_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[make_column("id", "BIGINT", comment="User ID")],
                    comment="User accounts",
                )
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[make_column("id", "BIGINT", comment="Different comment")],
                    comment="Different table comment",
                )
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is True
        assert result.issues == []


class TestValidateReturnsValidationResult:
    """Test that validate always returns a ValidationResult."""

    def test_validate_returns_validation_result(self):
        """validate() should always return a ValidationResult dataclass."""
        yaml_schema = Schema(tables={})
        db_schema = Schema(tables={})

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert isinstance(result, ValidationResult)
        assert isinstance(result.ok, bool)
        assert isinstance(result.issues, list)

    def test_validation_result_ok_is_true_iff_issues_empty(self):
        """ValidationResult.ok should be True iff issues list is empty."""
        result_ok = ValidationResult(ok=True, issues=[])
        result_fail = ValidationResult(
            ok=False,
            issues=[
                ValidationIssue(
                    table="t", column=None, kind="missing_table", message="Missing"
                )
            ],
        )

        assert result_ok.ok is True
        assert len(result_ok.issues) == 0
        assert result_fail.ok is False
        assert len(result_fail.issues) > 0


class TestValidationIssueDataclass:
    """Test ValidationIssue dataclass structure."""

    def test_validation_issue_has_required_fields(self):
        """ValidationIssue should have table, column, kind, and message fields."""
        issue = ValidationIssue(
            table="users",
            column="email",
            kind="missing_column",
            message="Column 'email' missing from table 'users'",
        )

        assert issue.table == "users"
        assert issue.column == "email"
        assert issue.kind == "missing_column"
        assert issue.message == "Column 'email' missing from table 'users'"

    def test_validation_issue_column_can_be_none(self):
        """ValidationIssue.column should be Optional for table-level issues."""
        issue = ValidationIssue(
            table="orders",
            column=None,
            kind="missing_table",
            message="Table 'orders' not found in database",
        )

        assert issue.table == "orders"
        assert issue.column is None


class TestValidateConstraintMismatch:
    """Test validation detects constraint mismatches."""

    def test_validate_fails_on_nullable_mismatch(self):
        """Validation should fail when nullable constraint doesn't match."""
        yaml_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[make_column("id", "BIGINT", nullable=False)],
                )
            }
        )
        db_schema = Schema(
            tables={
                "users": make_table(
                    "users",
                    columns=[make_column("id", "BIGINT", nullable=True)],
                )
            }
        )

        validator = SchemaValidator()
        result = validator.validate(yaml_schema, db_schema)

        assert result.ok is False
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.kind == "constraint_mismatch"
        assert issue.column == "id"
