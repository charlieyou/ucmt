"""Tests for ucmt.schema.models module - TDD tests written first."""

from ucmt.schema.models import (
    CheckConstraint,
    Column,
    DeltaTypeRules,
    ForeignKey,
    PrimaryKey,
    Table,
)


class TestModelsBasic:
    """Basic tests for schema model dataclasses."""

    def test_column_creation(self):
        """Column can be created with required fields."""
        col = Column(name="id", type="BIGINT")
        assert col.name == "id"
        assert col.type == "BIGINT"
        assert col.nullable is True

    def test_table_creation(self):
        """Table can be created with columns."""
        cols = [Column(name="id", type="BIGINT"), Column(name="name", type="STRING")]
        table = Table(name="users", columns=cols)
        assert table.name == "users"
        assert len(table.columns) == 2

    def test_primary_key_creation(self):
        """PrimaryKey can be created with columns and rely flag."""
        pk = PrimaryKey(columns=["id", "tenant_id"], rely=True)
        assert pk.columns == ["id", "tenant_id"]
        assert pk.rely is True

    def test_foreign_key_creation(self):
        """ForeignKey can be created with table and column."""
        fk = ForeignKey(table="users", column="id")
        assert fk.table == "users"
        assert fk.column == "id"

    def test_check_constraint_creation(self):
        """CheckConstraint can be created with name and expression."""
        cc = CheckConstraint(name="positive_amount", expression="amount > 0")
        assert cc.name == "positive_amount"
        assert cc.expression == "amount > 0"


class TestDeltaTypeRulesWidening:
    """Tests for Delta type widening rules."""

    def test_int_to_bigint_allowed(self):
        """INT to BIGINT is a valid widening operation."""
        assert DeltaTypeRules.is_widening("INT", "BIGINT") is True

    def test_bigint_to_int_rejected(self):
        """BIGINT to INT is narrowing and should be rejected."""
        assert DeltaTypeRules.is_widening("BIGINT", "INT") is False

    def test_float_to_double_allowed(self):
        """FLOAT to DOUBLE is a valid widening operation."""
        assert DeltaTypeRules.is_widening("FLOAT", "DOUBLE") is True

    def test_double_to_float_rejected(self):
        """DOUBLE to FLOAT is narrowing and should be rejected."""
        assert DeltaTypeRules.is_widening("DOUBLE", "FLOAT") is False

    def test_tinyint_to_smallint_allowed(self):
        """TINYINT to SMALLINT is a valid widening operation."""
        assert DeltaTypeRules.is_widening("TINYINT", "SMALLINT") is True

    def test_smallint_to_int_allowed(self):
        """SMALLINT to INT is a valid widening operation."""
        assert DeltaTypeRules.is_widening("SMALLINT", "INT") is True

    def test_same_type_is_not_widening(self):
        """Same type to same type is not a widening operation."""
        assert DeltaTypeRules.is_widening("INT", "INT") is False

    def test_case_insensitive(self):
        """Type comparison should be case-insensitive."""
        assert DeltaTypeRules.is_widening("int", "bigint") is True
        assert DeltaTypeRules.is_widening("Int", "BigInt") is True


class TestDecimalPrecisionComparison:
    """Tests for DECIMAL precision/scale change handling."""

    def test_decimal_precision_change_unsupported(self):
        """DECIMAL precision changes are marked as unsupported in v1."""
        assert (
            DeltaTypeRules.is_decimal_change_unsupported(
                "DECIMAL(10,2)", "DECIMAL(12,2)"
            )
            is True
        )

    def test_decimal_scale_change_unsupported(self):
        """DECIMAL scale changes are marked as unsupported in v1."""
        assert (
            DeltaTypeRules.is_decimal_change_unsupported(
                "DECIMAL(10,2)", "DECIMAL(10,4)"
            )
            is True
        )

    def test_decimal_same_precision_scale_not_unsupported(self):
        """Same DECIMAL precision/scale is not a change."""
        assert (
            DeltaTypeRules.is_decimal_change_unsupported(
                "DECIMAL(10,2)", "DECIMAL(10,2)"
            )
            is False
        )

    def test_non_decimal_types_not_unsupported(self):
        """Non-DECIMAL types should not trigger unsupported."""
        assert DeltaTypeRules.is_decimal_change_unsupported("INT", "BIGINT") is False


class TestPrimaryKeyEquality:
    """Tests for PrimaryKey equality - requires columns and rely match."""

    def test_pk_equality_requires_columns_match(self):
        """Two PKs are equal if they have the same columns."""
        pk1 = PrimaryKey(columns=["id", "tenant_id"], rely=False)
        pk2 = PrimaryKey(columns=["id", "tenant_id"], rely=False)
        assert pk1 == pk2

    def test_pk_equality_requires_rely_match(self):
        """Two PKs are NOT equal if rely flag differs."""
        pk1 = PrimaryKey(columns=["id"], rely=True)
        pk2 = PrimaryKey(columns=["id"], rely=False)
        assert pk1 != pk2

    def test_pk_equality_column_order_matters(self):
        """Column order matters for PK equality."""
        pk1 = PrimaryKey(columns=["id", "tenant_id"])
        pk2 = PrimaryKey(columns=["tenant_id", "id"])
        assert pk1 != pk2


class TestColumnOrderNotSignificant:
    """Tests for Table equality - column order should not matter."""

    def test_tables_equal_with_same_columns_different_order(self):
        """Tables with same columns in different order are equal."""
        cols1 = [Column(name="id", type="BIGINT"), Column(name="name", type="STRING")]
        cols2 = [Column(name="name", type="STRING"), Column(name="id", type="BIGINT")]
        table1 = Table(name="users", columns=cols1)
        table2 = Table(name="users", columns=cols2)
        assert table1 == table2

    def test_tables_not_equal_with_different_columns(self):
        """Tables with different columns are not equal."""
        cols1 = [Column(name="id", type="BIGINT")]
        cols2 = [Column(name="id", type="INT")]
        table1 = Table(name="users", columns=cols1)
        table2 = Table(name="users", columns=cols2)
        assert table1 != table2


class TestTypeNormalizationCaseInsensitive:
    """Tests for type normalization - use normalized_type for case-insensitive comparison."""

    def test_column_type_preserved_as_is(self):
        """Column types are preserved (only stripped), use normalized_type for comparison."""
        col = Column(name="id", type="bigint")
        assert col.type == "bigint"
        assert col.normalized_type == "BIGINT"

    def test_column_type_whitespace_stripped(self):
        """Column types have whitespace stripped."""
        col = Column(name="id", type="  BIGINT  ")
        assert col.type == "BIGINT"

    def test_normalized_type_uppercase(self):
        """normalized_type returns uppercase for comparisons."""
        col = Column(name="id", type="VarChar")
        assert col.type == "VarChar"
        assert col.normalized_type == "VARCHAR"

    def test_complex_type_field_names_preserved(self):
        """Complex types like struct preserve field names (case-sensitive)."""
        col = Column(name="data", type="struct<id:int, event_time:timestamp>")
        assert col.type == "struct<id:int, event_time:timestamp>"
        assert col.normalized_type == "STRUCT<ID:INT, EVENT_TIME:TIMESTAMP>"

    def test_decimal_type_preserved(self):
        """DECIMAL types are preserved as-is."""
        col = Column(name="amount", type="decimal(10,2)")
        assert col.type == "decimal(10,2)"
        assert col.normalized_type == "DECIMAL(10,2)"
