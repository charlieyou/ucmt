"""Tests for ucmt.schema.diff module."""

from ucmt.schema.diff import SchemaDiffer
from ucmt.schema.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    PrimaryKey,
    Schema,
    Table,
)
from ucmt.types import ChangeType


def make_schema(*tables: Table) -> Schema:
    """Helper to create Schema from tables."""
    return Schema(tables={t.name: t for t in tables})


def make_table(
    name: str,
    columns: list[Column] | None = None,
    **kwargs,
) -> Table:
    """Helper to create Table with defaults."""
    if columns is None:
        columns = [Column(name="id", type="BIGINT", nullable=False)]
    return Table(name=name, columns=columns, **kwargs)


class TestSchemaDifferCreateTable:
    """Tests for CREATE_TABLE detection."""

    def test_diff_create_new_table(self):
        """New table in target generates CREATE_TABLE change."""
        source = make_schema()
        target = make_schema(make_table("users"))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.CREATE_TABLE
        assert changes[0].table_name == "users"
        assert "table" in changes[0].details

    def test_diff_create_multiple_tables(self):
        """Multiple new tables generate CREATE_TABLE for each."""
        source = make_schema()
        target = make_schema(make_table("users"), make_table("orders"))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        create_changes = [
            c for c in changes if c.change_type == ChangeType.CREATE_TABLE
        ]
        assert len(create_changes) == 2
        table_names = {c.table_name for c in create_changes}
        assert table_names == {"users", "orders"}


class TestSchemaDifferColumns:
    """Tests for column change detection."""

    def test_diff_add_column(self):
        """New column in target generates ADD_COLUMN change."""
        source = make_schema(
            make_table("users", columns=[Column(name="id", type="BIGINT")])
        )
        target = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="email", type="STRING"),
                ],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ADD_COLUMN
        assert changes[0].table_name == "users"
        assert changes[0].details["column"].name == "email"

    def test_diff_drop_column(self):
        """Column in source not in target generates DROP_COLUMN change."""
        source = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="legacy", type="STRING"),
                ],
            )
        )
        target = make_schema(
            make_table("users", columns=[Column(name="id", type="BIGINT")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.DROP_COLUMN
        assert changes[0].table_name == "users"
        assert changes[0].details["column_name"] == "legacy"
        assert changes[0].is_destructive is True

    def test_diff_drop_column_requires_column_mapping(self):
        """DROP_COLUMN change sets requires_column_mapping flag."""
        source = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="legacy", type="STRING"),
                ],
            )
        )
        target = make_schema(
            make_table("users", columns=[Column(name="id", type="BIGINT")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        drop_change = changes[0]
        assert drop_change.requires_column_mapping is True

    def test_diff_column_reorder_no_change(self):
        """Reordering columns should not generate changes."""
        source = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="name", type="STRING"),
                    Column(name="email", type="STRING"),
                ],
            )
        )
        target = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="email", type="STRING"),
                    Column(name="id", type="BIGINT"),
                    Column(name="name", type="STRING"),
                ],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 0


class TestSchemaDifferTypeChanges:
    """Tests for type change validation."""

    def test_diff_type_widening_allowed(self):
        """Widening type changes (INT->BIGINT) are valid."""
        source = make_schema(
            make_table("users", columns=[Column(name="id", type="INT")])
        )
        target = make_schema(
            make_table("users", columns=[Column(name="id", type="BIGINT")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_COLUMN_TYPE
        assert changes[0].is_unsupported is False
        assert changes[0].error_message is None

    def test_diff_type_narrowing_blocked(self):
        """Narrowing type changes (BIGINT->INT) are unsupported."""
        source = make_schema(
            make_table("users", columns=[Column(name="id", type="BIGINT")])
        )
        target = make_schema(
            make_table("users", columns=[Column(name="id", type="INT")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_COLUMN_TYPE
        assert changes[0].is_unsupported is True
        assert changes[0].error_message is not None

    def test_diff_type_string_to_int_blocked(self):
        """Incompatible type changes are unsupported."""
        source = make_schema(
            make_table("users", columns=[Column(name="id", type="STRING")])
        )
        target = make_schema(
            make_table("users", columns=[Column(name="id", type="INT")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].is_unsupported is True

    def test_diff_decimal_change_unsupported(self):
        """DECIMAL precision changes are unsupported."""
        source = make_schema(
            make_table("orders", columns=[Column(name="amount", type="DECIMAL(10,2)")])
        )
        target = make_schema(
            make_table("orders", columns=[Column(name="amount", type="DECIMAL(15,4)")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].is_unsupported is True

    def test_diff_float_to_double_allowed(self):
        """FLOAT to DOUBLE widening is allowed."""
        source = make_schema(
            make_table("metrics", columns=[Column(name="value", type="FLOAT")])
        )
        target = make_schema(
            make_table("metrics", columns=[Column(name="value", type="DOUBLE")])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].is_unsupported is False


class TestSchemaDifferPartitioning:
    """Tests for partition change detection."""

    def test_diff_unsupported_partition_change(self):
        """Partition changes are unsupported in Delta Lake."""
        source = make_schema(make_table("events", partitioned_by=["event_date"]))
        target = make_schema(make_table("events", partitioned_by=["event_month"]))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_PARTITIONING
        assert changes[0].is_unsupported is True
        assert "Cannot change partitioning" in changes[0].error_message


class TestSchemaDifferPrimaryKey:
    """Tests for primary key change detection."""

    def test_diff_primary_key_changes(self):
        """Primary key changes generate DROP + SET changes."""
        source = make_schema(
            make_table(
                "users",
                primary_key=PrimaryKey(columns=["id"]),
            )
        )
        target = make_schema(
            make_table(
                "users",
                primary_key=PrimaryKey(columns=["id", "tenant_id"]),
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        pk_changes = [
            c
            for c in changes
            if c.change_type
            in (ChangeType.SET_PRIMARY_KEY, ChangeType.DROP_PRIMARY_KEY)
        ]
        assert len(pk_changes) == 2

        drop_pk = [
            c for c in pk_changes if c.change_type == ChangeType.DROP_PRIMARY_KEY
        ]
        set_pk = [c for c in pk_changes if c.change_type == ChangeType.SET_PRIMARY_KEY]
        assert len(drop_pk) == 1
        assert len(set_pk) == 1

    def test_diff_add_primary_key(self):
        """Adding a primary key generates SET_PRIMARY_KEY."""
        source = make_schema(make_table("users", primary_key=None))
        target = make_schema(
            make_table("users", primary_key=PrimaryKey(columns=["id"]))
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.SET_PRIMARY_KEY

    def test_diff_drop_primary_key(self):
        """Dropping a primary key generates DROP_PRIMARY_KEY."""
        source = make_schema(
            make_table("users", primary_key=PrimaryKey(columns=["id"]))
        )
        target = make_schema(make_table("users", primary_key=None))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.DROP_PRIMARY_KEY


class TestSchemaDifferCheckConstraints:
    """Tests for CHECK constraint detection."""

    def test_diff_check_constraints_add(self):
        """New CHECK constraint generates ADD_CHECK_CONSTRAINT."""
        source = make_schema(make_table("users"))
        target = make_schema(
            make_table(
                "users",
                check_constraints=[
                    CheckConstraint(name="age_check", expression="age >= 0")
                ],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ADD_CHECK_CONSTRAINT

    def test_diff_check_constraints_drop(self):
        """Removed CHECK constraint generates DROP_CHECK_CONSTRAINT."""
        source = make_schema(
            make_table(
                "users",
                check_constraints=[
                    CheckConstraint(name="age_check", expression="age >= 0")
                ],
            )
        )
        target = make_schema(make_table("users"))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.DROP_CHECK_CONSTRAINT


class TestSchemaDifferTableProperties:
    """Tests for table properties detection."""

    def test_diff_table_properties_only_set_never_drop(self):
        """Properties are set/overwritten but never dropped."""
        source = make_schema(
            make_table(
                "users",
                table_properties={
                    "delta.enableChangeDataFeed": "true",
                    "custom.prop": "x",
                },
            )
        )
        target = make_schema(
            make_table(
                "users",
                table_properties={"delta.enableChangeDataFeed": "false"},
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        prop_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_TABLE_PROPERTIES
        ]
        assert len(prop_changes) == 1
        assert prop_changes[0].details["properties"] == {
            "delta.enableChangeDataFeed": "false"
        }

    def test_diff_add_table_properties(self):
        """New property generates ALTER_TABLE_PROPERTIES."""
        source = make_schema(make_table("users", table_properties={}))
        target = make_schema(
            make_table("users", table_properties={"delta.minReaderVersion": "2"})
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_TABLE_PROPERTIES
        assert "delta.minReaderVersion" in changes[0].details["properties"]


class TestSchemaDifferLiquidClustering:
    """Tests for liquid clustering detection."""

    def test_diff_liquid_clustering(self):
        """Liquid clustering changes generate ALTER_CLUSTERING."""
        source = make_schema(make_table("events", liquid_clustering=["event_type"]))
        target = make_schema(
            make_table("events", liquid_clustering=["event_type", "region"])
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_CLUSTERING
        assert changes[0].details["to_columns"] == ["event_type", "region"]

    def test_diff_add_liquid_clustering(self):
        """Adding liquid clustering generates ALTER_CLUSTERING."""
        source = make_schema(make_table("events", liquid_clustering=[]))
        target = make_schema(make_table("events", liquid_clustering=["event_date"]))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.ALTER_CLUSTERING


class TestSchemaDifferNoChanges:
    """Tests for empty diff scenarios."""

    def test_diff_no_changes_is_empty(self):
        """No changes returns empty list."""
        table = make_table(
            "users",
            columns=[
                Column(name="id", type="BIGINT"),
                Column(name="name", type="STRING"),
            ],
            primary_key=PrimaryKey(columns=["id"]),
        )
        source = make_schema(table)
        target = make_schema(table)

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert changes == []

    def test_diff_identical_schemas_returns_empty(self):
        """Identical multi-table schemas return empty list."""
        tables = [
            make_table("users", columns=[Column(name="id", type="BIGINT")]),
            make_table("orders", columns=[Column(name="id", type="BIGINT")]),
        ]
        source = make_schema(*tables)
        target = make_schema(*tables)

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert changes == []


class TestSchemaDifferIgnoresTables:
    """Tests for table filtering behavior."""

    def test_diff_ignores_extra_db_tables(self):
        """Extra tables in source (DB) do NOT generate DROP_TABLE in v1."""
        source = make_schema(
            make_table("users"),
            make_table("_temp_staging"),
        )
        target = make_schema(make_table("users"))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        drop_changes = [c for c in changes if c.change_type == ChangeType.DROP_TABLE]
        assert len(drop_changes) == 0


class TestSchemaDifferForeignKeys:
    """Tests for foreign key handling."""

    def test_diff_foreign_key_changes_ignored(self):
        """Foreign key changes are ignored in v1 (informational only)."""
        source = make_schema(
            make_table(
                "orders",
                columns=[
                    Column(
                        name="user_id",
                        type="BIGINT",
                        foreign_key=ForeignKey(table="users", column="id"),
                    )
                ],
            )
        )
        target = make_schema(
            make_table(
                "orders",
                columns=[
                    Column(
                        name="user_id",
                        type="BIGINT",
                        foreign_key=ForeignKey(table="customers", column="id"),
                    )
                ],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        fk_changes = [
            c
            for c in changes
            if c.change_type
            in (ChangeType.ADD_FOREIGN_KEY, ChangeType.DROP_FOREIGN_KEY)
        ]
        assert len(fk_changes) == 0


class TestSchemaDifferOrdering:
    """Tests for deterministic change ordering."""

    def test_diff_ordering_deterministic(self):
        """Changes are ordered: creates first, then alters, then drops."""
        source = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="legacy", type="STRING"),
                ],
            )
        )
        target = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="email", type="STRING"),
                ],
            ),
            make_table("orders"),
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        change_types = [c.change_type for c in changes]
        create_idx = change_types.index(ChangeType.CREATE_TABLE)
        add_idx = change_types.index(ChangeType.ADD_COLUMN)
        drop_idx = change_types.index(ChangeType.DROP_COLUMN)

        assert create_idx < add_idx < drop_idx

    def test_diff_ordering_by_table_name(self):
        """Same change types are ordered by table name."""
        source = make_schema()
        target = make_schema(
            make_table("zebra"),
            make_table("alpha"),
            make_table("middle"),
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        table_names = [c.table_name for c in changes]
        assert table_names == sorted(table_names)

    def test_diff_add_multiple_columns_ordered_by_column_name(self):
        """Multiple ADD_COLUMN changes on same table are ordered by column name."""
        source = make_schema(
            make_table("users", columns=[Column(name="id", type="BIGINT")])
        )
        target = make_schema(
            make_table(
                "users",
                columns=[
                    Column(name="id", type="BIGINT"),
                    Column(name="b", type="STRING"),
                    Column(name="a", type="STRING"),
                    Column(name="c", type="STRING"),
                ],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        add_changes = [c for c in changes if c.change_type == ChangeType.ADD_COLUMN]
        col_names = [c.details["column_name"] for c in add_changes]
        assert col_names == ["a", "b", "c"]


class TestSchemaDifferNullabilityAndDefaults:
    """Tests for nullability and default changes."""

    def test_diff_column_nullability_change(self):
        """Nullability change generates ALTER_COLUMN_NULLABILITY."""
        source = make_schema(
            make_table(
                "users",
                columns=[Column(name="id", type="BIGINT", nullable=False)],
            )
        )
        target = make_schema(
            make_table(
                "users",
                columns=[Column(name="id", type="BIGINT", nullable=True)],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        change = changes[0]
        assert change.change_type == ChangeType.ALTER_COLUMN_NULLABILITY
        assert change.details["column_name"] == "id"
        assert change.details["from_nullable"] is False
        assert change.details["to_nullable"] is True

    def test_diff_column_default_change(self):
        """Default change generates ALTER_COLUMN_DEFAULT."""
        source = make_schema(
            make_table(
                "users",
                columns=[Column(name="id", type="BIGINT", default="0")],
            )
        )
        target = make_schema(
            make_table(
                "users",
                columns=[Column(name="id", type="BIGINT", default="1")],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        assert len(changes) == 1
        change = changes[0]
        assert change.change_type == ChangeType.ALTER_COLUMN_DEFAULT
        assert change.details["column_name"] == "id"
        assert change.details["from_default"] == "0"
        assert change.details["to_default"] == "1"


class TestSchemaDifferForeignKeysExtended:
    """Extended tests for foreign key handling."""

    def test_diff_add_foreign_key_ignored(self):
        """Adding a foreign key is ignored in v1."""
        source = make_schema(
            make_table(
                "orders",
                columns=[Column(name="user_id", type="BIGINT")],
            )
        )
        target = make_schema(
            make_table(
                "orders",
                columns=[
                    Column(
                        name="user_id",
                        type="BIGINT",
                        foreign_key=ForeignKey(table="users", column="id"),
                    )
                ],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        fk_changes = [
            c
            for c in changes
            if c.change_type
            in (ChangeType.ADD_FOREIGN_KEY, ChangeType.DROP_FOREIGN_KEY)
        ]
        assert fk_changes == []

    def test_diff_drop_foreign_key_ignored(self):
        """Dropping a foreign key is ignored in v1."""
        source = make_schema(
            make_table(
                "orders",
                columns=[
                    Column(
                        name="user_id",
                        type="BIGINT",
                        foreign_key=ForeignKey(table="users", column="id"),
                    )
                ],
            )
        )
        target = make_schema(
            make_table(
                "orders",
                columns=[Column(name="user_id", type="BIGINT")],
            )
        )

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        fk_changes = [
            c
            for c in changes
            if c.change_type
            in (ChangeType.ADD_FOREIGN_KEY, ChangeType.DROP_FOREIGN_KEY)
        ]
        assert fk_changes == []


class TestSchemaDifferPartitioningExtended:
    """Extended tests for partitioning behavior."""

    def test_diff_partition_order_no_change(self):
        """Different partition column order with same columns yields no change."""
        source = make_schema(make_table("events", partitioned_by=["ds", "region"]))
        target = make_schema(make_table("events", partitioned_by=["region", "ds"]))

        differ = SchemaDiffer()
        changes = differ.diff(source, target)

        partition_changes = [
            c for c in changes if c.change_type == ChangeType.ALTER_PARTITIONING
        ]
        assert partition_changes == []
