"""Generate SQL migrations from schema changes."""

from datetime import datetime

from ucmt.exceptions import CodegenError
from ucmt.schema.diff import SchemaChange
from ucmt.schema.models import CheckConstraint, Column, PrimaryKey, Table
from ucmt.types import ChangeType


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


class MigrationGenerator:
    """Generate SQL migration files from schema changes."""

    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema

    def generate(self, changes: list[SchemaChange], description: str) -> str:
        """Generate SQL migration file content."""
        errors = [c for c in changes if c.is_unsupported]
        if errors:
            error_msgs = "\n".join(f"-- ERROR: {c.error_message}" for c in errors)
            raise CodegenError(
                f"Cannot generate migration - unsupported changes:\n{error_msgs}"
            )

        lines = [
            "-- Migration: Auto-generated",
            f"-- Description: {description}",
            f"-- Generated: {datetime.now().isoformat()}",
            "",
            "-- Variable substitution: ${catalog}, ${schema}",
            "",
        ]

        destructive = [c for c in changes if c.is_destructive]
        if destructive:
            lines.append("-- WARNING: This migration contains destructive changes:")
            for c in destructive:
                lines.append(f"--   - {c.change_type.value}: {c.table_name}")
            lines.append("")

        for change in changes:
            lines.append(f"-- {change.change_type.value}: {change.table_name}")
            if change.requires_column_mapping:
                lines.append("-- Requires: delta.columnMapping.mode = 'name'")
            sql = self._generate_sql(change)
            lines.append(sql)
            lines.append("")

        return "\n".join(lines)

    def _generate_sql(self, change: SchemaChange) -> str:
        """Generate SQL for a single change."""
        generators = {
            ChangeType.CREATE_TABLE: self._gen_create_table,
            ChangeType.DROP_TABLE: self._gen_drop_table,
            ChangeType.ADD_COLUMN: self._gen_add_column,
            ChangeType.DROP_COLUMN: self._gen_drop_column,
            ChangeType.ALTER_COLUMN_TYPE: self._gen_alter_column_type,
            ChangeType.ALTER_COLUMN_NULLABILITY: self._gen_alter_nullability,
            ChangeType.ALTER_COLUMN_DEFAULT: self._gen_alter_default,
            ChangeType.ADD_CHECK_CONSTRAINT: self._gen_add_check,
            ChangeType.DROP_CHECK_CONSTRAINT: self._gen_drop_check,
            ChangeType.SET_PRIMARY_KEY: self._gen_set_pk,
            ChangeType.DROP_PRIMARY_KEY: self._gen_drop_pk,
            ChangeType.ALTER_CLUSTERING: self._gen_alter_clustering,
            ChangeType.ALTER_TABLE_PROPERTIES: self._gen_alter_properties,
        }
        generator = generators.get(change.change_type)
        if not generator:
            raise CodegenError(f"No generator for {change.change_type}")
        return generator(change)

    def _fqn(self, table_name: str) -> str:
        """Generate fully-qualified table name with variables."""
        return f"${{catalog}}.${{schema}}.{table_name}"

    def _gen_create_table(self, change: SchemaChange) -> str:
        """Generate CREATE TABLE statement."""
        table: Table = change.details["table"]
        fqn = self._fqn(table.name)

        col_defs = []
        for col in table.columns:
            col_def = f"    {col.name} {col.type}"
            if col.generated:
                col_def += f" GENERATED {col.generated}"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.default:
                col_def += f" DEFAULT {col.default}"
            if col.comment:
                col_def += f" COMMENT '{_escape_sql_string(col.comment)}'"
            col_defs.append(col_def)

        if table.primary_key:
            pk_cols = ", ".join(table.primary_key.columns)
            rely = " RELY" if table.primary_key.rely else " NORELY"
            col_defs.append(
                f"    CONSTRAINT pk_{table.name} PRIMARY KEY ({pk_cols}){rely}"
            )

        columns_sql = ",\n".join(col_defs)
        sql = f"CREATE TABLE IF NOT EXISTS {fqn} (\n{columns_sql}\n) USING DELTA"

        if table.liquid_clustering:
            cols = ", ".join(table.liquid_clustering)
            sql += f"\nCLUSTER BY ({cols})"
        elif table.partitioned_by:
            cols = ", ".join(table.partitioned_by)
            sql += f"\nPARTITIONED BY ({cols})"

        if table.table_properties:
            props = ", ".join(
                f"'{k}' = '{_escape_sql_string(str(v))}'"
                for k, v in table.table_properties.items()
            )
            sql += f"\nTBLPROPERTIES ({props})"

        if table.comment:
            sql += f"\nCOMMENT '{_escape_sql_string(table.comment)}'"

        return sql + ";"

    def _gen_add_column(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ADD COLUMN."""
        col: Column = change.details["column"]
        fqn = self._fqn(change.table_name)

        col_def = f"{col.name} {col.type}"
        if not col.nullable:
            if not col.default:
                raise CodegenError(
                    f"Cannot add non-nullable column '{col.name}' without a default."
                )
            col_def += " NOT NULL"
        if col.default:
            col_def += f" DEFAULT {col.default}"
        if col.comment:
            col_def += f" COMMENT '{_escape_sql_string(col.comment)}'"

        return f"ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {col_def};"

    def _gen_drop_column(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE DROP COLUMN."""
        col_name = change.details["column_name"]
        fqn = self._fqn(change.table_name)

        return f"ALTER TABLE {fqn} DROP COLUMN IF EXISTS {col_name};"

    def _gen_alter_column_type(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ALTER COLUMN TYPE."""
        fqn = self._fqn(change.table_name)
        col_name = change.details["column_name"]
        to_type = change.details["to_type"]

        return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} TYPE {to_type};"

    def _gen_alter_nullability(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET/DROP NOT NULL."""
        fqn = self._fqn(change.table_name)
        col_name = change.details["column_name"]
        to_nullable = change.details["to_nullable"]

        if to_nullable:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} DROP NOT NULL;"
        else:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} SET NOT NULL;"

    def _gen_alter_default(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET/DROP DEFAULT."""
        fqn = self._fqn(change.table_name)
        col_name = change.details["column_name"]
        to_default = change.details["to_default"]

        if to_default:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} SET DEFAULT {to_default};"
        else:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} DROP DEFAULT;"

    def _gen_add_check(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ADD CONSTRAINT CHECK."""
        fqn = self._fqn(change.table_name)
        constraint: CheckConstraint = change.details["constraint"]

        return f"ALTER TABLE {fqn} ADD CONSTRAINT {constraint.name} CHECK ({constraint.expression});"

    def _gen_drop_check(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT."""
        fqn = self._fqn(change.table_name)
        name = change.details["constraint_name"]

        return f"ALTER TABLE {fqn} DROP CONSTRAINT IF EXISTS {name};"

    def _gen_set_pk(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ADD PRIMARY KEY."""
        fqn = self._fqn(change.table_name)
        pk: PrimaryKey = change.details["constraint"]
        cols = ", ".join(pk.columns)
        rely = " RELY" if pk.rely else " NORELY"

        return f"ALTER TABLE {fqn} ADD CONSTRAINT pk_{change.table_name} PRIMARY KEY ({cols}){rely};"

    def _gen_drop_pk(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE DROP PRIMARY KEY."""
        fqn = self._fqn(change.table_name)
        return f"ALTER TABLE {fqn} DROP PRIMARY KEY IF EXISTS;"

    def _gen_alter_clustering(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE CLUSTER BY."""
        fqn = self._fqn(change.table_name)
        to_cols = change.details["to_columns"]

        if not to_cols:
            return f"ALTER TABLE {fqn} CLUSTER BY NONE;\n-- Note: Run OPTIMIZE to apply clustering changes"

        cols = ", ".join(to_cols)
        return f"ALTER TABLE {fqn} CLUSTER BY ({cols});\n-- Note: Run OPTIMIZE to apply clustering changes"

    def _gen_alter_properties(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE SET TBLPROPERTIES."""
        fqn = self._fqn(change.table_name)
        props = change.details["properties"]

        props_sql = ", ".join(
            f"'{k}' = '{_escape_sql_string(str(v))}'" for k, v in props.items()
        )
        return f"ALTER TABLE {fqn} SET TBLPROPERTIES ({props_sql});"

    def _gen_drop_table(self, change: SchemaChange) -> str:
        """Generate DROP TABLE (commented out for safety)."""
        fqn = self._fqn(change.table_name)

        return f"-- DROP TABLE IF EXISTS {fqn};"
