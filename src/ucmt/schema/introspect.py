"""Read current database state from Unity Catalog."""

from typing import Any, Optional

from ucmt.schema.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    PrimaryKey,
    Schema,
    Table,
)


class SchemaIntrospector:
    """Read current schema state from Databricks Unity Catalog."""

    def __init__(self, spark: Any, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def introspect(self) -> Schema:
        """Query information_schema to build Schema object."""
        tables = {}
        for table_name in self._get_table_names():
            tables[table_name] = self._introspect_table(table_name)
        return Schema(tables=tables)

    def _get_table_names(self) -> list[str]:
        """Get all table names in schema (excluding internal tables)."""
        query = f"""
            SELECT table_name
            FROM {self.catalog}.information_schema.tables
            WHERE table_schema = '{self.schema}'
              AND table_type IN ('MANAGED', 'EXTERNAL')
              AND table_name NOT LIKE '\\_%'
        """
        return [row.table_name for row in self.spark.sql(query).collect()]

    def _introspect_table(self, table_name: str) -> Table:
        """Build Table object from information_schema + DESCRIBE queries."""
        columns = self._get_columns(table_name)
        primary_key = self._get_primary_key(table_name)
        check_constraints = self._get_check_constraints(table_name)
        properties = self._get_table_properties(table_name)
        clustering = self._get_clustering(table_name)
        partitioning = self._get_partitioning(table_name)

        return Table(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            check_constraints=check_constraints,
            liquid_clustering=clustering,
            partitioned_by=partitioning,
            table_properties=properties,
        )

    def _get_columns(self, table_name: str) -> list[Column]:
        """Get column definitions from information_schema.columns."""
        query = f"""
            SELECT
                column_name,
                full_data_type,
                is_nullable,
                column_default,
                comment
            FROM {self.catalog}.information_schema.columns
            WHERE table_schema = '{self.schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        rows = self.spark.sql(query).collect()

        columns = []
        for row in rows:
            fk = self._get_foreign_key_for_column(table_name, row.column_name)
            columns.append(
                Column(
                    name=row.column_name,
                    type=row.full_data_type,
                    nullable=(row.is_nullable == "YES"),
                    default=row.column_default,
                    foreign_key=fk,
                    comment=row.comment,
                )
            )
        return columns

    def _get_primary_key(self, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key from information_schema.table_constraints."""
        query = f"""
            SELECT
                tc.constraint_name,
                tc.enforced,
                kcu.column_name,
                kcu.ordinal_position
            FROM {self.catalog}.information_schema.table_constraints tc
            JOIN {self.catalog}.information_schema.key_column_usage kcu
              ON tc.constraint_catalog = kcu.constraint_catalog
              AND tc.constraint_schema = kcu.constraint_schema
              AND tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = '{self.schema}'
              AND tc.table_name = '{table_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        rows = self.spark.sql(query).collect()

        if not rows:
            return None

        columns = [row.column_name for row in rows]
        rely = rows[0].enforced == "YES" if rows else False

        return PrimaryKey(columns=columns, rely=rely)

    def _get_foreign_key_for_column(
        self, table_name: str, column_name: str
    ) -> Optional[ForeignKey]:
        """Get foreign key reference for a specific column."""
        query = f"""
            SELECT
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column
            FROM {self.catalog}.information_schema.referential_constraints rc
            JOIN {self.catalog}.information_schema.key_column_usage kcu
              ON rc.constraint_catalog = kcu.constraint_catalog
              AND rc.constraint_schema = kcu.constraint_schema
              AND rc.constraint_name = kcu.constraint_name
            JOIN {self.catalog}.information_schema.constraint_column_usage ccu
              ON rc.unique_constraint_catalog = ccu.constraint_catalog
              AND rc.unique_constraint_schema = ccu.constraint_schema
              AND rc.unique_constraint_name = ccu.constraint_name
            WHERE kcu.table_schema = '{self.schema}'
              AND kcu.table_name = '{table_name}'
              AND kcu.column_name = '{column_name}'
        """
        rows = self.spark.sql(query).collect()

        if not rows:
            return None

        return ForeignKey(
            table=rows[0].referenced_table,
            column=rows[0].referenced_column,
        )

    def _get_check_constraints(self, table_name: str) -> list[CheckConstraint]:
        """Get CHECK constraints from table properties."""
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        rows = self.spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()

        constraints = []
        for row in rows:
            if row.key.startswith("delta.constraints."):
                name = row.key.replace("delta.constraints.", "")
                constraints.append(CheckConstraint(name=name, expression=row.value))

        return constraints

    def _get_table_properties(self, table_name: str) -> dict[str, str]:
        """Get table properties via SHOW TBLPROPERTIES."""
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        rows = self.spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()

        keep_prefixes = [
            "delta.enableChangeDataFeed",
            "delta.autoOptimize",
            "delta.columnMapping",
            "delta.minReaderVersion",
            "delta.minWriterVersion",
        ]

        props = {}
        for row in rows:
            if any(row.key.startswith(p) for p in keep_prefixes):
                props[row.key] = row.value

        return props

    def _get_clustering(self, table_name: str) -> list[str]:
        """Get liquid clustering columns from DESCRIBE DETAIL."""
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]
            if hasattr(detail, "clusteringColumns") and detail.clusteringColumns:
                return list(detail.clusteringColumns)
        except Exception:
            pass
        return []

    def _get_partitioning(self, table_name: str) -> list[str]:
        """Get partition columns from DESCRIBE DETAIL."""
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]
            if hasattr(detail, "partitionColumns") and detail.partitionColumns:
                return list(detail.partitionColumns)
        except Exception:
            pass
        return []
