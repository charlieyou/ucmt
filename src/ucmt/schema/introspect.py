"""Schema introspection from Unity Catalog using DatabricksClient."""

import json
from typing import Any, Protocol

from ucmt.schema.models import (
    CheckConstraint,
    Column,
    PrimaryKey,
    Schema,
    Table,
)


class SQLClient(Protocol):
    """Protocol for SQL client used by introspector."""

    def fetchall(self, sql: str) -> list: ...


class SchemaIntrospector:
    """Introspect schema from Unity Catalog using DatabricksClient."""

    VALID_TABLE_TYPES = {"MANAGED", "EXTERNAL"}

    def __init__(self, client: SQLClient, catalog: str, schema: str) -> None:
        self._client = client
        self._catalog = catalog
        self._schema = schema

    def _row_get(self, row: Any, key: str, default: Any = None) -> Any:
        """Safely get a value from a row, supporting dict-like and pyspark Row."""
        if hasattr(row, "get"):
            return row.get(key, default)
        if hasattr(row, "asDict"):
            return row.asDict().get(key, default)
        try:
            return row[key]
        except Exception:
            return default

    def introspect_table(self, table_name: str) -> Table | None:
        """Introspect a single table. Returns None if not found or not a Delta table."""
        table_info = self._fetch_table_info(table_name)
        if table_info is None:
            return None

        if not self._is_valid_table(table_info):
            return None

        columns = self._fetch_columns(table_name)
        primary_key = self._fetch_primary_key(table_name)
        check_constraints = self._fetch_check_constraints(table_name)
        table_properties = self._fetch_table_properties(table_name)
        liquid_clustering = self._parse_clustering_columns(table_info)

        return Table(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            check_constraints=check_constraints,
            liquid_clustering=liquid_clustering,
            table_properties=table_properties,
            comment=table_info.get("comment"),
        )

    def introspect_schema(self) -> Schema:
        """Introspect all Delta tables in the schema."""
        tables = {}
        table_names = self._fetch_all_table_names()

        for name in table_names:
            table = self.introspect_table(name)
            if table is not None:
                tables[name] = table

        return Schema(tables=tables)

    def _fetch_table_info(self, table_name: str) -> dict | None:
        """Fetch table metadata from information_schema.tables."""
        sql = f"""
            SELECT table_name, table_type, data_source_format, comment, clustering_columns
            FROM {self._catalog}.information_schema.tables
            WHERE table_schema = '{self._schema}'
              AND table_name = '{table_name}'
        """
        rows = self._client.fetchall(sql)
        if not rows:
            return None
        for row in rows:
            if row["table_name"] == table_name:
                return {
                    "table_name": row["table_name"],
                    "table_type": row["table_type"],
                    "data_source_format": row.get("data_source_format"),
                    "comment": row.get("comment"),
                    "clustering_columns": row.get("clustering_columns"),
                }
        return None

    def _is_valid_table(self, table_info: dict) -> bool:
        """Check if table is a valid Delta table (not view, temp, streaming)."""
        table_type = (table_info.get("table_type") or "").upper()
        data_source_format = (table_info.get("data_source_format") or "").strip().upper()

        if table_type not in self.VALID_TABLE_TYPES:
            return False

        if data_source_format and data_source_format != "DELTA":
            return False

        return True

    def _fetch_columns(self, table_name: str) -> list[Column]:
        """Fetch columns from information_schema.columns."""
        sql = f"""
            SELECT table_name, column_name, data_type, is_nullable, column_default, comment
            FROM {self._catalog}.information_schema.columns
            WHERE table_schema = '{self._schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        rows = self._client.fetchall(sql)
        columns = []
        for row in rows:
            if row.get("table_name", table_name) != table_name:
                continue
            col = Column(
                name=row["column_name"],
                type=row["data_type"].upper(),
                nullable=row["is_nullable"] != "NO",
                default=row.get("column_default"),
                comment=row.get("comment"),
            )
            columns.append(col)
        return columns

    def _fetch_primary_key(self, table_name: str) -> PrimaryKey | None:
        """Fetch primary key constraint."""
        sql = f"""
            SELECT constraint_name, constraint_type, column_name, rely
            FROM {self._catalog}.information_schema.table_constraints tc
            JOIN {self._catalog}.information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_schema = '{self._schema}'
              AND tc.table_name = '{table_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
        """
        try:
            rows = self._client.fetchall(sql)
        except Exception:
            return None

        if not rows:
            return None

        columns = [row["column_name"] for row in rows]
        rely = rows[0].get("rely", False)
        return PrimaryKey(columns=columns, rely=rely)

    def _fetch_check_constraints(self, table_name: str) -> list[CheckConstraint]:
        """Fetch CHECK constraints."""
        sql = f"""
            SELECT constraint_name, constraint_type, check_clause
            FROM {self._catalog}.information_schema.table_constraints
            WHERE table_schema = '{self._schema}'
              AND table_name = '{table_name}'
              AND constraint_type = 'CHECK'
        """
        try:
            rows = self._client.fetchall(sql)
        except Exception:
            return []

        return [
            CheckConstraint(name=row["constraint_name"], expression=row["check_clause"])
            for row in rows
        ]

    def _fetch_table_properties(self, table_name: str) -> dict[str, str]:
        """Fetch table properties using SHOW TBLPROPERTIES."""
        sql = f"SHOW TBLPROPERTIES `{self._catalog}`.`{self._schema}`.`{table_name}`"
        try:
            rows = self._client.fetchall(sql)
        except Exception:
            return {}

        return {row["key"]: row["value"] for row in rows}

    def _parse_clustering_columns(self, table_info: dict) -> list[str]:
        """Parse clustering columns from table info."""
        clustering = table_info.get("clustering_columns")
        if not clustering:
            return []

        if isinstance(clustering, list):
            return [str(c).strip() for c in clustering if str(c).strip()]

        text = str(clustering).strip()

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(c).strip() for c in parsed if str(c).strip()]
            except Exception:
                pass

        return [c.strip() for c in text.split(",") if c.strip()]

    def _fetch_all_table_names(self) -> list[str]:
        """Fetch all table names in the schema."""
        sql = f"""
            SELECT table_name
            FROM {self._catalog}.information_schema.tables
            WHERE table_schema = '{self._schema}'
        """
        rows = self._client.fetchall(sql)
        return [row["table_name"] for row in rows]
