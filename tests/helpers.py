"""Shared test helpers for UCMT tests."""

from unittest.mock import MagicMock

from ucmt.config import Config
from ucmt.schema.models import Schema


class FakeRow:
    """Mock row from DatabricksClient.fetchall().

    Supports dict-like access via __getitem__, .get(), and .asDict().
    """

    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def asDict(self):
        return self._data


def make_test_config(
    catalog: str = "test_catalog",
    schema: str = "test_schema",
) -> Config:
    """Create a Config for tests with sensible defaults."""
    return Config(catalog=catalog, schema=schema)


def make_mock_client(
    tables_data: list[dict] | None = None,
    columns_data: dict[str, list[dict]] | None = None,
    pk_constraints_data: dict[str, list[dict]] | None = None,
    check_constraints_data: dict[str, list[dict]] | None = None,
    table_properties_data: dict[str, list[dict]] | None = None,
) -> MagicMock:
    """Create a mock DatabricksClient with test data.

    Args:
        tables_data: List of table metadata dicts
        columns_data: Dict mapping table_name -> list of column dicts
        pk_constraints_data: Dict mapping table_name -> list of PK constraint dicts
        check_constraints_data: Dict mapping table_name -> list of CHECK constraint dicts
        table_properties_data: Dict mapping table_name -> list of property dicts
    """
    client = MagicMock()
    columns_data = columns_data or {}
    pk_constraints_data = pk_constraints_data or {}
    check_constraints_data = check_constraints_data or {}
    table_properties_data = table_properties_data or {}

    def fetchall_side_effect(sql: str):
        sql_lower = sql.lower()

        if "information_schema.tables" in sql_lower:
            for table in tables_data or []:
                if f"'{table['table_name']}'" in sql_lower:
                    return [FakeRow(table)]
            return [FakeRow(t) for t in (tables_data or [])]

        if "information_schema.columns" in sql_lower:
            for table_name, cols in columns_data.items():
                if f"'{table_name}'" in sql_lower:
                    return [FakeRow(c) for c in cols]
            return []

        if "constraint_column_usage" in sql_lower:
            for table_name, pks in pk_constraints_data.items():
                if f"'{table_name}'" in sql_lower:
                    return [FakeRow(pk) for pk in pks]
            return []

        if "table_constraints" in sql_lower and "check" in sql_lower:
            for table_name, checks in check_constraints_data.items():
                if f"'{table_name}'" in sql_lower:
                    return [FakeRow(c) for c in checks]
            return []

        if "tblproperties" in sql_lower:
            for table_name, props in table_properties_data.items():
                if f"`{table_name}`" in sql_lower:
                    return [FakeRow(p) for p in props]
            return []

        return []

    client.fetchall.side_effect = fetchall_side_effect
    return client


def build_db_state_from_schema(
    schema: Schema,
) -> tuple[
    list[dict],
    dict[str, list[dict]],
    dict[str, list[dict]],
    dict[str, list[dict]],
    dict[str, list[dict]],
]:
    """Build mock DB state data from a Schema object.

    Returns tuple of:
        (tables_data, columns_data, pk_constraints_data,
         check_constraints_data, table_properties_data)
    """
    tables_data = []
    columns_data = {}
    pk_constraints_data = {}
    check_constraints_data = {}
    table_properties_data = {}

    for table in schema.tables.values():
        tables_data.append(
            {
                "table_name": table.name,
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "comment": table.comment,
                "clustering_columns": (
                    ",".join(table.liquid_clustering)
                    if table.liquid_clustering
                    else None
                ),
            }
        )

        columns_data[table.name] = [
            {
                "table_name": table.name,
                "column_name": col.name,
                "data_type": col.type,
                "is_nullable": "NO" if not col.nullable else "YES",
                "column_default": col.default,
                "comment": col.comment,
            }
            for col in table.columns
        ]

        if table.primary_key:
            pk_constraints_data[table.name] = [
                {
                    "constraint_name": f"pk_{table.name}",
                    "constraint_type": "PRIMARY KEY",
                    "column_name": col,
                    "rely": table.primary_key.rely,
                }
                for col in table.primary_key.columns
            ]

        if table.check_constraints:
            check_constraints_data[table.name] = [
                {
                    "constraint_name": c.name,
                    "constraint_type": "CHECK",
                    "check_clause": c.expression,
                }
                for c in table.check_constraints
            ]

        if table.table_properties:
            table_properties_data[table.name] = [
                {"key": k, "value": v} for k, v in table.table_properties.items()
            ]

    return (
        tables_data,
        columns_data,
        pk_constraints_data,
        check_constraints_data,
        table_properties_data,
    )


def strip_timestamp_line(sql: str) -> list[str]:
    """Strip the '-- Generated:' timestamp line for comparison.

    Returns list of lines with trailing whitespace stripped.
    """
    return [
        line.rstrip()
        for line in sql.splitlines()
        if not line.startswith("-- Generated:")
    ]
