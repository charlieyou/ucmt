"""Databricks SQL client wrapper."""

from typing import Any


class DatabricksClient:
    """Client for executing SQL against Databricks."""

    def __init__(self, connection: Any):
        self.connection = connection

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_many(self, statements: list[str]) -> None:
        """Execute multiple SQL statements."""
        with self.connection.cursor() as cursor:
            for stmt in statements:
                cursor.execute(stmt)
