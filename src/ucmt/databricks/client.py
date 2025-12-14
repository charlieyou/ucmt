from typing import Any

from databricks import sql
from databricks.sql.client import Connection, Cursor


class DatabricksClient:
    """Thin wrapper around databricks-sql-connector for simple SQL execution."""

    def __init__(
        self,
        host: str,
        token: str,
        http_path: str,
    ) -> None:
        self._host = host
        self._token = token
        self._http_path = http_path
        self._connection: Connection | None = None
        self._cursor: Cursor | None = None

    def connect(self) -> None:
        """Establish a connection and cursor. Must be called before execute/fetchall."""
        if self._connection is not None or self._cursor is not None:
            raise RuntimeError("Already connected. Call close() before reconnecting.")

        self._connection = sql.connect(
            server_hostname=self._host,
            access_token=self._token,
            http_path=self._http_path,
        )
        self._cursor = self._connection.cursor()

    def execute(self, sql_statement: str, *args: Any, **kwargs: Any) -> None:
        if self._cursor is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._cursor.execute(sql_statement, *args, **kwargs)

    def fetchall(
        self, sql_statement: str, *args: Any, **kwargs: Any
    ) -> list[dict[str, Any]]:
        if self._cursor is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._cursor.execute(sql_statement, *args, **kwargs)
        rows = self._cursor.fetchall()
        return [row.asDict() if hasattr(row, "asDict") else dict(row) for row in rows]

    def close(self) -> None:
        if self._cursor is not None:
            try:
                self._cursor.close()
            finally:
                self._cursor = None

        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def __enter__(self) -> "DatabricksClient":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
