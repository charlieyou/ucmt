from databricks import sql
from databricks.sql.client import Connection, Cursor


class DatabricksClient:
    def __init__(
        self,
        host: str,
        token: str,
        http_path: str,
        warehouse_id: str,
    ) -> None:
        self._host = host
        self._token = token
        self._http_path = http_path
        self._warehouse_id = warehouse_id
        self._connection: Connection | None = None
        self._cursor: Cursor | None = None

    def connect(self) -> None:
        self._connection = sql.connect(
            server_hostname=self._host,
            access_token=self._token,
            http_path=self._http_path,
        )
        self._cursor = self._connection.cursor()

    def execute(self, sql_statement: str) -> None:
        if self._cursor is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._cursor.execute(sql_statement)

    def fetchall(self, sql_statement: str) -> list:
        if self._cursor is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._cursor.execute(sql_statement)
        return self._cursor.fetchall()

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
