from typing import Any, Optional

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


class DatabricksClient:
    """Thin wrapper around databricks-connect for simple SQL execution.

    Relies on Databricks SDK configuration (env vars, ~/.databrickscfg profiles)
    to determine compute target. Supports serverless_compute_id = auto.

    If host/token are provided, they override env/profile settings.
    If http_path is provided, a warning is logged (deprecated, ignored with Connect).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        http_path: Optional[str] = None,
    ) -> None:
        self._host = host
        self._token = token
        self._http_path = http_path
        self._session: SparkSession | None = None

    def connect(self) -> None:
        """Establish a DatabricksSession. Must be called before execute/fetchall.

        Compute selection (serverless, cluster, etc.) is determined by Databricks SDK
        configuration (env vars like DATABRICKS_SERVERLESS_COMPUTE_ID=auto, or
        ~/.databrickscfg profiles).
        """
        if self._session is not None:
            raise RuntimeError("Already connected. Call close() before reconnecting.")

        builder = DatabricksSession.builder

        if self._host:
            builder = builder.host(self._host)
        if self._token:
            builder = builder.token(self._token)

        self._session = builder.getOrCreate()

    def execute(self, sql_statement: str, *args: Any, **kwargs: Any) -> None:
        if self._session is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._session.sql(sql_statement).collect()

    def fetchall(
        self, sql_statement: str, *args: Any, **kwargs: Any
    ) -> list[dict[str, Any]]:
        if self._session is None:
            raise RuntimeError("Not connected. Call connect() first.")
        rows = self._session.sql(sql_statement).collect()
        return [row.asDict() for row in rows]

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.stop()
            finally:
                self._session = None

    def __enter__(self) -> "DatabricksClient":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
