from unittest.mock import MagicMock, patch

import pytest
from databricks.sql.exc import ServerOperationError

from ucmt.databricks.client import DatabricksClient


@pytest.fixture
def mock_connection():
    with patch("ucmt.databricks.client.sql.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_connect, mock_conn, mock_cursor


def test_client_uses_configured_connection_params(mock_connection):
    mock_connect, _, _ = mock_connection

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
        warehouse_id="abc123",
    )
    client.connect()

    mock_connect.assert_called_once_with(
        server_hostname="test.databricks.com",
        access_token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
    )


def test_client_executes_sql(mock_connection):
    _, _, mock_cursor = mock_connection

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
        warehouse_id="abc123",
    )
    client.connect()
    client.execute("CREATE TABLE test (id INT)")

    mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT)")


def test_client_fetchall_returns_rows(mock_connection):
    _, _, mock_cursor = mock_connection
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
        warehouse_id="abc123",
    )
    client.connect()
    rows = client.fetchall("SELECT * FROM users")

    mock_cursor.execute.assert_called_once_with("SELECT * FROM users")
    assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def test_client_raises_on_sql_error(mock_connection):
    _, _, mock_cursor = mock_connection
    mock_cursor.execute.side_effect = ServerOperationError("Table not found")

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
        warehouse_id="abc123",
    )
    client.connect()

    with pytest.raises(ServerOperationError, match="Table not found"):
        client.execute("SELECT * FROM nonexistent")


def test_client_supports_non_select_commands(mock_connection):
    _, _, mock_cursor = mock_connection

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
        warehouse_id="abc123",
    )
    client.connect()

    client.execute("DROP TABLE IF EXISTS temp_table")
    client.execute("INSERT INTO users VALUES (1, 'Test')")

    assert mock_cursor.execute.call_count == 2
