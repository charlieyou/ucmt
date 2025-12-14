from unittest.mock import MagicMock, patch

import pytest

from ucmt.databricks.client import DatabricksClient


@pytest.fixture
def mock_session():
    with patch("ucmt.databricks.client.DatabricksSession") as mock_db_session:
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.host.return_value = mock_builder
        mock_builder.token.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_db_session.builder = mock_builder
        yield mock_db_session, mock_builder, mock_spark


def test_client_uses_host_and_token_when_provided(mock_session):
    mock_db_session, mock_builder, _ = mock_session

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()

    mock_builder.host.assert_called_once_with("test.databricks.com")
    mock_builder.token.assert_called_once_with("dapi123")
    mock_builder.getOrCreate.assert_called_once()


def test_client_uses_env_config_when_no_host_token(mock_session):
    mock_db_session, mock_builder, _ = mock_session

    client = DatabricksClient()
    client.connect()

    mock_builder.host.assert_not_called()
    mock_builder.token.assert_not_called()
    mock_builder.getOrCreate.assert_called_once()


def test_client_ignores_http_path(mock_session):
    """http_path is accepted but ignored (deprecated for databricks-connect)."""
    mock_db_session, mock_builder, _ = mock_session

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
        http_path="/sql/1.0/warehouses/abc",
    )
    client.connect()

    mock_builder.host.assert_called_once_with("test.databricks.com")
    mock_builder.token.assert_called_once_with("dapi123")
    mock_builder.getOrCreate.assert_called_once()


def test_client_executes_sql(mock_session):
    _, _, mock_spark = mock_session
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()
    client.execute("CREATE TABLE test (id INT)")

    mock_spark.sql.assert_called_once_with("CREATE TABLE test (id INT)")
    mock_df.collect.assert_called_once()


def test_client_fetchall_returns_rows(mock_session):
    _, _, mock_spark = mock_session
    mock_row1 = MagicMock()
    mock_row1.asDict.return_value = {"id": 1, "name": "Alice"}
    mock_row2 = MagicMock()
    mock_row2.asDict.return_value = {"id": 2, "name": "Bob"}
    mock_df = MagicMock()
    mock_df.collect.return_value = [mock_row1, mock_row2]
    mock_spark.sql.return_value = mock_df

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()
    rows = client.fetchall("SELECT * FROM users")

    mock_spark.sql.assert_called_once_with("SELECT * FROM users")
    assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def test_client_raises_on_sql_error(mock_session):
    _, _, mock_spark = mock_session
    mock_spark.sql.side_effect = Exception("Table not found")

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()

    with pytest.raises(Exception, match="Table not found"):
        client.execute("SELECT * FROM nonexistent")


def test_client_supports_non_select_commands(mock_session):
    _, _, mock_spark = mock_session
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()

    client.execute("DROP TABLE IF EXISTS temp_table")
    client.execute("INSERT INTO users VALUES (1, 'Test')")

    assert mock_spark.sql.call_count == 2


def test_client_execute_before_connect_raises():
    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )

    with pytest.raises(RuntimeError, match="Not connected"):
        client.execute("SELECT 1")


def test_client_connect_twice_raises(mock_session):
    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()

    with pytest.raises(RuntimeError, match="Already connected"):
        client.connect()


def test_client_close_is_idempotent(mock_session):
    _, _, mock_spark = mock_session

    client = DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    )
    client.connect()
    client.close()
    client.close()

    mock_spark.stop.assert_called_once()


def test_client_context_manager(mock_session):
    _, _, mock_spark = mock_session
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df

    with DatabricksClient(
        host="test.databricks.com",
        token="dapi123",
    ) as client:
        client.execute("SELECT 1")

    mock_spark.sql.assert_called_once_with("SELECT 1")
    mock_spark.stop.assert_called_once()
