"""Tests for ucmt.databricks.utils module."""

import pytest
from unittest.mock import Mock, patch

from ucmt.config import Config
from ucmt.databricks.utils import (
    build_config_and_validate,
    get_online_schema,
    split_sql_statements,
)
from ucmt.exceptions import ConfigError
from ucmt.schema.models import Column, Schema, Table


class TestBuildConfigAndValidate:
    def test_returns_validated_config(self, monkeypatch):
        monkeypatch.setenv("UCMT_CATALOG", "test_catalog")
        monkeypatch.setenv("UCMT_SCHEMA", "test_schema")
        monkeypatch.setenv("DATABRICKS_HOST", "host.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "token123")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")

        config = build_config_and_validate()

        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"
        assert config.databricks_host == "host.databricks.com"

    def test_raises_config_error_when_missing_required(self, monkeypatch):
        monkeypatch.delenv("UCMT_CATALOG", raising=False)
        monkeypatch.delenv("UCMT_SCHEMA", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

        with pytest.raises(ConfigError):
            build_config_and_validate()


class TestGetOnlineSchema:
    def test_returns_schema_from_introspector(self, monkeypatch):
        mock_client = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)

        expected_schema = Schema(
            tables={
                "users": Table(name="users", columns=[Column(name="id", type="INT")])
            }
        )
        mock_introspector = Mock()
        mock_introspector.introspect_schema.return_value = expected_schema

        config = Config(
            catalog="cat",
            schema="sch",
            databricks_host="host",
            databricks_token="tok",
            databricks_http_path="/path",
        )

        with patch("ucmt.databricks.client.DatabricksClient", return_value=mock_client):
            with patch(
                "ucmt.schema.introspect.SchemaIntrospector",
                return_value=mock_introspector,
            ):
                result = get_online_schema(config)

        assert result == expected_schema
        mock_introspector.introspect_schema.assert_called_once()

    def test_raises_config_error_when_invalid(self):
        config = Config()  # Missing all required fields

        with pytest.raises(ConfigError):
            get_online_schema(config)


class TestSplitSqlStatements:
    def test_splits_on_semicolons(self):
        sql = "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);"
        result = split_sql_statements(sql)
        assert result == ["CREATE TABLE t1 (id INT)", "CREATE TABLE t2 (id INT)"]

    def test_skips_empty_statements(self):
        sql = "SELECT 1;; SELECT 2;"
        result = split_sql_statements(sql)
        assert result == ["SELECT 1", "SELECT 2"]

    def test_skips_comment_only_statements(self):
        sql = "SELECT 1; -- this is a comment; SELECT 2;"
        result = split_sql_statements(sql)
        assert result == ["SELECT 1", "SELECT 2"]

    def test_handles_trailing_semicolon(self):
        sql = "SELECT 1;"
        result = split_sql_statements(sql)
        assert result == ["SELECT 1"]

    def test_handles_no_semicolon(self):
        sql = "SELECT 1"
        result = split_sql_statements(sql)
        assert result == ["SELECT 1"]

    def test_handles_whitespace(self):
        sql = "  SELECT 1  ;  SELECT 2  "
        result = split_sql_statements(sql)
        assert result == ["SELECT 1", "SELECT 2"]

    def test_empty_string(self):
        result = split_sql_statements("")
        assert result == []

    def test_preserves_statement_content(self):
        sql = "CREATE TABLE users (id INT, name STRING);"
        result = split_sql_statements(sql)
        assert result == ["CREATE TABLE users (id INT, name STRING)"]
