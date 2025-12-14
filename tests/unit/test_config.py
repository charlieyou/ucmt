"""Tests for Config module."""

import pytest

from ucmt.config import Config
from ucmt.exceptions import ConfigError


class TestConfigDefaults:
    """Test Config default values."""

    def test_config_defaults(self):
        """Config should have sensible defaults for paths."""
        config = Config()
        assert config.schema_dir == "schema"
        assert config.migrations_dir == "sql/migrations"
        assert config.catalog is None
        assert config.schema is None
        assert config.databricks_host is None
        assert config.databricks_token is None
        assert config.databricks_http_path is None
        assert config.databricks_warehouse_id is None


class TestConfigFromEnv:
    """Test Config.from_env() loading from environment variables."""

    def test_config_loads_from_env(self, monkeypatch):
        """Config.from_env() should load all values from environment variables."""
        monkeypatch.setenv("UCMT_CATALOG", "my_catalog")
        monkeypatch.setenv("UCMT_SCHEMA", "my_schema")
        monkeypatch.setenv("UCMT_SCHEMA_DIR", "custom/schema")
        monkeypatch.setenv("UCMT_MIGRATIONS_DIR", "custom/migrations")
        monkeypatch.setenv("DATABRICKS_HOST", "my-workspace.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "abc123")

        config = Config.from_env()

        assert config.catalog == "my_catalog"
        assert config.schema == "my_schema"
        assert config.schema_dir == "custom/schema"
        assert config.migrations_dir == "custom/migrations"
        assert config.databricks_host == "my-workspace.databricks.com"
        assert config.databricks_token == "dapi123"
        assert config.databricks_http_path == "/sql/1.0/warehouses/abc"
        assert config.databricks_warehouse_id == "abc123"

    def test_config_uses_defaults_when_env_not_set(self, monkeypatch):
        """Config.from_env() should use defaults when env vars not set."""
        monkeypatch.delenv("UCMT_CATALOG", raising=False)
        monkeypatch.delenv("UCMT_SCHEMA", raising=False)
        monkeypatch.delenv("UCMT_SCHEMA_DIR", raising=False)
        monkeypatch.delenv("UCMT_MIGRATIONS_DIR", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)
        monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)

        config = Config.from_env()

        assert config.catalog is None
        assert config.schema is None
        assert config.schema_dir == "schema"
        assert config.migrations_dir == "sql/migrations"
        assert config.state_table == "_ucmt_migrations"

    def test_config_loads_state_table_from_env(self, monkeypatch):
        """Config.from_env() should load UCMT_STATE_TABLE."""
        monkeypatch.setenv("UCMT_STATE_TABLE", "custom_migrations")
        config = Config.from_env()
        assert config.state_table == "custom_migrations"

    def test_config_loads_warehouse_id_from_env(self, monkeypatch):
        """Config.from_env() should load DATABRICKS_WAREHOUSE_ID."""
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-12345")
        config = Config.from_env()
        assert config.databricks_warehouse_id == "wh-12345"


class TestConfigCliOverrides:
    """Test CLI argument overrides."""

    def test_config_cli_overrides_env(self, monkeypatch):
        """CLI arguments should override environment variables."""
        monkeypatch.setenv("UCMT_CATALOG", "env_catalog")
        monkeypatch.setenv("UCMT_SCHEMA", "env_schema")

        config = Config.from_env(
            catalog="cli_catalog",
            schema="cli_schema",
        )

        assert config.catalog == "cli_catalog"
        assert config.schema == "cli_schema"

    def test_config_cli_partial_override(self, monkeypatch):
        """CLI should only override specified values."""
        monkeypatch.setenv("UCMT_CATALOG", "env_catalog")
        monkeypatch.setenv("UCMT_SCHEMA", "env_schema")

        config = Config.from_env(catalog="cli_catalog")

        assert config.catalog == "cli_catalog"
        assert config.schema == "env_schema"


class TestConfigValidation:
    """Test Config.validate_for_db_ops()."""

    def test_config_missing_required_raises_ConfigError(self):
        """validate_for_db_ops() should raise ConfigError if required fields missing."""
        config = Config()

        with pytest.raises(ConfigError) as exc_info:
            config.validate_for_db_ops()

        assert (
            "catalog" in str(exc_info.value).lower()
            or "missing" in str(exc_info.value).lower()
        )

    def test_config_missing_catalog_raises_ConfigError(self):
        """validate_for_db_ops() should raise if catalog is missing."""
        config = Config(
            schema="my_schema",
            databricks_host="host",
            databricks_token="token",
            databricks_http_path="/path",
        )

        with pytest.raises(ConfigError):
            config.validate_for_db_ops()

    def test_config_missing_schema_raises_ConfigError(self):
        """validate_for_db_ops() should raise if schema is missing."""
        config = Config(
            catalog="my_catalog",
            databricks_host="host",
            databricks_token="token",
            databricks_http_path="/path",
        )

        with pytest.raises(ConfigError):
            config.validate_for_db_ops()

    def test_config_missing_connection_raises_ConfigError(self):
        """validate_for_db_ops() should raise if connection info is missing."""
        config = Config(
            catalog="my_catalog",
            schema="my_schema",
        )

        with pytest.raises(ConfigError):
            config.validate_for_db_ops()

    def test_config_valid_for_db_ops(self):
        """validate_for_db_ops() should not raise when all required fields present."""
        config = Config(
            catalog="my_catalog",
            schema="my_schema",
            databricks_host="host",
            databricks_token="token",
            databricks_http_path="/path",
        )

        config.validate_for_db_ops()
