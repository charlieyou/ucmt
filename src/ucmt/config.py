"""Configuration management for ucmt."""

import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ucmt.exceptions import ConfigError


def load_databrickscfg(profile: str = "DEFAULT") -> dict[str, str]:
    """Load credentials from ~/.databrickscfg.

    Args:
        profile: Profile name to load (default: "DEFAULT")

    Returns:
        Dict with host, token, and optionally http_path/serverless_compute_id

    Raises:
        ConfigError: If file not found or profile doesn't exist
    """
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return {}

    config = configparser.ConfigParser()
    config.read(cfg_path)

    if profile not in config:
        available = [s for s in config.sections() if s != "DEFAULT"] or ["DEFAULT"]
        raise ConfigError(
            f"Profile '{profile}' not found in ~/.databrickscfg. "
            f"Available profiles: {', '.join(available)}"
        )

    section = config[profile]
    result = {}

    if "host" in section:
        host = section["host"].strip()
        if host.startswith("https://"):
            host = host[8:]
        host = host.rstrip("/")
        result["host"] = host

    if "token" in section:
        result["token"] = section["token"].strip()

    if "http_path" in section:
        result["http_path"] = section["http_path"].strip()
    elif "serverless_compute_id" in section:
        compute_id = section["serverless_compute_id"].strip()
        if compute_id and compute_id != "auto":
            result["http_path"] = f"/sql/1.0/warehouses/{compute_id}"

    return result


@dataclass
class Config:
    """Configuration for ucmt."""

    catalog: Optional[str] = None
    schema: Optional[str] = None
    schema_dir: str = "schema"
    migrations_dir: str = "sql/migrations"
    state_table: str = "_ucmt_migrations"
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_http_path: Optional[str] = None
    databricks_warehouse_id: Optional[str] = None

    @classmethod
    def from_env(
        cls,
        *,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        schema_dir: Optional[str] = None,
        migrations_dir: Optional[str] = None,
        state_table: Optional[str] = None,
        databricks_host: Optional[str] = None,
        databricks_token: Optional[str] = None,
        databricks_http_path: Optional[str] = None,
        databricks_warehouse_id: Optional[str] = None,
        profile: Optional[str] = None,
    ) -> "Config":
        """Load configuration from ~/.databrickscfg, env vars, with CLI overrides.

        Priority (highest to lowest):
        1. Explicit parameters (CLI args)
        2. Environment variables
        3. ~/.databrickscfg profile
        """
        databricks_cfg = {}
        profile_name = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        try:
            databricks_cfg = load_databrickscfg(profile_name)
        except ConfigError:
            pass

        def resolve(explicit, env_key, cfg_key=None):
            if explicit is not None:
                return explicit
            env_val = os.environ.get(env_key)
            if env_val is not None:
                return env_val
            if cfg_key and cfg_key in databricks_cfg:
                return databricks_cfg[cfg_key]
            return None

        return cls(
            catalog=resolve(catalog, "UCMT_CATALOG"),
            schema=resolve(schema, "UCMT_SCHEMA"),
            schema_dir=schema_dir
            if schema_dir is not None
            else os.environ.get("UCMT_SCHEMA_DIR", "schema"),
            migrations_dir=migrations_dir
            if migrations_dir is not None
            else os.environ.get("UCMT_MIGRATIONS_DIR", "sql/migrations"),
            state_table=state_table
            if state_table is not None
            else os.environ.get("UCMT_STATE_TABLE", "_ucmt_migrations"),
            databricks_host=resolve(databricks_host, "DATABRICKS_HOST", "host"),
            databricks_token=resolve(databricks_token, "DATABRICKS_TOKEN", "token"),
            databricks_http_path=resolve(
                databricks_http_path, "DATABRICKS_HTTP_PATH", "http_path"
            ),
            databricks_warehouse_id=resolve(
                databricks_warehouse_id, "DATABRICKS_WAREHOUSE_ID"
            ),
        )

    def validate_for_db_ops(self) -> None:
        """Validate that all required fields for database operations are present.

        Raises:
            ConfigError: If catalog, schema, or connection info is missing.
        """
        missing = []
        if not self.catalog:
            missing.append("catalog (use --catalog or UCMT_CATALOG)")
        if not self.schema:
            missing.append("schema (use --schema or UCMT_SCHEMA)")
        if not self.databricks_host:
            missing.append("databricks_host (use --profile or DATABRICKS_HOST)")
        if not self.databricks_token:
            missing.append("databricks_token (use --profile or DATABRICKS_TOKEN)")
        if not self.databricks_http_path:
            missing.append(
                "databricks_http_path (use --profile or DATABRICKS_HTTP_PATH)"
            )

        if missing:
            raise ConfigError(
                "Missing required configuration:\n  - " + "\n  - ".join(missing)
            )
