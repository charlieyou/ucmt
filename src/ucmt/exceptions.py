"""Exception classes for ucmt."""

from ucmt.types import ChangeType


class UcmtError(Exception):
    """Base exception for ucmt."""


class SchemaLoadError(UcmtError):
    """Error loading schema definition files."""


class IntrospectionError(UcmtError):
    """Error introspecting database schema."""


class DiffError(UcmtError):
    """Error computing schema diff."""


class UnsupportedChangeError(UcmtError):
    """Change is not supported by Databricks/Delta Lake."""

    def __init__(self, change_type: ChangeType, message: str):
        self.change_type = change_type
        super().__init__(message)


class CodegenError(UcmtError):
    """Error generating migration SQL."""


class MigrationError(UcmtError):
    """Error executing migration."""


class ConfigError(UcmtError):
    """Error in configuration."""
