"""Exception classes for ucmt."""

from ucmt.types import ChangeType

__all__ = [
    "UcmtError",
    "SchemaLoadError",
    "IntrospectionError",
    "DiffError",
    "MigrationError",
    "MigrationParseError",
    "MigrationStateConflictError",
    "MigrationChecksumMismatchError",
    "UnsupportedChangeError",
    "UnsupportedSchemaChangeError",
    "CodegenError",
    "ConfigError",
]


class UcmtError(Exception):
    """Base exception for ucmt."""


class SchemaLoadError(UcmtError):
    """Error loading schema definition files."""


class IntrospectionError(UcmtError):
    """Error introspecting database schema."""


class DiffError(UcmtError):
    """Error computing schema diff."""


class MigrationError(UcmtError):
    """Base error during migration execution or management."""


class MigrationParseError(MigrationError):
    """Error parsing migration file."""


class MigrationStateConflictError(MigrationError):
    """State conflict during migration."""


class MigrationChecksumMismatchError(MigrationError):
    """Migration checksum does not match recorded checksum."""


class UnsupportedChangeError(UcmtError):
    """Change is not supported by Databricks/Delta Lake."""

    def __init__(self, change_type: ChangeType, message: str):
        self.change_type = change_type
        super().__init__(message)


UnsupportedSchemaChangeError = UnsupportedChangeError


class CodegenError(UcmtError):
    """Error generating migration SQL."""


class ConfigError(UcmtError):
    """Error in configuration."""
