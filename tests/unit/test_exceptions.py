"""Tests for ucmt.exceptions module."""

import pytest

from ucmt.exceptions import (
    ConfigError,
    MigrationChecksumMismatchError,
    MigrationError,
    MigrationParseError,
    MigrationStateConflictError,
    SchemaLoadError,
    UcmtError,
    UnsupportedChangeError,
    UnsupportedSchemaChangeError,
)
from ucmt.types import ChangeType


class TestExceptionHierarchy:
    """Tests for exception hierarchy."""

    def test_exception_hierarchy(self):
        """All exceptions inherit from UcmtError."""
        assert issubclass(SchemaLoadError, UcmtError)
        assert issubclass(MigrationError, UcmtError)
        assert issubclass(MigrationParseError, MigrationError)
        assert issubclass(MigrationStateConflictError, MigrationError)
        assert issubclass(MigrationChecksumMismatchError, MigrationError)
        assert issubclass(ConfigError, UcmtError)
        assert issubclass(UnsupportedSchemaChangeError, UcmtError)

    def test_ucmt_error_is_exception(self):
        """UcmtError inherits from Exception."""
        assert issubclass(UcmtError, Exception)

    def test_exceptions_can_be_raised_and_caught(self):
        """All exceptions can be raised and caught."""
        with pytest.raises(UcmtError):
            raise SchemaLoadError("Failed to load schema")

        with pytest.raises(UcmtError):
            raise MigrationParseError("Invalid migration file")

        with pytest.raises(UcmtError):
            raise MigrationStateConflictError("State conflict")

        with pytest.raises(UcmtError):
            raise MigrationChecksumMismatchError("Checksum mismatch")

        with pytest.raises(UcmtError):
            raise ConfigError("Missing config")

        with pytest.raises(UcmtError):
            raise UnsupportedSchemaChangeError(
                ChangeType.ALTER_COLUMN_TYPE, "Not supported"
            )

    def test_unsupported_schema_change_error_has_change_type(self):
        """UnsupportedSchemaChangeError stores change_type."""
        error = UnsupportedSchemaChangeError(
            ChangeType.DROP_COLUMN, "Cannot drop column"
        )
        assert error.change_type == ChangeType.DROP_COLUMN
        assert str(error) == "Cannot drop column"

    def test_unsupported_schema_change_error_is_alias(self):
        """UnsupportedSchemaChangeError is an alias for UnsupportedChangeError."""
        assert UnsupportedSchemaChangeError is UnsupportedChangeError
