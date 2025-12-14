"""Migration file parser for V###__name.sql files."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from ucmt.exceptions import MigrationParseError

__all__ = ["MigrationFile", "parse_migration_file", "parse_migrations_dir"]

MIGRATION_FILENAME_PATTERN = re.compile(r"^V(\d+)__(.+)\.sql$")


@dataclass(frozen=True)
class MigrationFile:
    version: int
    name: str
    path: Path
    checksum: str
    sql: str

    def __post_init__(self):
        if not isinstance(self.version, int):
            raise TypeError(f"version must be int, got {type(self.version).__name__}")


def _compute_checksum(content: str) -> str:
    """Compute SHA256 checksum with normalized line endings."""
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_migration_file(path: Path) -> MigrationFile:
    """Parse a single migration file.

    Args:
        path: Path to the migration file (must be V<version>__name.sql format,
              e.g. V1__init.sql, V001__create_users.sql)

    Returns:
        MigrationFile with parsed metadata and content

    Raises:
        MigrationParseError: If filename is invalid, file is empty, or cannot be read
    """
    match = MIGRATION_FILENAME_PATTERN.match(path.name)
    if not match:
        raise MigrationParseError(
            f"Invalid filename '{path.name}'. Expected format: V<version>__name.sql"
        )

    version_str, name = match.groups()
    version = int(version_str)

    if not name:
        raise MigrationParseError(
            f"Invalid filename '{path.name}'. Migration name cannot be empty."
        )

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MigrationParseError(
            f"Failed to read migration file '{path}': {exc}"
        ) from exc

    if not content.strip():
        raise MigrationParseError(f"Migration file '{path.name}' is empty.")

    checksum = _compute_checksum(content)

    return MigrationFile(
        version=version,
        name=name,
        path=path,
        checksum=checksum,
        sql=content,
    )


def parse_migrations_dir(directory: Path) -> list[MigrationFile]:
    """Parse all migration files in a directory.

    Args:
        directory: Path to directory containing migration files

    Returns:
        List of MigrationFile objects sorted by version (ascending)

    Raises:
        MigrationParseError: If duplicate versions are found
    """
    migrations: list[MigrationFile] = []
    versions_seen: dict[int, Path] = {}

    for sql_file in directory.glob("*.sql"):
        if not MIGRATION_FILENAME_PATTERN.match(sql_file.name):
            continue

        migration = parse_migration_file(sql_file)

        if migration.version in versions_seen:
            raise MigrationParseError(
                f"Duplicate version {migration.version}: "
                f"'{versions_seen[migration.version].name}' and '{sql_file.name}'"
            )

        versions_seen[migration.version] = sql_file
        migrations.append(migration)

    migrations.sort(key=lambda m: m.version)
    return migrations
