"""Migration execution."""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ucmt.exceptions import MigrationError
from ucmt.state import MigrationRecord, MigrationState


@dataclass
class Migration:
    """A migration file to be executed."""

    version: str
    description: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        """Compute checksum of migration content."""
        return hashlib.sha256(self.sql.encode()).hexdigest()[:16]


class MigrationRunner:
    """Execute migrations against Databricks."""

    VERSION_PATTERN = re.compile(r"^V(\d+)__(.+)\.sql$")

    def __init__(self, client: Any, state: MigrationState, catalog: str, schema: str):
        self.client = client
        self.state = state
        self.catalog = catalog
        self.schema = schema

    def discover_migrations(self, migrations_path: Path) -> list[Migration]:
        """Find and parse migration files."""
        migrations = []
        for sql_file in sorted(migrations_path.glob("V*.sql")):
            match = self.VERSION_PATTERN.match(sql_file.name)
            if match:
                version = match.group(1)
                description = match.group(2).replace("_", " ")
                sql = sql_file.read_text()
                migrations.append(
                    Migration(
                        version=version,
                        description=description,
                        path=sql_file,
                        sql=sql,
                    )
                )
        return migrations

    def get_pending(self, migrations: list[Migration]) -> list[Migration]:
        """Get migrations that haven't been applied yet."""
        applied = self.state.get_applied_versions()
        return [m for m in migrations if m.version not in applied]

    def run(self, migration: Migration) -> MigrationRecord:
        """Execute a single migration."""
        sql = self._substitute_variables(migration.sql)
        statements = self._split_statements(sql)

        try:
            for stmt in statements:
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--"):
                    self.client.execute(stmt)

            record = MigrationRecord(
                version=migration.version,
                description=migration.description,
                applied_at=datetime.now(),
                checksum=migration.checksum,
                success=True,
            )
        except Exception as e:
            record = MigrationRecord(
                version=migration.version,
                description=migration.description,
                applied_at=datetime.now(),
                checksum=migration.checksum,
                success=False,
                error_message=str(e),
            )
            raise MigrationError(f"Migration V{migration.version} failed: {e}") from e
        finally:
            self.state.record_migration(record)

        return record

    def _substitute_variables(self, sql: str) -> str:
        """Replace ${catalog} and ${schema} variables."""
        return sql.replace("${catalog}", self.catalog).replace("${schema}", self.schema)

    def _split_statements(self, sql: str) -> list[str]:
        """Split SQL into individual statements."""
        return [s.strip() for s in sql.split(";") if s.strip()]
