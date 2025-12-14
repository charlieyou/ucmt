"""Command-line interface for ucmt."""

import argparse
import sys
from contextlib import closing
from pathlib import Path

from ucmt.config import Config
from ucmt.schema.codegen import MigrationGenerator
from ucmt.schema.diff import SchemaDiffer
from ucmt.schema.loader import load_schema


def _validate_db_config(config: Config) -> bool:
    """Validate that all required DB config is present."""
    missing = []
    if not config.catalog:
        missing.append("DATABRICKS_CATALOG")
    if not config.schema:
        missing.append("DATABRICKS_SCHEMA")
    if not config.server_hostname:
        missing.append("DATABRICKS_SERVER_HOSTNAME")
    if not config.http_path:
        missing.append("DATABRICKS_HTTP_PATH")
    if not config.access_token:
        missing.append("DATABRICKS_ACCESS_TOKEN")

    if missing:
        print(
            "Error: missing required environment variables: " + ", ".join(missing),
            file=sys.stderr,
        )
        return False
    return True


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="ucmt",
        description="Unity Catalog Migration Tool",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    diff_parser = subparsers.add_parser("diff", help="Show schema diff")
    diff_parser.add_argument("--schema-path", type=Path, default=Path("schema/tables"))

    generate_parser = subparsers.add_parser("generate", help="Generate migration")
    generate_parser.add_argument("description", help="Migration description")
    generate_parser.add_argument(
        "--schema-path", type=Path, default=Path("schema/tables")
    )

    validate_parser = subparsers.add_parser("validate", help="Validate schema files")
    validate_parser.add_argument(
        "--schema-path", type=Path, default=Path("schema/tables")
    )

    status_parser = subparsers.add_parser("status", help="Show migration status")
    status_parser.add_argument(
        "--migrations-path", type=Path, default=Path("sql/migrations")
    )

    run_parser = subparsers.add_parser("run", help="Run pending migrations")
    run_parser.add_argument(
        "--migrations-path", type=Path, default=Path("sql/migrations")
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending migrations without executing",
    )

    args = parser.parse_args()

    if args.command == "validate":
        return cmd_validate(args)
    elif args.command == "diff":
        return cmd_diff(args)
    elif args.command == "generate":
        return cmd_generate(args)
    elif args.command == "status":
        return cmd_status(args)
    elif args.command == "run":
        return cmd_run(args)
    else:
        print(f"Command '{args.command}' not yet implemented", file=sys.stderr)
        return 1


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate schema files."""
    try:
        schema = load_schema(args.schema_path)
        print(f"Validated {len(schema.tables)} tables:")
        for name in sorted(schema.table_names()):
            table = schema.get_table(name)
            print(f"  - {name} ({len(table.columns)} columns)")
        return 0
    except Exception as e:
        print(f"Validation error: {e}", file=sys.stderr)
        return 1


def cmd_diff(args: argparse.Namespace) -> int:
    """Show schema diff (offline mode - declared vs empty)."""
    try:
        from ucmt.schema.models import Schema

        declared = load_schema(args.schema_path)
        current = Schema(tables={})
        differ = SchemaDiffer()
        changes = differ.diff(current, declared)

        if not changes:
            print("No changes detected")
            return 0

        print(f"Found {len(changes)} changes:")
        for change in changes:
            prefix = "[UNSUPPORTED] " if change.is_unsupported else ""
            print(f"  {prefix}{change.change_type.value}: {change.table_name}")

        return 0
    except Exception as e:
        print(f"Diff error: {e}", file=sys.stderr)
        return 1


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate migration from diff."""
    try:
        from ucmt.schema.models import Schema

        config = Config.from_env()
        declared = load_schema(args.schema_path)
        current = Schema(tables={})
        differ = SchemaDiffer()
        changes = differ.diff(current, declared)

        if not changes:
            print("No changes to generate")
            return 0

        generator = MigrationGenerator(
            catalog=config.catalog or "${catalog}",
            schema=config.schema or "${schema}",
        )
        sql = generator.generate(changes, args.description)
        print(sql)
        return 0
    except Exception as e:
        print(f"Generation error: {e}", file=sys.stderr)
        return 1


def cmd_status(args: argparse.Namespace) -> int:
    """Show migration status."""
    try:
        from databricks import sql as databricks_sql
    except ImportError:
        print(
            "Error: databricks-sql-connector not installed. Run: pip install databricks-sql-connector",
            file=sys.stderr,
        )
        return 1

    try:
        from ucmt.client import DatabricksClient
        from ucmt.runner import MigrationRunner
        from ucmt.state import MigrationState

        config = Config.from_env()
        if not _validate_db_config(config):
            return 1

        with closing(
            databricks_sql.connect(
                server_hostname=config.server_hostname,
                http_path=config.http_path,
                access_token=config.access_token,
            )
        ) as connection:
            client = DatabricksClient(connection)
            state = MigrationState(client, config.catalog, config.schema)
            runner = MigrationRunner(client, state, config.catalog, config.schema)

            migrations_path = args.migrations_path
            all_migrations = runner.discover_migrations(migrations_path)

            if not all_migrations:
                print(f"No migrations found in {migrations_path}")
                return 0

            try:
                state.ensure_table()
                applied_versions = state.get_applied_versions()
            except Exception as e:
                print(f"Error reading migration state: {e}", file=sys.stderr)
                return 1

            print(f"Migrations in {migrations_path}:")
            for migration in all_migrations:
                status = (
                    "✓ applied"
                    if migration.version in applied_versions
                    else "○ pending"
                )
                print(f"  V{migration.version}: {migration.description} [{status}]")

            pending_count = sum(
                1 for m in all_migrations if m.version not in applied_versions
            )
            applied_count = len(applied_versions)
            print(
                f"\nTotal: {len(all_migrations)} migrations "
                f"({applied_count} applied, {pending_count} pending)"
            )

            return 0
    except Exception as e:
        print(f"Status error: {e}", file=sys.stderr)
        return 1


def cmd_run(args: argparse.Namespace) -> int:
    """Run pending migrations."""
    try:
        from databricks import sql as databricks_sql
    except ImportError:
        print(
            "Error: databricks-sql-connector not installed. Run: pip install databricks-sql-connector",
            file=sys.stderr,
        )
        return 1

    try:
        from ucmt.client import DatabricksClient
        from ucmt.runner import MigrationRunner
        from ucmt.state import MigrationState

        config = Config.from_env()
        if not _validate_db_config(config):
            return 1

        with closing(
            databricks_sql.connect(
                server_hostname=config.server_hostname,
                http_path=config.http_path,
                access_token=config.access_token,
            )
        ) as connection:
            client = DatabricksClient(connection)
            state = MigrationState(client, config.catalog, config.schema)
            runner = MigrationRunner(client, state, config.catalog, config.schema)

            migrations_path = args.migrations_path
            all_migrations = runner.discover_migrations(migrations_path)
            state.ensure_table()
            pending = runner.get_pending(all_migrations)

            if not pending:
                print("No pending migrations")
                return 0

            print(f"Found {len(pending)} pending migration(s):")
            for migration in pending:
                print(f"  V{migration.version}: {migration.description}")

            if args.dry_run:
                print("\nDry run - no migrations executed")
                return 0

            print()
            for migration in pending:
                print(
                    f"Running V{migration.version}: {migration.description}...", end=" "
                )
                try:
                    runner.run(migration)
                    print("✓")
                except Exception as e:
                    print(f"✗ ({e})")
                    return 1

            print(f"\nSuccessfully applied {len(pending)} migration(s)")
            return 0
    except Exception as e:
        print(f"Run error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
