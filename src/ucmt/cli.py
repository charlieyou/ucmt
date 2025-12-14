"""Command-line interface for ucmt."""

import argparse
import logging
import sys
from pathlib import Path

from ucmt.config import Config
from ucmt.databricks.utils import (
    build_config_from_env_and_validate,
    get_online_schema,
    split_sql_statements,
)
from ucmt.exceptions import ConfigError
from ucmt.schema.codegen import MigrationGenerator
from ucmt.schema.diff import SchemaDiffer
from ucmt.schema.loader import load_schema
from ucmt.schema.models import Schema


def main() -> int:
    """Main entry point."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        prog="ucmt",
        description="Unity Catalog Migration Tool",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    diff_parser = subparsers.add_parser("diff", help="Show schema diff")
    diff_parser.add_argument("--schema-path", type=Path, default=Path("schema/tables"))
    diff_parser.add_argument(
        "--online",
        action="store_true",
        help="Compare against actual database state (requires DB connection)",
    )

    generate_parser = subparsers.add_parser("generate", help="Generate migration")
    generate_parser.add_argument("description", help="Migration description")
    generate_parser.add_argument(
        "--schema-path", type=Path, default=Path("schema/tables")
    )
    generate_parser.add_argument(
        "--online",
        action="store_true",
        help="Compare against actual database state (requires DB connection)",
    )
    generate_parser.add_argument(
        "--allow-destructive",
        action="store_true",
        help="Allow destructive changes (DROP TABLE, DROP COLUMN, etc.)",
    )
    generate_parser.add_argument(
        "--output",
        type=Path,
        help="Output file path (default: stdout)",
    )

    validate_parser = subparsers.add_parser("validate", help="Validate schema files")
    validate_parser.add_argument(
        "--schema-path", type=Path, default=Path("schema/tables")
    )

    status_parser = subparsers.add_parser("status", help="Show migration status")
    status_parser.add_argument(
        "--migrations-path", type=Path, default=Path("sql/migrations")
    )

    plan_parser = subparsers.add_parser("plan", help="Show pending migrations")
    plan_parser.add_argument(
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
    elif args.command == "plan":
        return cmd_plan(args)
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
    """Show schema diff (offline mode compares declared vs empty, online mode compares against DB)."""
    try:
        declared = load_schema(args.schema_path)

        if args.online:
            config = build_config_from_env_and_validate()
            current = get_online_schema(config)
        else:
            current = Schema(tables={})

        differ = SchemaDiffer()
        changes = differ.diff(current, declared)

        if not changes:
            print("No changes detected")
            return 0

        mode = "online" if args.online else "offline"
        print(f"Found {len(changes)} changes ({mode} mode):")
        for change in changes:
            prefix = "[UNSUPPORTED] " if change.is_unsupported else ""
            print(f"  {prefix}{change.change_type.value}: {change.table_name}")

        return 0
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Diff error: {e}", file=sys.stderr)
        return 1


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate migration from diff."""
    try:
        config = Config.from_env()
        declared = load_schema(args.schema_path)

        if args.online:
            config.validate_for_db_ops()
            current = get_online_schema(config)
        else:
            current = Schema(tables={})

        differ = SchemaDiffer()
        changes = differ.diff(current, declared)

        if not changes:
            print("No changes to generate")
            return 0

        destructive_changes = [c for c in changes if c.is_destructive]
        if destructive_changes and not args.allow_destructive:
            print(
                "Error: destructive changes detected. Use --allow-destructive to proceed.",
                file=sys.stderr,
            )
            for change in destructive_changes:
                print(
                    f"  - {change.change_type.value}: {change.table_name}",
                    file=sys.stderr,
                )
            return 1

        generator = MigrationGenerator(
            catalog=config.catalog or "${catalog}",
            schema=config.schema or "${schema}",
        )
        sql = generator.generate(changes, args.description)
        print(sql)
        return 0
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Generation error: {e}", file=sys.stderr)
        return 1


def cmd_status(args: argparse.Namespace) -> int:
    """Show migration status."""
    try:
        from databricks import sql  # noqa: F401
    except ImportError:
        print(
            "Error: databricks-sql-connector not installed. Run: pip install databricks-sql-connector",
            file=sys.stderr,
        )
        return 1

    try:
        from ucmt.migrations.parser import parse_migrations_dir
        from ucmt.migrations.state import DatabricksMigrationStateStore

        config = build_config_from_env_and_validate()

        with DatabricksMigrationStateStore(config) as state_store:
            migrations_path = args.migrations_path
            all_migrations = parse_migrations_dir(migrations_path)

            if not all_migrations:
                print(f"No migrations found in {migrations_path}")
                return 0

            try:
                applied = state_store.list_applied()
                applied_versions = {m.version for m in applied}
                failed_versions = {m.version for m in applied if not m.success}
            except Exception as e:
                print(f"Error reading migration state: {e}", file=sys.stderr)
                return 1

            print(f"Migrations in {migrations_path}:")
            for migration in all_migrations:
                if migration.version in failed_versions:
                    status = "✗ failed"
                elif migration.version in applied_versions:
                    status = "✓ applied"
                else:
                    status = "○ pending"
                print(f"  V{migration.version}: {migration.name} [{status}]")

            pending_count = sum(
                1 for m in all_migrations if m.version not in applied_versions
            )
            applied_count = sum(
                1 for m in all_migrations if m.version in applied_versions
            )
            failed_count = sum(
                1 for m in all_migrations if m.version in failed_versions
            )
            print(
                f"\nTotal: {len(all_migrations)} migrations "
                f"({applied_count} applied, {pending_count} pending"
                + (f", {failed_count} failed)" if failed_count else ")")
            )

            return 0
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Status error: {e}", file=sys.stderr)
        return 1


def cmd_plan(args: argparse.Namespace) -> int:
    """Show pending migrations without executing."""
    try:
        from ucmt.migrations.parser import parse_migrations_dir
        from ucmt.migrations.runner import plan
        from ucmt.migrations.state import DatabricksMigrationStateStore

        config = build_config_from_env_and_validate()

        with DatabricksMigrationStateStore(config) as state_store:
            migrations_path = args.migrations_path
            all_migrations = parse_migrations_dir(migrations_path)

            if not all_migrations:
                print(f"No migrations found in {migrations_path}")
                return 0

            pending = plan(all_migrations, state_store)

            if not pending:
                print("No pending migrations")
                return 0

            print(f"Pending migrations ({len(pending)}):")
            for pm in pending:
                print(f"  V{pm.version}: {pm.name}")

            return 0
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Plan error: {e}", file=sys.stderr)
        return 1


def cmd_run(args: argparse.Namespace) -> int:
    """Run pending migrations."""
    try:
        from ucmt.migrations.parser import parse_migrations_dir
        from ucmt.migrations.runner import Runner, plan
        from ucmt.migrations.state import DatabricksMigrationStateStore

        config = build_config_from_env_and_validate()

        with DatabricksMigrationStateStore(config) as state_store:
            migrations_path = args.migrations_path
            all_migrations = parse_migrations_dir(migrations_path)

            pending = plan(all_migrations, state_store)

            if not pending:
                print("No pending migrations")
                return 0

            print(f"Found {len(pending)} pending migration(s):")
            for pm in pending:
                print(f"  V{pm.version}: {pm.name}")

            if args.dry_run:
                print("\nDry run - no migrations executed")
                runner = Runner(
                    state_store=state_store,
                    executor=lambda sql, version: None,
                    catalog=config.catalog,
                    schema=config.schema,
                )
                runner.apply(all_migrations, dry_run=True)
                return 0

            from ucmt.databricks.client import DatabricksClient

            with DatabricksClient(
                host=config.databricks_host,
                token=config.databricks_token,
                http_path=config.databricks_http_path,
            ) as client:

                def executor(sql: str, version: int) -> None:
                    for stmt in split_sql_statements(sql):
                        client.execute(stmt)

                runner = Runner(
                    state_store=state_store,
                    executor=executor,
                    catalog=config.catalog,
                    schema=config.schema,
                )

                print()
                try:
                    runner.apply(all_migrations)
                except Exception as e:
                    print(f"Run error: {e}", file=sys.stderr)
                    return 1

                print(f"\nSuccessfully applied {len(pending)} migration(s)")
                return 0
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Run error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
