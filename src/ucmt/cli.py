"""Command-line interface for ucmt."""

import argparse
import sys
from pathlib import Path

from ucmt.config import Config
from ucmt.schema.codegen import MigrationGenerator
from ucmt.schema.diff import SchemaDiffer
from ucmt.schema.loader import load_schema


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
    generate_parser.add_argument("--schema-path", type=Path, default=Path("schema/tables"))

    validate_parser = subparsers.add_parser("validate", help="Validate schema files")
    validate_parser.add_argument("--schema-path", type=Path, default=Path("schema/tables"))

    subparsers.add_parser("status", help="Show migration status")
    subparsers.add_parser("run", help="Run pending migrations")

    args = parser.parse_args()

    if args.command == "validate":
        return cmd_validate(args)
    elif args.command == "diff":
        return cmd_diff(args)
    elif args.command == "generate":
        return cmd_generate(args)
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


if __name__ == "__main__":
    sys.exit(main())
