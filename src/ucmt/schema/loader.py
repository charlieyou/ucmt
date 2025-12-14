"""Load schema definitions from YAML files."""

from pathlib import Path

import yaml

from ucmt.exceptions import SchemaLoadError
from ucmt.schema.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    PrimaryKey,
    Schema,
    Table,
)


def load_schema(schema_path: Path) -> Schema:
    """Load schema from a directory of YAML files or a single file."""
    if schema_path.is_file():
        return _load_single_file(schema_path)
    elif schema_path.is_dir():
        return _load_directory(schema_path)
    else:
        raise SchemaLoadError(f"Schema path does not exist: {schema_path}")


def _load_directory(directory: Path) -> Schema:
    """Load schema from a directory of YAML files."""
    tables: dict[str, Table] = {}
    for yaml_file in directory.glob("*.yaml"):
        table = _parse_table_yaml(yaml_file)
        tables[table.name] = table
    return Schema(tables=tables)


def _load_single_file(file_path: Path) -> Schema:
    """Load schema from a single YAML file."""
    with open(file_path) as f:
        data = yaml.safe_load(f)

    tables: dict[str, Table] = {}
    for table_data in data.get("tables", []):
        table = _parse_table_dict(table_data)
        tables[table.name] = table
    return Schema(tables=tables)


def _parse_table_yaml(file_path: Path) -> Table:
    """Parse a table definition from a YAML file."""
    with open(file_path) as f:
        data = yaml.safe_load(f)
    return _parse_table_dict(data)


def _parse_table_dict(data: dict) -> Table:
    """Parse a table definition from a dictionary."""
    name = data.get("table")
    if not name:
        raise SchemaLoadError("Table definition missing 'table' field")

    columns = [_parse_column(col) for col in data.get("columns", [])]

    primary_key = None
    if pk_data := data.get("primary_key"):
        primary_key = PrimaryKey(
            columns=pk_data.get("columns", []),
            rely=pk_data.get("rely", False),
        )

    check_constraints = []
    for cc_data in data.get("check_constraints", []):
        check_constraints.append(
            CheckConstraint(
                name=cc_data.get("name"),
                expression=cc_data.get("expression"),
            )
        )

    return Table(
        name=name,
        columns=columns,
        primary_key=primary_key,
        check_constraints=check_constraints,
        liquid_clustering=data.get("liquid_clustering", []),
        partitioned_by=data.get("partitioned_by", []),
        table_properties=data.get("table_properties", {}),
        comment=data.get("comment"),
    )


def _parse_column(data: dict) -> Column:
    """Parse a column definition from a dictionary."""
    foreign_key = None
    if fk_data := data.get("foreign_key"):
        foreign_key = ForeignKey(
            table=fk_data.get("table"),
            column=fk_data.get("column"),
        )

    return Column(
        name=data.get("name"),
        type=data.get("type"),
        nullable=data.get("nullable", True),
        default=data.get("default"),
        generated=data.get("generated"),
        check=data.get("check"),
        foreign_key=foreign_key,
        comment=data.get("comment"),
    )
