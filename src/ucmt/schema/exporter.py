"""Export schema models to YAML files."""

from pathlib import Path
from typing import Any

import yaml

from ucmt.schema.models import Column, Schema, Table


def table_to_dict(table: Table) -> dict[str, Any]:
    """Convert a Table model to a dictionary suitable for YAML export."""
    data: dict[str, Any] = {"table": table.name}

    if table.comment:
        data["comment"] = table.comment

    columns = []
    for col in table.columns:
        columns.append(_column_to_dict(col))
    data["columns"] = columns

    if table.primary_key:
        pk_data: dict[str, Any] = {"columns": table.primary_key.columns}
        if table.primary_key.rely:
            pk_data["rely"] = True
        data["primary_key"] = pk_data

    if table.check_constraints:
        data["check_constraints"] = [
            {"name": cc.name, "expression": cc.expression}
            for cc in table.check_constraints
        ]

    if table.liquid_clustering:
        data["liquid_clustering"] = table.liquid_clustering

    if table.partitioned_by:
        data["partitioned_by"] = table.partitioned_by

    if table.table_properties:
        data["table_properties"] = table.table_properties

    return data


def _column_to_dict(col: Column) -> dict[str, Any]:
    """Convert a Column model to a dictionary."""
    data: dict[str, Any] = {"name": col.name, "type": col.type}

    if not col.nullable:
        data["nullable"] = False

    if col.default is not None:
        data["default"] = col.default

    if col.generated is not None:
        data["generated"] = col.generated

    if col.check is not None:
        data["check"] = col.check

    if col.foreign_key is not None:
        data["foreign_key"] = {
            "table": col.foreign_key.table,
            "column": col.foreign_key.column,
        }

    if col.comment is not None:
        data["comment"] = col.comment

    return data


def export_table_yaml(table: Table) -> str:
    """Export a single table to YAML string."""
    data = table_to_dict(table)
    return yaml.dump(
        data, default_flow_style=False, sort_keys=False, allow_unicode=True
    )


def export_schema_to_directory(schema: Schema, output_dir: Path) -> list[Path]:
    """Export all tables in a schema to individual YAML files.

    Returns list of created file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    created_files = []

    for table_name in sorted(schema.table_names()):
        table = schema.get_table(table_name)
        if table is None:
            continue

        file_path = output_dir / f"{table_name}.yaml"
        yaml_content = export_table_yaml(table)
        file_path.write_text(yaml_content)
        created_files.append(file_path)

    return created_files
