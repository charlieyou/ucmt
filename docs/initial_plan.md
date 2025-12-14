# Databricks SQL Migration Runner - Implementation Plan (v3 - Corrected)

> **Note:** This is an initial design document from early development. The actual implementation has diverged in several ways:
> - Package name is `ucmt` (not `db_migrate`)
> - Uses `databricks-sql-connector` instead of Spark for introspection
> - CLI semantics differ (e.g., `validate` currently only checks YAML syntax)
>
> See `ucmt --help` and module docstrings for current behavior.

## Overview

Build a SQL migration system for Databricks that integrates with Databricks Asset Bundles (DABs). The system uses **declarative schema files** as the source of truth and can:
1. Auto-generate migrations by diffing declared schema vs. current database state
2. Validate that migrations produce the declared schema
3. Execute migrations with proper state tracking

This follows the pattern of tools like Prisma, Atlas, and Alembic autogenerate.

---

## ⚠️ Databricks/Unity Catalog Constraints

**Before implementing, understand these platform limitations:**

| Feature | Support | Notes |
|---------|---------|-------|
| Traditional indexes | ❌ None | Use liquid clustering or Z-ORDER instead |
| PRIMARY KEY | ✅ Informational | **Not enforced** - for optimizer hints and BI tools only |
| FOREIGN KEY | ✅ Informational | **Not enforced** - no referential integrity checks |
| CHECK constraints | ✅ Enforced | Transactions fail on violation |
| NOT NULL | ✅ Enforced | |
| UNIQUE constraints | ❌ None | Must enforce at application/pipeline level |
| Liquid clustering | ✅ | Replaces partitioning + Z-ORDER for new tables |
| Change partitioning | ❌ | Requires table recreation |
| DROP COLUMN | ✅ | Requires column mapping mode enabled |
| Type widening | ✅ Limited | INT→BIGINT, FLOAT→DOUBLE, etc. |
| Type narrowing | ❌ | Not supported |

---

## 1. Project Structure

```
migrations/
├── pyproject.toml
├── databricks.yml
├── src/
│   └── db_migrate/
│       ├── __init__.py
│       ├── runner.py          # Migration execution
│       ├── state.py           # Migration history table
│       ├── parser.py          # SQL file parsing
│       ├── schema/
│       │   ├── __init__.py
│       │   ├── models.py      # Schema representation classes
│       │   ├── loader.py      # Parse schema definition files
│       │   ├── introspect.py  # Read current DB state from Unity Catalog
│       │   ├── diff.py        # Compare schemas, generate changes
│       │   └── codegen.py     # Generate SQL migrations from diffs
│       ├── cli.py             # Entry points
│       └── config.py          # Configuration
├── schema/                    # CANONICAL SCHEMA DEFINITIONS
│   ├── tables/
│   │   ├── users.yaml
│   │   ├── transactions.yaml
│   │   └── ...
│   └── schema.yaml            # Optional: single-file alternative
├── sql/
│   └── migrations/            # Generated/manual migrations
│       ├── V001__initial_schema.sql
│       └── ...
├── tests/
└── README.md
```

---

## 2. Schema Definition Format

### YAML per table (recommended)

`schema/tables/users.yaml`:
```yaml
table: users
description: Core user accounts

columns:
  - name: id
    type: BIGINT
    generated: ALWAYS AS IDENTITY
    nullable: false
    
  - name: email
    type: STRING
    nullable: false
    
  - name: display_name
    type: STRING
    nullable: true
    
  - name: status
    type: STRING
    nullable: false
    default: "'active'"
    check: "status IN ('active', 'suspended', 'deleted')"
    
  - name: metadata
    type: MAP<STRING, STRING>
    nullable: true
    
  - name: created_at
    type: TIMESTAMP
    nullable: false
    default: current_timestamp()
    
  - name: updated_at
    type: TIMESTAMP
    nullable: true

# Informational only - NOT enforced by Databricks
# Useful for: query optimization (with RELY), BI tool integration, documentation
primary_key: 
  columns: [id]
  rely: true  # Enable query optimizer to use this constraint

# NO indexes field - Databricks doesn't support traditional indexes
# Use liquid_clustering instead for query optimization

# Liquid clustering replaces partitioning + Z-ORDER for query optimization
# Max 4 columns, only columns with statistics (first 32 by default)
liquid_clustering: [status, created_at]

# Alternative: traditional partitioning (legacy, cannot be changed after creation)
# partitioned_by: [status]

table_properties:
  delta.enableChangeDataFeed: "true"
  delta.autoOptimize.optimizeWrite: "true"
  # Required for DROP COLUMN and RENAME COLUMN support:
  delta.columnMapping.mode: "name"

comment: "Core user accounts table"
```

`schema/tables/transactions.yaml`:
```yaml
table: transactions
description: Financial transactions

columns:
  - name: id
    type: BIGINT
    generated: ALWAYS AS IDENTITY
    nullable: false
    
  - name: user_id
    type: BIGINT
    nullable: false
    # Foreign key is INFORMATIONAL ONLY - not enforced!
    # Referential integrity must be enforced by your pipeline
    foreign_key:
      table: users
      column: id
    
  - name: amount
    type: DECIMAL(18, 8)
    nullable: false
    
  - name: currency
    type: STRING
    nullable: false
    
  - name: tx_type
    type: STRING
    nullable: false
    check: "tx_type IN ('deposit', 'withdrawal', 'transfer', 'fee')"
    
  - name: tx_date
    type: DATE
    nullable: false
    
  - name: created_at
    type: TIMESTAMP
    nullable: false
    default: current_timestamp()

primary_key:
  columns: [id]
  rely: true

liquid_clustering: [user_id, tx_date]

table_properties:
  delta.enableChangeDataFeed: "true"
  delta.columnMapping.mode: "name"
```

### Key Schema Design Notes

1. **No `indexes` field** - Databricks doesn't have traditional indexes
2. **`liquid_clustering`** replaces indexes for query optimization
3. **`primary_key.rely: true`** enables query optimizer to use the constraint
4. **Foreign keys** are documentation only - add a comment noting this
5. **Enable column mapping mode** on all tables for future flexibility

---

## 3. Schema Model Classes

### `schema/models.py`

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class ForeignKey:
    """
    Foreign key reference.
    
    WARNING: Foreign keys in Databricks are INFORMATIONAL ONLY.
    They are NOT enforced - no referential integrity checks occur.
    Use for: query hints, BI tool integration, documentation.
    """
    table: str
    column: str

@dataclass
class PrimaryKey:
    """
    Primary key definition.
    
    WARNING: Primary keys in Databricks are INFORMATIONAL ONLY.
    Uniqueness is NOT enforced. Use MERGE with dedup logic in pipelines.
    
    Args:
        columns: List of column names in the primary key
        rely: If True, query optimizer can use this constraint for optimizations
    """
    columns: list[str]
    rely: bool = False

@dataclass
class CheckConstraint:
    """
    CHECK constraint - these ARE enforced by Databricks.
    Transactions will fail if constraint is violated.
    """
    name: str
    expression: str

@dataclass
class Column:
    name: str
    type: str  # Raw Spark SQL type string
    nullable: bool = True
    default: Optional[str] = None
    generated: Optional[str] = None  # "ALWAYS AS IDENTITY"
    check: Optional[str] = None  # Inline CHECK constraint expression
    foreign_key: Optional[ForeignKey] = None  # Informational only!
    comment: Optional[str] = None

@dataclass
class Table:
    name: str
    columns: list[Column]
    primary_key: Optional[PrimaryKey] = None
    check_constraints: list[CheckConstraint] = field(default_factory=list)
    
    # Query optimization (choose one approach)
    liquid_clustering: list[str] = field(default_factory=list)  # Recommended
    partitioned_by: list[str] = field(default_factory=list)     # Legacy
    
    table_properties: dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None
    
    def has_column_mapping(self) -> bool:
        """Check if column mapping mode is enabled (required for DROP/RENAME)."""
        return self.table_properties.get("delta.columnMapping.mode") == "name"

@dataclass
class Schema:
    """Complete schema definition."""
    tables: dict[str, Table]  # table_name -> Table
    
    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name)
    
    def table_names(self) -> set[str]:
        return set(self.tables.keys())
```

---

## 4. Schema Introspection

### `schema/introspect.py`

Read current database state from Unity Catalog's information_schema:

```python
class SchemaIntrospector:
    """
    Read current schema state from Databricks Unity Catalog.
    
    Uses information_schema views which are available in Unity Catalog.
    """
    
    def __init__(self, spark, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def introspect(self) -> Schema:
        """Query information_schema to build Schema object."""
        tables = {}
        for table_name in self._get_table_names():
            tables[table_name] = self._introspect_table(table_name)
        return Schema(tables=tables)
    
    def _get_table_names(self) -> list[str]:
        """Get all table names in schema (excluding internal tables)."""
        query = f"""
            SELECT table_name 
            FROM {self.catalog}.information_schema.tables
            WHERE table_schema = '{self.schema}'
              AND table_type IN ('MANAGED', 'EXTERNAL')
              AND table_name NOT LIKE '\\_%'
        """
        return [row.table_name for row in self.spark.sql(query).collect()]
    
    def _introspect_table(self, table_name: str) -> Table:
        """Build Table object from information_schema + DESCRIBE queries."""
        columns = self._get_columns(table_name)
        primary_key = self._get_primary_key(table_name)
        check_constraints = self._get_check_constraints(table_name)
        properties = self._get_table_properties(table_name)
        clustering = self._get_clustering(table_name)
        partitioning = self._get_partitioning(table_name)
        
        return Table(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            check_constraints=check_constraints,
            liquid_clustering=clustering,
            partitioned_by=partitioning,
            table_properties=properties,
        )
    
    def _get_columns(self, table_name: str) -> list[Column]:
        """Get column definitions from information_schema.columns."""
        query = f"""
            SELECT 
                column_name,
                full_data_type,
                is_nullable,
                column_default,
                comment
            FROM {self.catalog}.information_schema.columns
            WHERE table_schema = '{self.schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        rows = self.spark.sql(query).collect()
        
        columns = []
        for row in rows:
            # Get foreign key info if exists
            fk = self._get_foreign_key_for_column(table_name, row.column_name)
            
            columns.append(Column(
                name=row.column_name,
                type=row.full_data_type,
                nullable=(row.is_nullable == 'YES'),
                default=row.column_default,
                foreign_key=fk,
                comment=row.comment
            ))
        return columns
    
    def _get_primary_key(self, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key from information_schema.table_constraints."""
        query = f"""
            SELECT 
                tc.constraint_name,
                tc.enforced,
                kcu.column_name,
                kcu.ordinal_position
            FROM {self.catalog}.information_schema.table_constraints tc
            JOIN {self.catalog}.information_schema.key_column_usage kcu
              ON tc.constraint_catalog = kcu.constraint_catalog
              AND tc.constraint_schema = kcu.constraint_schema
              AND tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = '{self.schema}'
              AND tc.table_name = '{table_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        rows = self.spark.sql(query).collect()
        
        if not rows:
            return None
        
        columns = [row.column_name for row in rows]
        # Check if RELY is set (enforced = 'YES' means RELY was specified)
        rely = rows[0].enforced == 'YES' if rows else False
        
        return PrimaryKey(columns=columns, rely=rely)
    
    def _get_foreign_key_for_column(self, table_name: str, column_name: str) -> Optional[ForeignKey]:
        """Get foreign key reference for a specific column."""
        query = f"""
            SELECT 
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column
            FROM {self.catalog}.information_schema.referential_constraints rc
            JOIN {self.catalog}.information_schema.key_column_usage kcu
              ON rc.constraint_catalog = kcu.constraint_catalog
              AND rc.constraint_schema = kcu.constraint_schema  
              AND rc.constraint_name = kcu.constraint_name
            JOIN {self.catalog}.information_schema.constraint_column_usage ccu
              ON rc.unique_constraint_catalog = ccu.constraint_catalog
              AND rc.unique_constraint_schema = ccu.constraint_schema
              AND rc.unique_constraint_name = ccu.constraint_name
            WHERE kcu.table_schema = '{self.schema}'
              AND kcu.table_name = '{table_name}'
              AND kcu.column_name = '{column_name}'
        """
        rows = self.spark.sql(query).collect()
        
        if not rows:
            return None
        
        return ForeignKey(
            table=rows[0].referenced_table,
            column=rows[0].referenced_column
        )
    
    def _get_check_constraints(self, table_name: str) -> list[CheckConstraint]:
        """
        Get CHECK constraints from table properties.
        
        CHECK constraints are stored in table properties as:
        delta.constraints.{constraint_name} = '{expression}'
        """
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        rows = self.spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
        
        constraints = []
        for row in rows:
            if row.key.startswith("delta.constraints."):
                name = row.key.replace("delta.constraints.", "")
                constraints.append(CheckConstraint(name=name, expression=row.value))
        
        return constraints
    
    def _get_table_properties(self, table_name: str) -> dict[str, str]:
        """Get table properties via SHOW TBLPROPERTIES."""
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        rows = self.spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
        
        # Filter to relevant properties (exclude internal delta.* except useful ones)
        keep_prefixes = [
            "delta.enableChangeDataFeed",
            "delta.autoOptimize",
            "delta.columnMapping",
            "delta.minReaderVersion",
            "delta.minWriterVersion",
        ]
        
        props = {}
        for row in rows:
            if any(row.key.startswith(p) for p in keep_prefixes):
                props[row.key] = row.value
        
        return props
    
    def _get_clustering(self, table_name: str) -> list[str]:
        """
        Get liquid clustering columns from DESCRIBE DETAIL.
        
        Clustering info is in the 'clusteringColumns' field of DESCRIBE DETAIL.
        """
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]
            # clusteringColumns is an array field
            if hasattr(detail, 'clusteringColumns') and detail.clusteringColumns:
                return list(detail.clusteringColumns)
        except Exception:
            pass
        return []
    
    def _get_partitioning(self, table_name: str) -> list[str]:
        """Get partition columns from DESCRIBE DETAIL."""
        fqn = f"{self.catalog}.{self.schema}.{table_name}"
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]
            if hasattr(detail, 'partitionColumns') and detail.partitionColumns:
                return list(detail.partitionColumns)
        except Exception:
            pass
        return []
```

---

## 5. Schema Diff Engine

### `schema/diff.py`

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

class ChangeType(Enum):
    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    ALTER_COLUMN_TYPE = "alter_column_type"
    ALTER_COLUMN_NULLABILITY = "alter_column_nullability"
    ALTER_COLUMN_DEFAULT = "alter_column_default"
    ADD_CHECK_CONSTRAINT = "add_check_constraint"
    DROP_CHECK_CONSTRAINT = "drop_check_constraint"
    SET_PRIMARY_KEY = "set_primary_key"
    DROP_PRIMARY_KEY = "drop_primary_key"
    ADD_FOREIGN_KEY = "add_foreign_key"
    DROP_FOREIGN_KEY = "drop_foreign_key"
    ALTER_CLUSTERING = "alter_clustering"
    ALTER_TABLE_PROPERTIES = "alter_table_properties"
    # Not supported - will generate error
    ALTER_PARTITIONING = "alter_partitioning"

@dataclass
class SchemaChange:
    change_type: ChangeType
    table_name: str
    details: dict
    
    # Metadata
    is_destructive: bool = False
    requires_column_mapping: bool = False
    is_unsupported: bool = False
    error_message: Optional[str] = None

class SchemaDiffer:
    """Compare two schemas and produce list of changes."""
    
    def __init__(self, source: Schema, target: Schema):
        self.source = source  # Current database state
        self.target = target  # Desired state from schema files
    
    def diff(self) -> list[SchemaChange]:
        """Compute changes needed to transform source into target."""
        changes = []
        
        source_tables = self.source.table_names()
        target_tables = self.target.table_names()
        
        # New tables
        for name in target_tables - source_tables:
            changes.append(SchemaChange(
                change_type=ChangeType.CREATE_TABLE,
                table_name=name,
                details={"table": self.target.get_table(name)}
            ))
        
        # Dropped tables
        for name in source_tables - target_tables:
            changes.append(SchemaChange(
                change_type=ChangeType.DROP_TABLE,
                table_name=name,
                details={},
                is_destructive=True
            ))
        
        # Modified tables
        for name in source_tables & target_tables:
            changes.extend(self._diff_table(
                self.source.get_table(name),
                self.target.get_table(name)
            ))
        
        return self._order_changes(changes)
    
    def _diff_table(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff a single table."""
        changes = []
        
        # Column changes
        changes.extend(self._diff_columns(source, target))
        
        # Constraint changes
        changes.extend(self._diff_constraints(source, target))
        
        # Clustering changes
        changes.extend(self._diff_clustering(source, target))
        
        # Partitioning changes (will generate error if different)
        changes.extend(self._diff_partitioning(source, target))
        
        # Property changes
        changes.extend(self._diff_properties(source, target))
        
        return changes
    
    def _diff_columns(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff columns between source and target table."""
        changes = []
        
        source_cols = {c.name: c for c in source.columns}
        target_cols = {c.name: c for c in target.columns}
        
        # Added columns
        for name in set(target_cols) - set(source_cols):
            col = target_cols[name]
            changes.append(SchemaChange(
                change_type=ChangeType.ADD_COLUMN,
                table_name=source.name,
                details={"column": col}
            ))
        
        # Dropped columns
        for name in set(source_cols) - set(target_cols):
            changes.append(SchemaChange(
                change_type=ChangeType.DROP_COLUMN,
                table_name=source.name,
                details={"column_name": name},
                is_destructive=True,
                requires_column_mapping=True
            ))
        
        # Modified columns
        for name in set(source_cols) & set(target_cols):
            col_changes = self._diff_column(
                source.name, source_cols[name], target_cols[name]
            )
            changes.extend(col_changes)
        
        return changes
    
    def _diff_column(self, table_name: str, source: Column, target: Column) -> list[SchemaChange]:
        """Diff a single column."""
        changes = []
        
        # Type change
        if source.type.upper() != target.type.upper():
            is_valid, error = self._validate_type_change(source.type, target.type)
            changes.append(SchemaChange(
                change_type=ChangeType.ALTER_COLUMN_TYPE,
                table_name=table_name,
                details={
                    "column_name": source.name,
                    "from_type": source.type,
                    "to_type": target.type
                },
                is_unsupported=not is_valid,
                error_message=error
            ))
        
        # Nullability change
        if source.nullable != target.nullable:
            changes.append(SchemaChange(
                change_type=ChangeType.ALTER_COLUMN_NULLABILITY,
                table_name=table_name,
                details={
                    "column_name": source.name,
                    "from_nullable": source.nullable,
                    "to_nullable": target.nullable
                }
            ))
        
        # Default change
        if source.default != target.default:
            changes.append(SchemaChange(
                change_type=ChangeType.ALTER_COLUMN_DEFAULT,
                table_name=table_name,
                details={
                    "column_name": source.name,
                    "from_default": source.default,
                    "to_default": target.default
                }
            ))
        
        return changes
    
    def _validate_type_change(self, from_type: str, to_type: str) -> tuple[bool, Optional[str]]:
        """
        Validate if a type change is supported in Delta Lake.
        
        Delta only supports widening type changes:
        - BYTE -> SHORT -> INT -> BIGINT
        - FLOAT -> DOUBLE
        - Increasing precision/scale of DECIMAL
        """
        # Simplified validation - expand as needed
        widening_allowed = {
            ("INT", "BIGINT"),
            ("SMALLINT", "INT"),
            ("SMALLINT", "BIGINT"),
            ("TINYINT", "SMALLINT"),
            ("TINYINT", "INT"),
            ("TINYINT", "BIGINT"),
            ("FLOAT", "DOUBLE"),
        }
        
        from_upper = from_type.upper().split("(")[0]
        to_upper = to_type.upper().split("(")[0]
        
        if (from_upper, to_upper) in widening_allowed:
            return True, None
        
        if from_upper == to_upper:
            # Same base type - might be precision change
            # For DECIMAL, check if scale/precision increased
            return True, None  # Simplified - should validate properly
        
        return False, f"Type change from {from_type} to {to_type} is not supported in Delta Lake. Only widening conversions are allowed."
    
    def _diff_clustering(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff liquid clustering configuration."""
        changes = []
        
        if set(source.liquid_clustering) != set(target.liquid_clustering):
            changes.append(SchemaChange(
                change_type=ChangeType.ALTER_CLUSTERING,
                table_name=source.name,
                details={
                    "from_columns": source.liquid_clustering,
                    "to_columns": target.liquid_clustering
                }
            ))
        
        return changes
    
    def _diff_partitioning(self, source: Table, target: Table) -> list[SchemaChange]:
        """
        Diff partitioning - generates ERROR if different.
        
        Partitioning CANNOT be changed after table creation in Delta Lake.
        """
        changes = []
        
        if set(source.partitioned_by) != set(target.partitioned_by):
            changes.append(SchemaChange(
                change_type=ChangeType.ALTER_PARTITIONING,
                table_name=source.name,
                details={
                    "from_columns": source.partitioned_by,
                    "to_columns": target.partitioned_by
                },
                is_unsupported=True,
                error_message=(
                    f"Cannot change partitioning for table '{source.name}'. "
                    f"Current: {source.partitioned_by}, Desired: {target.partitioned_by}. "
                    "Delta Lake does not support changing partition columns after table creation. "
                    "You must recreate the table."
                )
            ))
        
        return changes
    
    def _diff_constraints(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff constraints between tables."""
        changes = []
        
        # Primary key changes
        if source.primary_key != target.primary_key:
            if source.primary_key:
                changes.append(SchemaChange(
                    change_type=ChangeType.DROP_PRIMARY_KEY,
                    table_name=source.name,
                    details={"constraint": source.primary_key}
                ))
            if target.primary_key:
                changes.append(SchemaChange(
                    change_type=ChangeType.SET_PRIMARY_KEY,
                    table_name=source.name,
                    details={"constraint": target.primary_key}
                ))
        
        # CHECK constraints
        source_checks = {c.name: c for c in source.check_constraints}
        target_checks = {c.name: c for c in target.check_constraints}
        
        for name in set(target_checks) - set(source_checks):
            changes.append(SchemaChange(
                change_type=ChangeType.ADD_CHECK_CONSTRAINT,
                table_name=source.name,
                details={"constraint": target_checks[name]}
            ))
        
        for name in set(source_checks) - set(target_checks):
            changes.append(SchemaChange(
                change_type=ChangeType.DROP_CHECK_CONSTRAINT,
                table_name=source.name,
                details={"constraint_name": name}
            ))
        
        return changes
    
    def _diff_properties(self, source: Table, target: Table) -> list[SchemaChange]:
        """Diff table properties."""
        changes = []
        
        # Find changed/new properties
        changed_props = {}
        for key, value in target.table_properties.items():
            if source.table_properties.get(key) != value:
                changed_props[key] = value
        
        if changed_props:
            changes.append(SchemaChange(
                change_type=ChangeType.ALTER_TABLE_PROPERTIES,
                table_name=source.name,
                details={"properties": changed_props}
            ))
        
        return changes
    
    def _order_changes(self, changes: list[SchemaChange]) -> list[SchemaChange]:
        """Order changes by dependency (creates first, drops last)."""
        order = {
            ChangeType.CREATE_TABLE: 0,
            ChangeType.ADD_COLUMN: 1,
            ChangeType.ALTER_COLUMN_TYPE: 2,
            ChangeType.ALTER_COLUMN_NULLABILITY: 2,
            ChangeType.ALTER_COLUMN_DEFAULT: 2,
            ChangeType.SET_PRIMARY_KEY: 3,
            ChangeType.ADD_FOREIGN_KEY: 3,
            ChangeType.ADD_CHECK_CONSTRAINT: 3,
            ChangeType.ALTER_CLUSTERING: 4,
            ChangeType.ALTER_TABLE_PROPERTIES: 4,
            ChangeType.DROP_CHECK_CONSTRAINT: 5,
            ChangeType.DROP_FOREIGN_KEY: 5,
            ChangeType.DROP_PRIMARY_KEY: 5,
            ChangeType.DROP_COLUMN: 6,
            ChangeType.DROP_TABLE: 7,
        }
        return sorted(changes, key=lambda c: order.get(c.change_type, 99))
```

---

## 6. Migration Code Generator

### `schema/codegen.py`

```python
from datetime import datetime

class MigrationGenerator:
    """Generate SQL migration files from schema changes."""
    
    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema
    
    def generate(self, changes: list[SchemaChange], description: str) -> str:
        """Generate SQL migration file content."""
        # Check for unsupported changes
        errors = [c for c in changes if c.is_unsupported]
        if errors:
            error_msgs = "\n".join(f"-- ERROR: {c.error_message}" for c in errors)
            raise ValueError(f"Cannot generate migration - unsupported changes:\n{error_msgs}")
        
        lines = [
            f"-- Migration: Auto-generated",
            f"-- Description: {description}",
            f"-- Generated: {datetime.now().isoformat()}",
            "",
            "-- Variable substitution: ${catalog}, ${schema}",
            "",
        ]
        
        # Add warnings for destructive changes
        destructive = [c for c in changes if c.is_destructive]
        if destructive:
            lines.append("-- ⚠️  WARNING: This migration contains destructive changes:")
            for c in destructive:
                lines.append(f"--   - {c.change_type.value}: {c.table_name}")
            lines.append("")
        
        for change in changes:
            lines.append(f"-- {change.change_type.value}: {change.table_name}")
            sql = self._generate_sql(change)
            if change.requires_column_mapping:
                lines.append("-- Requires: delta.columnMapping.mode = 'name'")
            lines.append(sql)
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_sql(self, change: SchemaChange) -> str:
        """Generate SQL for a single change."""
        generators = {
            ChangeType.CREATE_TABLE: self._gen_create_table,
            ChangeType.DROP_TABLE: self._gen_drop_table,
            ChangeType.ADD_COLUMN: self._gen_add_column,
            ChangeType.DROP_COLUMN: self._gen_drop_column,
            ChangeType.ALTER_COLUMN_TYPE: self._gen_alter_column_type,
            ChangeType.ALTER_COLUMN_NULLABILITY: self._gen_alter_nullability,
            ChangeType.ALTER_COLUMN_DEFAULT: self._gen_alter_default,
            ChangeType.ADD_CHECK_CONSTRAINT: self._gen_add_check,
            ChangeType.DROP_CHECK_CONSTRAINT: self._gen_drop_check,
            ChangeType.SET_PRIMARY_KEY: self._gen_set_pk,
            ChangeType.DROP_PRIMARY_KEY: self._gen_drop_pk,
            ChangeType.ALTER_CLUSTERING: self._gen_alter_clustering,
            ChangeType.ALTER_TABLE_PROPERTIES: self._gen_alter_properties,
        }
        generator = generators.get(change.change_type)
        if not generator:
            raise ValueError(f"No generator for {change.change_type}")
        return generator(change)
    
    def _fqn(self, table_name: str) -> str:
        """Generate fully-qualified table name with variables."""
        return f"${{catalog}}.${{schema}}.{table_name}"
    
    def _gen_create_table(self, change: SchemaChange) -> str:
        """Generate CREATE TABLE statement."""
        table: Table = change.details["table"]
        fqn = self._fqn(table.name)
        
        col_defs = []
        for col in table.columns:
            col_def = f"    {col.name} {col.type}"
            if col.generated:
                col_def += f" GENERATED {col.generated}"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.default:
                col_def += f" DEFAULT {col.default}"
            if col.comment:
                col_def += f" COMMENT '{col.comment}'"
            col_defs.append(col_def)
        
        # Add primary key constraint (informational only)
        if table.primary_key:
            pk_cols = ", ".join(table.primary_key.columns)
            rely = " RELY" if table.primary_key.rely else " NORELY"
            col_defs.append(
                f"    CONSTRAINT pk_{table.name} PRIMARY KEY ({pk_cols}){rely}"
            )
        
        columns_sql = ",\n".join(col_defs)
        
        sql = f"CREATE TABLE IF NOT EXISTS {fqn} (\n{columns_sql}\n) USING DELTA"
        
        # Liquid clustering (preferred over partitioning)
        if table.liquid_clustering:
            cols = ", ".join(table.liquid_clustering)
            sql += f"\nCLUSTER BY ({cols})"
        # Legacy partitioning
        elif table.partitioned_by:
            cols = ", ".join(table.partitioned_by)
            sql += f"\nPARTITIONED BY ({cols})"
        
        # Table properties
        if table.table_properties:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in table.table_properties.items())
            sql += f"\nTBLPROPERTIES ({props})"
        
        if table.comment:
            sql += f"\nCOMMENT '{table.comment}'"
        
        return sql + ";"
    
    def _gen_add_column(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ADD COLUMN."""
        col: Column = change.details["column"]
        fqn = self._fqn(change.table_name)
        
        col_def = f"{col.name} {col.type}"
        if not col.nullable:
            if not col.default:
                raise ValueError(
                    f"Cannot add non-nullable column '{col.name}' without a default value."
                )
            col_def += " NOT NULL"
        if col.default:
            col_def += f" DEFAULT {col.default}"
        if col.comment:
            col_def += f" COMMENT '{col.comment}'"
        
        return f"ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {col_def};"
    
    def _gen_drop_column(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE DROP COLUMN."""
        col_name = change.details["column_name"]
        fqn = self._fqn(change.table_name)
        
        return f"""-- ⚠️  DESTRUCTIVE: Dropping column {col_name}
-- Requires delta.columnMapping.mode = 'name' on the table
ALTER TABLE {fqn} DROP COLUMN IF EXISTS {col_name};"""
    
    def _gen_alter_column_type(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ALTER COLUMN TYPE."""
        fqn = self._fqn(change.table_name)
        col_name = change.details["column_name"]
        to_type = change.details["to_type"]
        
        return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} TYPE {to_type};"
    
    def _gen_alter_nullability(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET/DROP NOT NULL."""
        fqn = self._fqn(change.table_name)
        col_name = change.details["column_name"]
        to_nullable = change.details["to_nullable"]
        
        if to_nullable:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} DROP NOT NULL;"
        else:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} SET NOT NULL;"
    
    def _gen_alter_default(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ALTER COLUMN SET/DROP DEFAULT."""
        fqn = self._fqn(change.table_name)
        col_name = change.details["column_name"]
        to_default = change.details["to_default"]
        
        if to_default:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} SET DEFAULT {to_default};"
        else:
            return f"ALTER TABLE {fqn} ALTER COLUMN {col_name} DROP DEFAULT;"
    
    def _gen_add_check(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ADD CONSTRAINT CHECK."""
        fqn = self._fqn(change.table_name)
        constraint: CheckConstraint = change.details["constraint"]
        
        return f"ALTER TABLE {fqn} ADD CONSTRAINT {constraint.name} CHECK ({constraint.expression});"
    
    def _gen_drop_check(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT."""
        fqn = self._fqn(change.table_name)
        name = change.details["constraint_name"]
        
        return f"ALTER TABLE {fqn} DROP CONSTRAINT IF EXISTS {name};"
    
    def _gen_set_pk(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE ADD PRIMARY KEY."""
        fqn = self._fqn(change.table_name)
        pk: PrimaryKey = change.details["constraint"]
        cols = ", ".join(pk.columns)
        rely = " RELY" if pk.rely else " NORELY"
        
        return f"""-- Note: Primary key is INFORMATIONAL ONLY - not enforced!
ALTER TABLE {fqn} ADD CONSTRAINT pk_{change.table_name} PRIMARY KEY ({cols}){rely};"""
    
    def _gen_drop_pk(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE DROP PRIMARY KEY."""
        fqn = self._fqn(change.table_name)
        
        return f"ALTER TABLE {fqn} DROP PRIMARY KEY IF EXISTS;"
    
    def _gen_alter_clustering(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE CLUSTER BY."""
        fqn = self._fqn(change.table_name)
        to_cols = change.details["to_columns"]
        
        if not to_cols:
            return f"""ALTER TABLE {fqn} CLUSTER BY NONE;
-- Note: Run OPTIMIZE to apply clustering changes"""
        
        cols = ", ".join(to_cols)
        return f"""ALTER TABLE {fqn} CLUSTER BY ({cols});
-- Note: Run OPTIMIZE to apply clustering changes"""
    
    def _gen_alter_properties(self, change: SchemaChange) -> str:
        """Generate ALTER TABLE SET TBLPROPERTIES."""
        fqn = self._fqn(change.table_name)
        props = change.details["properties"]
        
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        return f"ALTER TABLE {fqn} SET TBLPROPERTIES ({props_sql});"
    
    def _gen_drop_table(self, change: SchemaChange) -> str:
        """Generate DROP TABLE (commented out for safety)."""
        fqn = self._fqn(change.table_name)
        
        return f"""-- ⚠️  DESTRUCTIVE: Dropping table
-- Uncomment the following line to enable. Data will be permanently deleted!
-- DROP TABLE IF EXISTS {fqn};"""
```

---

## 7. CLI Commands

```bash
# Generate migration from schema diff
db-migrate generate "add user preferences table"

# Show diff without creating file
db-migrate diff

# Run pending migrations  
db-migrate run

# Run with OPTIMIZE after clustering changes
db-migrate run --optimize

# Validate schema files
db-migrate validate

# Pull current DB schema into YAML files
db-migrate pull

# Status of applied migrations
db-migrate status
```

---

## 8. Key Implementation Notes

### Things That Work Differently in Databricks

1. **No unique constraints** — Enforce uniqueness via:
   - `MERGE` with dedup logic
   - DLT expectations
   - Application-level checks

2. **Primary/foreign keys are hints only** — Add `rely: true` for optimizer to use them

3. **Clustering replaces indexes** — Use `CLUSTER BY` instead of `CREATE INDEX`

4. **Must enable column mapping for schema evolution** — Set `delta.columnMapping.mode = 'name'` on all tables

5. **Partitioning is immutable** — Cannot change after table creation; use liquid clustering instead

6. **Type changes are limited** — Only widening allowed (INT→BIGINT, FLOAT→DOUBLE)

### Recommended Table Properties

Always include these in schema definitions:

```yaml
table_properties:
  # Enable CDC for downstream consumers
  delta.enableChangeDataFeed: "true"
  # Auto-compact small files
  delta.autoOptimize.optimizeWrite: "true"
  # Enable DROP/RENAME column support
  delta.columnMapping.mode: "name"
```

---

## 9. Acceptance Criteria

- [ ] Schema YAML format correctly represents Databricks capabilities
- [ ] Introspection accurately reads from information_schema
- [ ] Diff correctly identifies supported vs unsupported changes
- [ ] Generator produces valid Databricks SQL
- [ ] Primary/foreign keys are clearly documented as informational only
- [ ] Liquid clustering is used instead of indexes
- [ ] Partitioning changes are blocked with clear error
- [ ] Column mapping mode is checked before DROP COLUMN
- [ ] Destructive changes require confirmation
- [ ] OPTIMIZE hint is included after clustering changes
