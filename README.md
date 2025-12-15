# UCMT: Unity Catalog Migration Tool

A SQL migration system for Databricks that integrates with Databricks Asset Bundles (DABs). Uses **declarative YAML schema files** as the source of truth.

## Features

- **YAML schema loader** with validation
- **Schema diff engine** (detects adds, drops, type changes)
- **SQL migration codegen** from schema changes
- **Migration file parser** (V###__name.sql format)
- **Migration runner** with state tracking
- **Databricks client** and state store
- **Schema introspection** from Unity Catalog
- **Pull command** to generate YAML from existing schema

## CLI Commands

```bash
ucmt validate                      # Validate schema files
ucmt diff                          # Show diff vs empty schema (offline)
ucmt diff --online                 # Show diff vs actual database state
ucmt generate "description"        # Generate migration from schema diff (offline)
ucmt generate "desc" --online      # Generate migration comparing against actual DB
ucmt pull                          # Pull current DB schema to YAML
ucmt status                        # Show applied migrations
ucmt plan                          # Show pending migrations
ucmt run                           # Run pending migrations
ucmt run --dry-run                 # Preview migrations without executing
```

### Offline vs Online Mode

- **Offline mode (default)**: Compares declared YAML schema against an empty schema. Useful for generating initial migrations or when DB access is not available.
- **Online mode (`--online`)**: Compares declared YAML schema against the actual database state using introspection. Requires Databricks connection.

## Supported Schema Features

### Column Properties

| Property | YAML Key | Description |
|----------|----------|-------------|
| Name | `name` | Column identifier (required) |
| Type | `type` | Data type: STRING, BIGINT, DECIMAL(10,2), etc. (required) |
| Nullable | `nullable` | Allow NULL values (default: true) |
| Default | `default` | Default value expression |
| Generated | `generated` | Identity column: `ALWAYS AS IDENTITY` |
| Check | `check` | Column-level check constraint expression |
| Foreign Key | `foreign_key` | Reference to another table: `{table: users, column: id}` |
| Comment | `comment` | Column documentation |

### Table Properties

| Property | YAML Key | Description |
|----------|----------|-------------|
| Name | `table` | Table identifier (required) |
| Columns | `columns` | List of column definitions (required) |
| Primary Key | `primary_key` | `{columns: [id], rely: true}` |
| Check Constraints | `check_constraints` | `[{name: chk_positive, expression: "amount > 0"}]` |
| Liquid Clustering | `liquid_clustering` | Columns for adaptive clustering: `[status, created_at]` |
| Partitioning | `partitioned_by` | Partition columns (prefer liquid clustering) |
| Table Properties | `table_properties` | Delta properties as key-value pairs |
| Comment | `comment` | Table documentation |

### Example Schema

```yaml
table: orders
comment: Customer orders with status tracking

columns:
  - name: id
    type: BIGINT
    generated: ALWAYS AS IDENTITY
    nullable: false
  - name: customer_id
    type: BIGINT
    nullable: false
    foreign_key:
      table: customers
      column: id
  - name: amount
    type: DECIMAL(10,2)
    nullable: false
    check: "amount > 0"
  - name: status
    type: STRING
    default: "'pending'"
  - name: created_at
    type: TIMESTAMP
    nullable: false

primary_key:
  columns: [id]
  rely: true

check_constraints:
  - name: valid_status
    expression: "status IN ('pending', 'confirmed', 'shipped', 'delivered')"

liquid_clustering: [status, created_at]

table_properties:
  delta.enableChangeDataFeed: "true"
  delta.columnMapping.mode: "name"
```

### Unity Catalog Constraints

| Feature | Support | Notes |
|---------|---------|-------|
| PRIMARY KEY | ✅ Informational | Not enforced — optimizer hints only |
| FOREIGN KEY | ✅ Informational | Not enforced — no referential integrity |
| CHECK constraints | ✅ Enforced | Transactions fail on violation |
| NOT NULL | ✅ Enforced | |
| Traditional indexes | ❌ | Use liquid clustering instead |
| UNIQUE constraints | ❌ | Enforce at application level |
| Liquid clustering | ✅ | Replaces partitioning + Z-ORDER |
| Change partitioning | ❌ | Requires table recreation |
| DROP COLUMN | ✅ | Requires column mapping mode |
| Type widening | ✅ Limited | INT→BIGINT, FLOAT→DOUBLE, etc. |
| Type narrowing | ❌ | Not supported |

## Key Design Decisions

1. **No indexes** — Use `liquid_clustering` for query optimization
2. **PK/FK are hints** — Add `rely: true` for optimizer usage
3. **Column mapping required** — Enable on all tables for DROP/RENAME support
4. **Partitioning is immutable** — Prefer liquid clustering
5. **Type changes are limited** — Only widening conversions allowed
