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

## Schema Definition (YAML)

```yaml
table: users
columns:
  - name: id
    type: BIGINT
    generated: ALWAYS AS IDENTITY
    nullable: false
  - name: email
    type: STRING
    nullable: false

primary_key:
  columns: [id]
  rely: true # Enable query optimizer hints

liquid_clustering: [status, created_at]

table_properties:
  delta.enableChangeDataFeed: "true"
  delta.columnMapping.mode: "name"
```

## Key Design Decisions

1. **No indexes** — Use `liquid_clustering` for query optimization
2. **PK/FK are hints** — Add `rely: true` for optimizer usage
3. **Column mapping required** — Enable on all tables for DROP/RENAME support
4. **Partitioning is immutable** — Prefer liquid clustering
5. **Type changes are limited** — Only widening conversions allowed
