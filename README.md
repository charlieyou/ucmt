# UCMT: Unity Catalog Migration Tool

> ⚠️ **Work in Progress** — This project is under active development and not yet ready for production use.

A SQL migration system for Databricks that integrates with Databricks Asset Bundles (DABs). Uses **declarative YAML schema files** as the source of truth.

## Core Concepts

- **Auto-generate migrations** by diffing declared schema vs. current database state
- **Validate migrations** to ensure they produce the declared schema
- **Execute migrations** with proper state tracking

Follows patterns from Prisma, Atlas, and Alembic autogenerate.

## Databricks/Unity Catalog Constraints

| Feature             | Support          | Notes                                       |
| ------------------- | ---------------- | ------------------------------------------- |
| PRIMARY KEY         | ✅ Informational | **Not enforced** — optimizer hints only     |
| FOREIGN KEY         | ✅ Informational | **Not enforced** — no referential integrity |
| CHECK constraints   | ✅ Enforced      | Transactions fail on violation              |
| NOT NULL            | ✅ Enforced      |                                             |
| Traditional indexes | ❌               | Use liquid clustering instead               |
| UNIQUE constraints  | ❌               | Enforce at application level                |
| Liquid clustering   | ✅               | Replaces partitioning + Z-ORDER             |
| Change partitioning | ❌               | Requires table recreation                   |
| DROP COLUMN         | ✅               | Requires column mapping mode                |
| Type widening       | ✅ Limited       | INT→BIGINT, FLOAT→DOUBLE, etc.              |
| Type narrowing      | ❌               | Not supported                               |

## Planned CLI Commands

```bash
ucmt generate "description"        # Generate migration from schema diff (offline)
ucmt generate "desc" --online      # Generate migration comparing against actual DB
ucmt diff                          # Show diff vs empty schema (offline)
ucmt diff --online                 # Show diff vs actual database state
ucmt run                           # Run pending migrations
ucmt validate                      # Validate schema files
ucmt pull                          # Pull current DB schema to YAML
ucmt status                        # Show applied migrations
```

### Offline vs Online Mode

- **Offline mode (default)**: Compares declared YAML schema against an empty schema. Useful for generating initial migrations or when DB access is not available.
- **Online mode (`--online`)**: Compares declared YAML schema against the actual database state using introspection. Requires `DATABRICKS_*` environment variables to be set.

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
