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
db-migrate generate "description"  # Generate migration from schema diff
db-migrate diff                    # Show diff without creating file
db-migrate run                     # Run pending migrations
db-migrate validate                # Validate schema files
db-migrate pull                    # Pull current DB schema to YAML
db-migrate status                  # Show applied migrations
```

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
