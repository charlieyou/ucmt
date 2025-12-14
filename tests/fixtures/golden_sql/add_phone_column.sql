-- Migration: Auto-generated
-- Description: Add phone column
-- Generated: 2025-01-01T00:00:00.000000

-- Variable substitution: ${catalog}, ${schema}

-- add_column: users
ALTER TABLE ${catalog}.${schema}.users ADD COLUMN IF NOT EXISTS phone STRING COMMENT 'User phone';
