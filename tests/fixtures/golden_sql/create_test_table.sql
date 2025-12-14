-- Migration: Auto-generated
-- Description: Create test_table
-- Generated: 2025-01-01T00:00:00.000000

-- Variable substitution: ${catalog}, ${schema}

-- create_table: test_table
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.test_table (
    id BIGINT NOT NULL,
    name STRING,
    CONSTRAINT pk_test_table PRIMARY KEY (id) RELY
) USING DELTA;
