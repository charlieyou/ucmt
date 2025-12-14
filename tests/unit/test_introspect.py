"""TDD tests for SchemaIntrospector - written FIRST before implementation."""

from unittest.mock import MagicMock

from ucmt.schema.introspect import SchemaIntrospector


class DictRow:
    """Row with .get() method like a dict."""

    def __init__(self, data: dict):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


class PySparkRow:
    """Row with .asDict() method like PySpark Row (no .get())."""

    def __init__(self, data: dict):
        self._data = data

    def asDict(self):
        return self._data

    def __getitem__(self, key):
        return self._data[key]


class GetItemOnlyRow:
    """Row with only __getitem__ (no .get() or .asDict())."""

    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]


class FakeRow:
    """Mock row from DatabricksClient.fetchall()."""

    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def asDict(self):
        return self._data


def make_mock_client(
    tables_data: list[dict] | None = None,
    columns_data: list[dict] | None = None,
    pk_constraints_data: list[dict] | None = None,
    check_constraints_data: list[dict] | None = None,
    table_properties_data: list[dict] | None = None,
) -> MagicMock:
    """Create a mock DatabricksClient with test data."""
    client = MagicMock()

    def fetchall_side_effect(sql: str):
        sql_lower = sql.lower()
        if "information_schema.tables" in sql_lower:
            return [FakeRow(d) for d in (tables_data or [])]
        elif "information_schema.columns" in sql_lower:
            return [FakeRow(d) for d in (columns_data or [])]
        elif "constraint_column_usage" in sql_lower:
            return [FakeRow(d) for d in (pk_constraints_data or [])]
        elif "table_constraints" in sql_lower and "check" in sql_lower:
            return [FakeRow(d) for d in (check_constraints_data or [])]
        elif "tblproperties" in sql_lower:
            return [FakeRow(d) for d in (table_properties_data or [])]
        return []

    client.fetchall.side_effect = fetchall_side_effect
    return client


class TestIntrospectUsersTable:
    """Test introspecting a users table matches YAML model structure."""

    def test_introspect_users_table_matches_yaml_model(self):
        """Introspected table should produce Table model matching YAML fixture."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": "User accounts table",
                }
            ],
            columns_data=[
                {
                    "column_name": "id",
                    "data_type": "BIGINT",
                    "is_nullable": "NO",
                    "column_default": None,
                    "comment": None,
                },
                {
                    "column_name": "email",
                    "data_type": "STRING",
                    "is_nullable": "NO",
                    "column_default": None,
                    "comment": None,
                },
                {
                    "column_name": "name",
                    "data_type": "STRING",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                },
                {
                    "column_name": "created_at",
                    "data_type": "TIMESTAMP",
                    "is_nullable": "NO",
                    "column_default": "CURRENT_TIMESTAMP()",
                    "comment": None,
                },
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("users")

        assert table is not None
        assert table.name == "users"
        assert table.comment == "User accounts table"
        assert len(table.columns) == 4

        id_col = table.get_column("id")
        assert id_col is not None
        assert id_col.type == "BIGINT"
        assert id_col.nullable is False


class TestIntrospectTableProperties:
    """Test introspecting table properties including column mapping."""

    def test_introspect_table_properties_includes_column_mapping(self):
        """Table properties should be extracted from TBLPROPERTIES."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "events",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                }
            ],
            columns_data=[
                {
                    "column_name": "id",
                    "data_type": "BIGINT",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                }
            ],
            table_properties_data=[
                {"key": "delta.columnMapping.mode", "value": "name"},
                {"key": "delta.minReaderVersion", "value": "2"},
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("events")

        assert table is not None
        assert table.table_properties.get("delta.columnMapping.mode") == "name"
        assert table.table_properties.get("delta.minReaderVersion") == "2"


class TestIntrospectPrimaryKey:
    """Test introspecting primary key constraints."""

    def test_introspect_primary_key(self):
        """Primary key should be extracted from table constraints."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "orders",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                }
            ],
            columns_data=[
                {
                    "column_name": "id",
                    "data_type": "BIGINT",
                    "is_nullable": "NO",
                    "column_default": None,
                    "comment": None,
                }
            ],
            pk_constraints_data=[
                {
                    "constraint_name": "pk_orders",
                    "constraint_type": "PRIMARY KEY",
                    "column_name": "id",
                    "rely": True,
                }
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("orders")

        assert table is not None
        assert table.primary_key is not None
        assert table.primary_key.columns == ["id"]
        assert table.primary_key.rely is True


class TestIntrospectCheckConstraints:
    """Test introspecting CHECK constraints."""

    def test_introspect_check_constraints(self):
        """CHECK constraints should be extracted from table constraints."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "products",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                }
            ],
            columns_data=[
                {
                    "column_name": "price",
                    "data_type": "DECIMAL(10,2)",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                }
            ],
            check_constraints_data=[
                {
                    "constraint_name": "positive_price",
                    "constraint_type": "CHECK",
                    "check_clause": "price > 0",
                }
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("products")

        assert table is not None
        assert len(table.check_constraints) == 1
        assert table.check_constraints[0].name == "positive_price"
        assert table.check_constraints[0].expression == "price > 0"


class TestIntrospectLiquidClustering:
    """Test introspecting liquid clustering configuration."""

    def test_introspect_liquid_clustering(self):
        """Liquid clustering columns should be extracted."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "events",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                    "clustering_columns": "event_date,user_id",
                }
            ],
            columns_data=[
                {
                    "column_name": "event_date",
                    "data_type": "DATE",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                },
                {
                    "column_name": "user_id",
                    "data_type": "BIGINT",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                },
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("events")

        assert table is not None
        assert table.liquid_clustering == ["event_date", "user_id"]


class TestIntrospectIgnoresViews:
    """Test that views are ignored during introspection."""

    def test_introspect_ignores_views(self):
        """Views should return None, not a Table."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "user_summary",
                    "table_type": "VIEW",
                    "data_source_format": None,
                    "comment": None,
                }
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("user_summary")

        assert table is None


class TestIntrospectIgnoresTempTables:
    """Test that temporary/streaming tables are ignored."""

    def test_introspect_ignores_temp_tables(self):
        """TEMPORARY tables should return None."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "temp_data",
                    "table_type": "TEMPORARY",
                    "data_source_format": "DELTA",
                    "comment": None,
                }
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("temp_data")

        assert table is None

    def test_introspect_ignores_streaming_tables(self):
        """STREAMING tables should return None."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "stream_data",
                    "table_type": "STREAMING_TABLE",
                    "data_source_format": "DELTA",
                    "comment": None,
                }
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("stream_data")

        assert table is None


class TestIntrospectMissingTable:
    """Test introspecting a table that doesn't exist."""

    def test_introspect_missing_table_returns_none(self):
        """Missing table should return None, not raise."""
        client = make_mock_client(tables_data=[])

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("nonexistent")

        assert table is None


class TestIntrospectNormalizesTypes:
    """Test that types are normalized to uppercase."""

    def test_introspect_normalizes_types_to_uppercase(self):
        """Column types from DB should be normalized to uppercase."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "test",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                }
            ],
            columns_data=[
                {
                    "column_name": "id",
                    "data_type": "bigint",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                },
                {
                    "column_name": "data",
                    "data_type": "string",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                },
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        table = introspector.introspect_table("test")

        assert table is not None
        id_col = table.get_column("id")
        data_col = table.get_column("data")
        assert id_col is not None
        assert id_col.type == "BIGINT"
        assert data_col is not None
        assert data_col.type == "STRING"


class TestIntrospectSchema:
    """Test introspecting entire schema (all tables)."""

    def test_introspect_schema_returns_all_delta_tables(self):
        """introspect_schema should return Schema with all Delta tables."""
        client = make_mock_client(
            tables_data=[
                {
                    "table_name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                },
                {
                    "table_name": "orders",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "comment": None,
                },
                {
                    "table_name": "user_view",
                    "table_type": "VIEW",
                    "data_source_format": None,
                    "comment": None,
                },
            ],
            columns_data=[
                {
                    "column_name": "id",
                    "data_type": "BIGINT",
                    "is_nullable": "YES",
                    "column_default": None,
                    "comment": None,
                },
            ],
        )

        introspector = SchemaIntrospector(client, catalog="main", schema="default")
        schema = introspector.introspect_schema()

        assert len(schema.tables) == 2
        assert "users" in schema.tables
        assert "orders" in schema.tables
        assert "user_view" not in schema.tables


class TestRowGetHelper:
    """Tests for _row_get helper method handling different row types."""

    def test_row_get_with_dict_row(self):
        """_row_get works with dict-like rows that have .get()."""
        client = MagicMock()
        introspector = SchemaIntrospector(client, catalog="main", schema="default")

        row = DictRow({"name": "alice", "age": 30})

        assert introspector._row_get(row, "name") == "alice"
        assert introspector._row_get(row, "age") == 30
        assert introspector._row_get(row, "missing") is None
        assert introspector._row_get(row, "missing", "default") == "default"

    def test_row_get_with_pyspark_row(self):
        """_row_get works with PySpark Row objects that have .asDict()."""
        client = MagicMock()
        introspector = SchemaIntrospector(client, catalog="main", schema="default")

        row = PySparkRow({"column_name": "id", "data_type": "BIGINT"})

        assert introspector._row_get(row, "column_name") == "id"
        assert introspector._row_get(row, "data_type") == "BIGINT"
        assert introspector._row_get(row, "missing") is None
        assert introspector._row_get(row, "missing", False) is False

    def test_row_get_with_getitem_only_row(self):
        """_row_get works with rows that only have __getitem__."""
        client = MagicMock()
        introspector = SchemaIntrospector(client, catalog="main", schema="default")

        row = GetItemOnlyRow({"key": "value", "count": 42})

        assert introspector._row_get(row, "key") == "value"
        assert introspector._row_get(row, "count") == 42
        assert introspector._row_get(row, "missing") is None
        assert introspector._row_get(row, "missing", "fallback") == "fallback"

    def test_row_get_with_plain_dict(self):
        """_row_get works with plain Python dicts."""
        client = MagicMock()
        introspector = SchemaIntrospector(client, catalog="main", schema="default")

        row = {"table_name": "users", "table_type": "MANAGED"}

        assert introspector._row_get(row, "table_name") == "users"
        assert introspector._row_get(row, "table_type") == "MANAGED"
        assert introspector._row_get(row, "missing") is None
        assert introspector._row_get(row, "missing", "N/A") == "N/A"
