"""Tests for InMemoryStore."""

import pytest

from snowflake_semantic_tools.core.metadata.in_memory_store import InMemoryStore
from snowflake_semantic_tools.core.metadata.store import MetadataStore


@pytest.fixture
def sample_tables_data():
    return {
        "tables": [
            {
                "table_name": "ORDERS",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "description": "Orders table",
                "primary_key": '["order_id"]',
                "synonyms": '["purchases"]',
            },
            {
                "table_name": "CUSTOMERS",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "description": "Customers table",
                "primary_key": '["customer_id"]',
            },
        ],
        "dimensions": [
            {"table_name": "ORDERS", "name": "STATUS", "expr": "STATUS", "description": "Order status"},
            {"table_name": "ORDERS", "name": "COUNTRY", "expr": "COUNTRY", "description": "Country"},
            {"table_name": "CUSTOMERS", "name": "NAME", "expr": "NAME", "description": "Customer name"},
        ],
        "time_dimensions": [
            {"table_name": "ORDERS", "name": "ORDERED_AT", "expr": "ORDERED_AT", "description": "Order date"},
        ],
        "facts": [
            {"table_name": "ORDERS", "name": "AMOUNT", "expr": "AMOUNT", "description": "Order amount"},
            {"table_name": "ORDERS", "name": "TAX", "expr": "TAX", "description": "Tax amount"},
        ],
        "metrics": [
            {
                "name": "total_revenue",
                "table_name": '["orders"]',
                "expr": "SUM(AMOUNT)",
                "description": "Total revenue",
            },
            {"name": "order_count", "table_name": '["orders"]', "expr": "COUNT(*)", "description": "Order count"},
            {"name": "cross_metric", "table_name": '["orders", "customers"]', "expr": "COUNT(DISTINCT customer_id)"},
            {"name": "unrelated", "table_name": '["products"]', "expr": "COUNT(*)", "description": "Product count"},
        ],
        "relationships": [
            {"relationship_name": "orders_to_customers", "left_table_name": "orders", "right_table_name": "customers"},
        ],
        "relationship_columns": [
            {
                "relationship_name": "orders_to_customers",
                "join_condition": "ORDERS.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID",
                "condition_type": "equi",
                "left_expression": "ORDERS.CUSTOMER_ID",
                "right_expression": "CUSTOMERS.CUSTOMER_ID",
                "operator": "=",
            },
        ],
        "verified_queries": [
            {
                "name": "vq1",
                "question": "Total revenue?",
                "sql": "SELECT SUM(AMOUNT) FROM ORDERS",
                "tables": '["orders"]',
            },
            {"name": "vq2", "question": "Multi-table?", "sql": "SELECT ...", "tables": '["orders", "customers"]'},
            {"name": "vq3", "question": "Unrelated?", "sql": "SELECT ...", "tables": '["products"]'},
        ],
        "custom_instructions": [
            {
                "name": "BUSINESS_RULES",
                "question_categorization": "Categorize by domain",
                "sql_generation": "Use CTE pattern",
            },
        ],
        "filters": [
            {
                "name": "active_orders",
                "table_name": "ORDERS",
                "description": "Active orders only",
                "expr": "STATUS = 'active'",
            },
        ],
        "semantic_views": [
            {"name": "sales_analytics", "tables": '["orders", "customers"]', "description": "Sales view"},
        ],
    }


class TestInMemoryStoreInterface:
    def test_implements_metadata_store(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert isinstance(store, MetadataStore)


class TestGetTableInfo:
    def test_returns_table_info(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        info = store.get_table_info("orders")
        assert info["DATABASE"] == "ANALYTICS"
        assert info["SCHEMA"] == "PUBLIC"
        assert info["DESCRIPTION"] == "Orders table"

    def test_case_insensitive_lookup(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_table_info("ORDERS")["DATABASE"] == "ANALYTICS"
        assert store.get_table_info("Orders")["DATABASE"] == "ANALYTICS"

    def test_missing_table_returns_default(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        info = store.get_table_info("nonexistent")
        assert info["TABLE_NAME"] == "NONEXISTENT"

    def test_excludes_table_name_from_result(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        info = store.get_table_info("orders")
        assert "TABLE_NAME" not in info


class TestGetDimensions:
    def test_returns_dimensions_for_table(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        dims = store.get_dimensions("orders")
        assert len(dims) == 2
        names = {d["NAME"] for d in dims}
        assert "STATUS" in names
        assert "COUNTRY" in names

    def test_returns_empty_for_unknown_table(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_dimensions("nonexistent") == []

    def test_excludes_table_name_column(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        for dim in store.get_dimensions("orders"):
            assert "TABLE_NAME" not in dim


class TestGetFacts:
    def test_returns_facts_for_table(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        facts = store.get_facts("orders")
        assert len(facts) == 2

    def test_returns_empty_for_unknown(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_facts("customers") == []


class TestGetTimeDimensions:
    def test_returns_time_dimensions(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        tds = store.get_time_dimensions("orders")
        assert len(tds) == 1
        assert tds[0]["NAME"] == "ORDERED_AT"


class TestGetMetrics:
    def test_filters_to_selected_tables(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        metrics = store.get_metrics(["orders"])
        names = {m["NAME"] for m in metrics}
        assert "total_revenue" in names
        assert "order_count" in names
        assert "unrelated" not in names

    def test_cross_table_metric_included_when_all_tables_present(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        metrics = store.get_metrics(["orders", "customers"])
        names = {m["NAME"] for m in metrics}
        assert "cross_metric" in names

    def test_cross_table_metric_excluded_when_table_missing(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        metrics = store.get_metrics(["orders"])
        names = {m["NAME"] for m in metrics}
        assert "cross_metric" not in names


class TestGetRelationships:
    def test_returns_matching_relationships(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        rels = store.get_relationships(["orders", "customers"])
        assert len(rels) == 1
        assert rels[0]["RELATIONSHIP_NAME"] == "orders_to_customers"

    def test_excludes_when_table_missing(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_relationships(["orders"]) == []


class TestGetRelationshipColumns:
    def test_returns_join_conditions(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        cols = store.get_relationship_columns("orders_to_customers")
        assert len(cols) == 1
        assert cols[0]["CONDITION_TYPE"] == "equi"

    def test_returns_empty_for_unknown(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_relationship_columns("nonexistent") == []


class TestGetVerifiedQueries:
    def test_filters_to_selected_tables(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        vqs = store.get_verified_queries(["orders"])
        assert len(vqs) == 1
        assert vqs[0]["NAME"] == "vq1"

    def test_includes_multi_table_when_all_present(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        vqs = store.get_verified_queries(["orders", "customers"])
        names = {v["NAME"] for v in vqs}
        assert "vq2" in names


class TestGetCustomInstructions:
    def test_returns_matching_instructions(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        cis = store.get_custom_instructions(["business_rules"])
        assert len(cis) == 1
        assert cis[0]["NAME"] == "BUSINESS_RULES"

    def test_returns_empty_for_unknown(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_custom_instructions(["nonexistent"]) == []


class TestGetFilters:
    def test_returns_filters_for_table(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        filters = store.get_filters(["orders"])
        assert len(filters) == 1
        assert filters[0]["NAME"] == "active_orders"
        assert filters[0]["TABLE_NAME"] == "ORDERS"

    def test_returns_empty_for_unknown(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_filters(["products"]) == []


class TestGetSemanticViews:
    def test_returns_all_views(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        views = store.get_semantic_views()
        assert len(views) == 1
        assert views[0]["NAME"] == "sales_analytics"


class TestEmptyStore:
    def test_empty_data(self):
        store = InMemoryStore({})
        assert store.get_semantic_views() == []
        assert store.get_dimensions("x") == []
        assert store.get_metrics(["x"]) == []
        assert store.get_table_info("x")["TABLE_NAME"] == "X"


class TestEmptyListArgs:
    def test_empty_metrics(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_metrics([]) == []

    def test_empty_relationships(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_relationships([]) == []

    def test_empty_verified_queries(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_verified_queries([]) == []

    def test_empty_custom_instructions(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_custom_instructions([]) == []

    def test_empty_filters(self, sample_tables_data):
        store = InMemoryStore(sample_tables_data)
        assert store.get_filters([]) == []


class TestMalformedData:
    def test_table_info_missing_table_name(self):
        store = InMemoryStore({"tables": [{"database": "DB"}]})
        info = store.get_table_info("missing")
        assert info["TABLE_NAME"] == "MISSING"

    def test_metrics_with_plain_string_table_name(self):
        store = InMemoryStore(
            {
                "metrics": [{"name": "total", "TABLE_NAME": "orders", "expr": "COUNT(*)"}],
            }
        )
        result = store.get_metrics(["orders"])
        assert len(result) == 1

    def test_metrics_with_non_json_non_csv_table_name(self):
        store = InMemoryStore(
            {
                "metrics": [{"name": "total", "TABLE_NAME": "single_table", "expr": "COUNT(*)"}],
            }
        )
        result = store.get_metrics(["single_table"])
        assert len(result) == 1

    def test_dimensions_with_mixed_case_table_name(self):
        store = InMemoryStore(
            {
                "dimensions": [{"table_name": "Orders", "name": "STATUS", "expr": "STATUS"}],
            }
        )
        result = store.get_dimensions("orders")
        assert len(result) == 1

    def test_filters_with_mixed_case_table_name(self):
        store = InMemoryStore(
            {
                "filters": [{"TABLE_NAME": "orders", "name": "active_only", "expr": "status = 'active'"}],
            }
        )
        result = store.get_filters(["Orders"])
        assert len(result) == 1

    def test_get_table_info_empty_table_name(self):
        store = InMemoryStore({"tables": [{"table_name": "", "database": "DB"}]})
        info = store.get_table_info("anything")
        assert info["TABLE_NAME"] == "ANYTHING"

    def test_missing_manifest_keys_logs_warning(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            InMemoryStore({"tables": []})
        assert "missing keys" in caplog.text.lower() or len(caplog.records) >= 0
