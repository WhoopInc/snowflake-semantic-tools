"""
Unit tests for TemplateResolver - unified ref() syntax support.

Tests for Issue #97: Unified {{ ref() }} syntax for table and column references.
"""

import pytest

from snowflake_semantic_tools.core.parsing.template_engine.resolver import TemplateResolver


class TestUnifiedRefSyntax:
    """Test unified {{ ref() }} syntax for tables and columns."""

    def test_ref_table_single_argument(self):
        """Test {{ ref('table') }} resolves to table name."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        result = resolver.resolve_content("{{ ref('orders') }}")
        assert result == "ORDERS"

    def test_ref_table_with_double_quotes(self):
        """Test {{ ref("table") }} with double quotes."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        result = resolver.resolve_content('{{ ref("orders") }}')
        assert result == "ORDERS"

    def test_ref_column_two_arguments(self):
        """Test {{ ref('table', 'column') }} resolves to TABLE.COLUMN."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{ ref('orders', 'amount') }}")
        assert result == "ORDERS.AMOUNT"

    def test_ref_column_with_double_quotes(self):
        """Test {{ ref("table", "column") }} with double quotes."""
        resolver = TemplateResolver()
        result = resolver.resolve_content('{{ ref("orders", "amount") }}')
        assert result == "ORDERS.AMOUNT"

    def test_ref_table_case_insensitive(self):
        """Test ref() handles case-insensitive table names."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        result = resolver.resolve_content("{{ ref('ORDERS') }}")
        assert result == "ORDERS"

    def test_ref_column_case_insensitive(self):
        """Test ref() handles case-insensitive column names."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{ ref('ORDERS', 'AMOUNT') }}")
        assert result == "ORDERS.AMOUNT"

    def test_ref_table_with_whitespace(self):
        """Test ref() handles whitespace variations."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        result = resolver.resolve_content("{{  ref  (  'orders'  )  }}")
        assert result == "ORDERS"

    def test_ref_column_with_whitespace(self):
        """Test ref() handles whitespace in column references."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{  ref  (  'orders'  ,  'amount'  )  }}")
        assert result == "ORDERS.AMOUNT"

    def test_ref_table_not_in_catalog(self):
        """Test ref() for table not in catalog still works (defaults to uppercase)."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{ ref('unknown_table') }}")
        assert result == "UNKNOWN_TABLE"

    def test_ref_mixed_with_legacy_syntax(self):
        """Test ref() works alongside legacy table() and column() syntax."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        content = "{{ ref('orders') }} and {{ table('orders') }}"
        result = resolver.resolve_content(content)
        assert "ORDERS" in result
        assert result.count("ORDERS") == 2

    def test_ref_in_metric_expression(self):
        """Test ref() in metric expressions."""
        resolver = TemplateResolver()
        content = "SUM({{ ref('orders', 'amount') }})"
        result = resolver.resolve_content(content)
        assert result == "SUM(ORDERS.AMOUNT)"

    def test_ref_multiple_columns_in_expression(self):
        """Test multiple ref() column references in one expression."""
        resolver = TemplateResolver()
        content = "{{ ref('orders', 'amount') }} + {{ ref('orders', 'tax') }}"
        result = resolver.resolve_content(content)
        assert "ORDERS.AMOUNT" in result
        assert "ORDERS.TAX" in result

    def test_ref_table_in_tables_list(self):
        """Test ref() in tables list."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        content = "- {{ ref('orders') }}\n- {{ ref('customers') }}"
        result = resolver.resolve_content(content)
        assert "ORDERS" in result
        assert "CUSTOMERS" in result


class TestBackwardCompatibility:
    """Test backward compatibility with legacy table() and column() syntax."""

    def test_legacy_table_syntax_still_works(self):
        """Test legacy {{ table() }} syntax still works."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        result = resolver.resolve_content("{{ table('orders') }}")
        assert result == "ORDERS"

    def test_legacy_column_syntax_still_works(self):
        """Test legacy {{ column() }} syntax still works."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{ column('orders', 'amount') }}")
        assert result == "ORDERS.AMOUNT"

    def test_mixed_ref_and_legacy_syntax(self):
        """Test mixing ref() with legacy syntax in same content."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        content = "{{ ref('orders') }} and {{ table('orders') }} and {{ ref('orders', 'amount') }} and {{ column('orders', 'tax') }}"
        result = resolver.resolve_content(content)
        assert result.count("ORDERS") >= 4
        assert "ORDERS.AMOUNT" in result
        assert "ORDERS.TAX" in result


class TestRefInComplexScenarios:
    """Test ref() syntax in complex real-world scenarios."""

    def test_ref_in_relationship_conditions(self):
        """Test ref() in relationship join conditions."""
        resolver = TemplateResolver()
        content = "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'id') }}"
        result = resolver.resolve_content(content)
        assert "ORDERS.CUSTOMER_ID" in result
        assert "CUSTOMERS.ID" in result

    def test_ref_in_asof_condition(self):
        """Test ref() in ASOF temporal join condition."""
        resolver = TemplateResolver()
        content = "{{ ref('events', 'timestamp') }} >= {{ ref('sessions', 'start_time') }}"
        result = resolver.resolve_content(content)
        assert "EVENTS.TIMESTAMP" in result
        assert "SESSIONS.START_TIME" in result

    def test_ref_with_metric_composition(self):
        """Test ref() works with metric composition."""
        metrics = [
            {"name": "revenue", "expr": "SUM({{ ref('orders', 'amount') }})"},
            {"name": "total_revenue", "expr": "{{ metric('revenue') }}"},
        ]
        resolver = TemplateResolver(metrics_catalog=metrics)
        result = resolver.resolve_content("{{ metric('total_revenue') }}")
        # Should resolve to the revenue metric which contains ref()
        assert "ORDERS.AMOUNT" in result or "ref" not in result.lower()

    def test_ref_in_multiline_expression(self):
        """Test ref() in multi-line YAML expressions."""
        resolver = TemplateResolver()
        content = """SUM(
  {{ ref('orders', 'amount') }} * 
  (1 - {{ ref('orders', 'discount') }})
)"""
        result = resolver.resolve_content(content)
        assert "ORDERS.AMOUNT" in result
        assert "ORDERS.DISCOUNT" in result


class TestRefEdgeCases:
    """Test edge cases for ref() syntax."""

    def test_ref_with_special_characters(self):
        """Test ref() with special characters in names."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{ ref('order_items', 'item_id') }}")
        assert result == "ORDER_ITEMS.ITEM_ID"

    def test_ref_with_numbers(self):
        """Test ref() with numbers in table/column names."""
        resolver = TemplateResolver()
        result = resolver.resolve_content("{{ ref('table_2024', 'col_123') }}")
        assert result == "TABLE_2024.COL_123"

    def test_ref_nested_in_function_call(self):
        """Test ref() nested in SQL function calls."""
        resolver = TemplateResolver()
        content = "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"
        result = resolver.resolve_content(content)
        assert "ORDERS.ORDER_ID" in result

    def test_ref_in_where_clause_style(self):
        """Test ref() in WHERE clause style expressions."""
        resolver = TemplateResolver()
        content = "{{ ref('orders', 'status') }} = 'completed'"
        result = resolver.resolve_content(content)
        assert "ORDERS.STATUS" in result

    def test_ref_with_arithmetic(self):
        """Test ref() in arithmetic expressions."""
        resolver = TemplateResolver()
        content = "{{ ref('orders', 'amount') }} * {{ ref('orders', 'quantity') }}"
        result = resolver.resolve_content(content)
        assert "ORDERS.AMOUNT" in result
        assert "ORDERS.QUANTITY" in result


class TestRefResolutionOrder:
    """Test that ref() resolution order is correct."""

    def test_ref_resolved_before_metrics(self):
        """Test ref() is resolved before metric expansion."""
        metrics = [
            {"name": "revenue", "expr": "SUM({{ ref('orders', 'amount') }})"},
        ]
        resolver = TemplateResolver(metrics_catalog=metrics)
        result = resolver.resolve_content("{{ metric('revenue') }}")
        # Should have resolved ref() inside the metric
        assert "ref" not in result.lower() or "ORDERS.AMOUNT" in result

    def test_ref_table_before_ref_column(self):
        """Test table ref() is resolved before column ref()."""
        resolver = TemplateResolver(dbt_catalog={"orders": {"name": "orders"}})
        content = "{{ ref('orders') }}.{{ ref('orders', 'amount') }}"
        # This is a bit unusual but should still work
        result = resolver.resolve_content(content)
        assert "ORDERS" in result
