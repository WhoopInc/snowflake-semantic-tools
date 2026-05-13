"""Tests for SST-V092: metric references undeclared column and generation-time warnings."""

import re
from unittest.mock import MagicMock

import pytest

from snowflake_semantic_tools.core.validation.rules.references import ReferenceValidator


class TestV092UndeclaredColumnValidation:
    """Test V092: metric with using_relationships references column not declared as fact/dimension."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_v092_fires_when_column_undeclared(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "TAX_WEIGHTED_REV",
                        "tables": ["ORDERS", "LOCATIONS"],
                        "expr": "SUM(ORDERS.ORDER_TOTAL * LOCATIONS.TAX_RATE)",
                        "using_relationships": ["orders_to_locations"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {
                "items": [
                    {"name": "orders_to_locations"},
                ]
            },
        }
        dbt_catalog = {
            "orders": {"columns": {"order_total": {}}},
            "locations": {"columns": {}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 1
        assert "LOCATIONS.TAX_RATE" in warnings[0].message or "locations.tax_rate" in warnings[0].message.lower()

    def test_v092_does_not_fire_when_column_declared(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "TAX_WEIGHTED_REV",
                        "tables": ["ORDERS", "LOCATIONS"],
                        "expr": "SUM(ORDERS.ORDER_TOTAL * LOCATIONS.TAX_RATE)",
                        "using_relationships": ["orders_to_locations"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {
                "items": [
                    {"name": "orders_to_locations"},
                ]
            },
        }
        dbt_catalog = {
            "orders": {"columns": {"order_total": {}}},
            "locations": {"columns": {"tax_rate": {}}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0

    def test_v092_does_not_fire_without_using_relationships(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "SIMPLE_METRIC",
                        "tables": ["ORDERS"],
                        "expr": "SUM(ORDERS.ORDER_TOTAL)",
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
        }
        dbt_catalog = {
            "orders": {"columns": {"order_total": {}}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0

    def test_v092_fires_for_multiple_undeclared_columns(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "COMPLEX_METRIC",
                        "tables": ["ORDERS", "LOCATIONS"],
                        "expr": "SUM(ORDERS.AMOUNT * LOCATIONS.TAX_RATE + LOCATIONS.SURCHARGE)",
                        "using_relationships": ["orders_to_locations"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {
                "items": [
                    {"name": "orders_to_locations"},
                ]
            },
        }
        dbt_catalog = {
            "orders": {"columns": {"amount": {}}},
            "locations": {"columns": {}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 2

    def test_v092_suggestion_is_actionable(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "MY_METRIC",
                        "tables": ["ORDERS", "STORES"],
                        "expr": "SUM(ORDERS.TOTAL * STORES.RATE)",
                        "using_relationships": ["orders_to_stores"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {
                "items": [
                    {"name": "orders_to_stores"},
                ]
            },
        }
        dbt_catalog = {
            "orders": {"columns": {"total": {}}},
            "stores": {"columns": {}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 1
        assert "column_type: fact" in warnings[0].suggestion

    def test_v092_skips_table_not_in_catalog(self, validator):
        """V092 silently skips references to tables not in the catalog."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "MY_METRIC",
                        "tables": ["ORDERS", "UNKNOWN"],
                        "expr": "SUM(ORDERS.TOTAL * UNKNOWN.VALUE)",
                        "using_relationships": ["orders_to_unknown"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {"items": [{"name": "orders_to_unknown"}]},
        }
        dbt_catalog = {"orders": {"columns": {"total": {}}}}
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0

    def test_v092_does_not_fire_with_empty_using_relationships(self, validator):
        """V092 does not fire when using_relationships is an empty list."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "MY_METRIC",
                        "tables": ["ORDERS"],
                        "expr": "SUM(ORDERS.TOTAL)",
                        "using_relationships": [],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
        }
        dbt_catalog = {"orders": {"columns": {"total": {}}}}
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0

    def test_v092_case_insensitive(self, validator):
        """V092 matches case-insensitively (ORDERS.Col matches orders/col)."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "MY_METRIC",
                        "tables": ["Orders", "Locations"],
                        "expr": "SUM(Orders.Order_Total * Locations.Tax_Rate)",
                        "using_relationships": ["orders_to_locations"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {"items": [{"name": "orders_to_locations"}]},
        }
        dbt_catalog = {
            "orders": {"columns": {"order_total": {}}},
            "locations": {"columns": {"tax_rate": {}}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0

    def test_v092_does_not_fire_for_derived_metrics(self, validator):
        """V092 does not fire on derived metrics (V046 handles those)."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "DERIVED_METRIC",
                        "tables": ["ORDERS", "LOCATIONS"],
                        "expr": "{{ metric('total_orders') }} / {{ metric('total_locations') }}",
                        "using_relationships": ["orders_to_locations"],
                        "source_file": "test.yml",
                        "derived": True,
                    }
                ]
            },
            "relationships": {"items": [{"name": "orders_to_locations"}]},
        }
        dbt_catalog = {
            "orders": {"columns": {}},
            "locations": {"columns": {}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0

    def test_v092_filters_sql_keywords(self, validator):
        """V092 does not fire on SQL keywords like CAST, EXTRACT."""
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "MY_METRIC",
                        "tables": ["ORDERS", "LOCATIONS"],
                        "expr": "SUM(CAST(ORDERS.AMOUNT AS FLOAT) * LOCATIONS.RATE)",
                        "using_relationships": ["orders_to_locations"],
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            },
            "relationships": {"items": [{"name": "orders_to_locations"}]},
        }
        dbt_catalog = {
            "orders": {"columns": {"amount": {}}},
            "locations": {"columns": {"rate": {}}},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        warnings = [w for w in result.get_warnings() if w.rule_id == "SST-V092"]
        assert len(warnings) == 0


class TestExtractColumnReferences:
    """Test the _extract_column_references_from_expression helper."""

    @pytest.fixture
    def builder(self):
        from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder

        return SemanticViewBuilder.__new__(SemanticViewBuilder)

    def test_extracts_simple_references(self, builder):
        refs = builder._extract_column_references_from_expression("SUM(ORDERS.AMOUNT)")
        assert ("ORDERS", "AMOUNT") in refs

    def test_extracts_multiple_references(self, builder):
        refs = builder._extract_column_references_from_expression("SUM(ORDERS.AMOUNT * LOCATIONS.TAX_RATE)")
        assert ("ORDERS", "AMOUNT") in refs
        assert ("LOCATIONS", "TAX_RATE") in refs

    def test_filters_sql_keywords(self, builder):
        refs = builder._extract_column_references_from_expression("CAST(ORDERS.AMOUNT AS FLOAT)")
        table_refs = [t for t, c in refs]
        assert "CAST" not in table_refs
        assert "ORDERS" in table_refs

    def test_handles_empty_expression(self, builder):
        refs = builder._extract_column_references_from_expression("")
        assert refs == []

    def test_handles_no_dot_references(self, builder):
        refs = builder._extract_column_references_from_expression("SUM(AMOUNT)")
        assert refs == []
