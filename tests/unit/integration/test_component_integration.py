"""
Test component integration.

Tests that core components integrate properly without external dependencies.
"""

from unittest.mock import Mock

import pytest

from snowflake_semantic_tools.core.models.dbt_model import DbtColumn, DbtModel
from snowflake_semantic_tools.core.models.schemas import SemanticTableSchemas
from snowflake_semantic_tools.core.models.semantic_model import Filter, Metric, Relationship, SemanticMetadataCollection
from snowflake_semantic_tools.core.models.validation import ValidationError, ValidationResult


class TestComponentIntegration:
    """Test integration between core components."""

    def test_validation_result_with_semantic_models(self):
        """Test ValidationResult works with semantic model components."""
        # Create realistic semantic model
        metric = Metric(
            name="total_revenue",
            expression="SUM(orders.amount)",
            tables=["orders"],
            description="Total revenue from all orders",
        )

        relationship = Relationship(
            name="orders_to_users",
            left_table="orders",
            right_table="users",
            join_type="left_outer",
            relationship_type="many_to_one",
            relationship_columns=[{"left_column": "user_id", "right_column": "id"}],
        )

        model = SemanticMetadataCollection(metrics=[metric], relationships=[relationship])

        # Test validation result integration
        result = ValidationResult()

        # Should be able to validate semantic models
        assert result.is_valid  # Empty result is valid
        assert result.error_count == 0

        # Add validation findings
        result.add_error("Test error", file_path="metrics.yml", line_number=10)
        result.add_warning("Test warning", file_path="relationships.yml")

        assert not result.is_valid  # Now has errors
        assert result.error_count == 1
        assert result.warning_count == 1

    def test_dbt_model_with_semantic_model_integration(self):
        """Test dbt models integrate with semantic models."""
        # Create dbt model
        dbt_model = DbtModel(
            name="orders",
            database="analytics",
            schema="public",
            description="Customer orders",
            columns=[
                DbtColumn(name="id", data_type="BIGINT"),
                DbtColumn(name="user_id", data_type="BIGINT"),
                DbtColumn(name="amount", data_type="DECIMAL"),
                DbtColumn(name="status", data_type="VARCHAR"),
            ],
            meta={"sst": {"cortex_searchable": True, "primary_key": "id"}},
        )

        # Create semantic model that references the dbt model
        metric = Metric(name="revenue", expression="SUM(orders.amount)", tables=["orders"])  # References the dbt model

        semantic_model = SemanticMetadataCollection(metrics=[metric])

        # Test integration
        assert dbt_model.name == "orders"
        assert dbt_model.has_sst_metadata()
        assert dbt_model.get_column("amount") is not None

        assert semantic_model.metrics[0].name == "revenue"
        assert "orders" in semantic_model.metrics[0].tables

    def test_schema_definitions_with_data_models(self):
        """Test schema definitions work with actual data models."""
        # Get schema definitions
        table_schema = SemanticTableSchemas.get_table_schema()
        metric_schema = SemanticTableSchemas.get_metric_schema()
        relationship_schema = SemanticTableSchemas.get_relationship_schema()

        # Should define expected tables
        assert table_schema.name == "sm_tables"
        assert metric_schema.name == "sm_metrics"
        assert relationship_schema.name == "sm_relationships"

        # Schemas should have required columns
        table_columns = [col.name for col in table_schema.columns]
        metric_columns = [col.name for col in metric_schema.columns]

        assert "table_name" in table_columns
        assert "cortex_searchable" in table_columns
        assert "name" in metric_columns
        assert "expr" in metric_columns or "expression" in metric_columns

    def test_validation_error_types_integration(self):
        """Test different validation error types work together."""
        result = ValidationResult()

        # Add different types of validation findings
        result.add_error(
            "Table 'orders' not found in dbt catalog",
            file_path="metrics/revenue.yml",
            line_number=5,
            context={"table": "orders", "type": "missing_table"},
        )

        result.add_warning(
            "Metric missing description",
            file_path="metrics/revenue.yml",
            line_number=2,
            context={"metric": "revenue", "type": "missing_description"},
        )

        result.add_info(
            "Consider adding synonyms for better AI understanding",
            file_path="metrics/revenue.yml",
            context={"suggestion": "synonyms"},
        )

        # Test result aggregation
        assert result.error_count == 1
        assert result.warning_count == 1
        assert result.info_count == 1
        assert not result.is_valid  # Errors make it invalid

        # Test error access
        errors = result.get_errors()
        warnings = result.get_warnings()
        info = result.get_info()

        assert len(errors) == 1
        assert len(warnings) == 1
        assert len(info) == 1

        # Test error details
        error = errors[0]
        assert "orders" in error.message
        assert error.file_path == "metrics/revenue.yml"
        assert error.line_number == 5
        assert error.context["table"] == "orders"

    def test_semantic_model_component_relationships(self):
        """Test relationships between semantic model components."""
        # Create components that reference each other
        base_metric = Metric(name="order_count", expression="COUNT(orders.id)", tables=["orders"])

        composed_metric = Metric(
            name="average_order_value",
            expression="SUM(orders.amount) / COUNT(orders.id)",  # Could reference base_metric
            tables=["orders"],
        )

        filter_obj = Filter(name="completed_orders", table_name="orders", expression="orders.status = 'completed'")

        relationship = Relationship(
            name="orders_users",
            left_table="orders",
            right_table="users",
            join_type="left_outer",
            relationship_type="many_to_one",
            relationship_columns=[{"left_column": "user_id", "right_column": "id"}],
        )

        # Combine into semantic model
        model = SemanticMetadataCollection(
            metrics=[base_metric, composed_metric], relationships=[relationship], filters=[filter_obj]
        )

        # Test component access
        assert len(model.metrics) == 2
        assert len(model.relationships) == 1
        assert len(model.filters) == 1

        # Test component properties
        assert model.metrics[0].name == "order_count"
        assert model.metrics[1].name == "average_order_value"
        assert model.relationships[0].name == "orders_users"
        assert model.filters[0].name == "completed_orders"

    def test_data_model_serialization_integration(self):
        """Test that data models can be serialized consistently."""
        # Create various model components
        metric = Metric(name="revenue", expression="SUM(amount)", tables=["orders"], synonyms=["sales", "income"])

        dbt_model = DbtModel(
            name="orders",
            database="analytics",
            schema="public",
            columns=[DbtColumn(name="amount", data_type="DECIMAL")],
        )

        validation_result = ValidationResult()
        validation_result.add_warning("Test warning")

        # Test serialization
        metric_dict = metric.to_dict()
        dbt_dict = dbt_model.to_dict()
        validation_dict = validation_result.to_dict()

        # All should serialize successfully
        assert isinstance(metric_dict, dict)
        assert isinstance(dbt_dict, dict)
        assert isinstance(validation_dict, dict)

        # Should contain expected keys
        assert "name" in metric_dict
        assert "expression" in metric_dict or "expr" in metric_dict
        assert "name" in dbt_dict
        assert "database" in dbt_dict
        assert "is_valid" in validation_dict
        assert "issues" in validation_dict


class TestRealWorldIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_complete_semantic_model_definition(self):
        """Test complete semantic model with all component types."""
        # Create a realistic e-commerce semantic model

        # Metrics
        revenue_metric = Metric(
            name="total_revenue",
            expression="SUM(orders.amount)",
            tables=["orders"],
            description="Total revenue from completed orders",
            synonyms=["sales", "income"],
        )

        user_metric = Metric(
            name="active_users",
            expression="COUNT(DISTINCT users.id)",
            tables=["users"],
            description="Count of active user accounts",
        )

        # Relationships
        orders_users = Relationship(
            name="orders_to_users",
            left_table="orders",
            right_table="users",
            join_type="left_outer",
            relationship_type="many_to_one",
            relationship_columns=[{"left_column": "user_id", "right_column": "id"}],
        )

        # Filters
        active_filter = Filter(name="active_users_only", table_name="users", expression="users.status = 'active'")

        completed_filter = Filter(
            name="completed_orders_only", table_name="orders", expression="orders.status = 'completed'"
        )

        # Combine into complete model
        complete_model = SemanticMetadataCollection(
            metrics=[revenue_metric, user_metric],
            relationships=[orders_users],
            filters=[active_filter, completed_filter],
        )

        # Test complete model
        assert len(complete_model.metrics) == 2
        assert len(complete_model.relationships) == 1
        assert len(complete_model.filters) == 2

        # Test component access
        revenue = next(m for m in complete_model.metrics if m.name == "total_revenue")
        assert revenue.expression == "SUM(orders.amount)"
        assert "sales" in revenue.synonyms

        relationship = complete_model.relationships[0]
        assert relationship.relationship_type == "many_to_one"
        assert len(relationship.relationship_columns) == 1

    def test_validation_workflow_integration(self):
        """Test validation workflow with realistic components."""
        # Create model with potential validation issues
        problematic_metric = Metric(
            name="revenue",
            expression="SUM(nonexistent_table.amount)",  # References non-existent table
            tables=["nonexistent_table"],
        )

        valid_metric = Metric(
            name="user_count", expression="COUNT(users.id)", tables=["users"], description="Count of users"
        )

        model = SemanticMetadataCollection(metrics=[problematic_metric, valid_metric])

        # Test validation result creation
        result = ValidationResult()

        # Simulate validation findings
        result.add_error(
            "Table 'nonexistent_table' not found",
            file_path="metrics.yml",
            context={"table": "nonexistent_table", "metric": "revenue"},
        )

        result.add_info(
            "Metric 'user_count' is well-defined", file_path="metrics.yml", context={"metric": "user_count"}
        )

        # Test result analysis
        assert not result.is_valid
        assert result.error_count == 1
        assert result.info_count == 1

        errors = result.get_errors()
        assert "nonexistent_table" in errors[0].message
        assert errors[0].context["metric"] == "revenue"
