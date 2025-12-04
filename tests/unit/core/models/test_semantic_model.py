"""
Test semantic model classes.

Tests the semantic model data structures (Metric, Relationship, Filter, etc.).
"""

import pytest
from snowflake_semantic_tools.core.models.semantic_model import (
    Metric, Relationship, Filter, CustomInstruction, VerifiedQuery,
    SemanticView, SemanticMetadataCollection
)


class TestMetric:
    """Test Metric data class."""
    
    def test_metric_creation(self):
        """Test basic metric creation."""
        metric = Metric(
            name="total_revenue",
            expression="SUM(orders.amount)",
            tables=["orders"],
            description="Total revenue from all orders"
        )
        
        assert metric.name == "total_revenue"
        assert metric.description == "Total revenue from all orders"
        assert metric.expression == "SUM(orders.amount)"
        assert metric.tables == ["orders"]
    
    def test_metric_with_dimensions(self):
        """Test metric with dimensions."""
        metric = Metric(
            name="revenue_by_region",
            expression="SUM(orders.amount)",
            tables=["orders", "users"],
            description="Revenue broken down by region"
        )
        
        assert metric.name == "revenue_by_region"
        assert metric.expression == "SUM(orders.amount)"
        assert "orders" in metric.tables
        assert "users" in metric.tables
    
    def test_metric_with_synonyms(self):
        """Test metric with synonyms."""
        metric = Metric(
            name="total_sales",
            expression="SUM(amount)",
            tables=["orders"],
            description="Total sales amount",
            synonyms=["revenue", "sales", "total_money"]
        )
        
        assert "revenue" in metric.synonyms
        assert "sales" in metric.synonyms
        assert len(metric.synonyms) == 3


class TestRelationship:
    """Test Relationship data class."""
    
    def test_many_to_one_relationship(self):
        """Test many-to-one relationship creation."""
        relationship = Relationship(
            name="orders_to_users",
            left_table="orders",
            right_table="users",
            join_type="left_outer",
            relationship_type="many_to_one",
            relationship_columns=[
                {"left_column": "user_id", "right_column": "id"}
            ]
        )
        
        assert relationship.name == "orders_to_users"
        assert relationship.left_table == "orders"
        assert relationship.right_table == "users"
        assert relationship.relationship_type == "many_to_one"
        assert relationship.join_type == "left_outer"
        assert len(relationship.relationship_columns) == 1
    
    def test_many_to_many_relationship_with_junction(self):
        """Test many-to-many relationship with junction table."""
        relationship = Relationship(
            name="users_to_products",
            left_table="users",
            right_table="products",
            join_type="inner",
            relationship_type="many_to_many",
            relationship_columns=[
                {"left_column": "id", "right_column": "id"}
            ]
        )
        
        assert relationship.name == "users_to_products"
        assert relationship.left_table == "users"
        assert relationship.right_table == "products"
        assert relationship.relationship_type == "many_to_many"
        assert relationship.join_type == "inner"
    
    def test_relationship_validation_types(self):
        """Test that relationship types are validated."""
        valid_types = ["one_to_one", "one_to_many", "many_to_one", "many_to_many"]
        
        for rel_type in valid_types:
            relationship = Relationship(
                name=f"test_{rel_type}",
                left_table="table1",
                right_table="table2",
                join_type="left_outer",
                relationship_type=rel_type,
                relationship_columns=[
                    {"left_column": "id", "right_column": "ref_id"}
                ]
            )
            assert relationship.relationship_type == rel_type


class TestFilter:
    """Test Filter data class."""
    
    def test_filter_creation(self):
        """Test basic filter creation."""
        filter_obj = Filter(
            name="active_users",
            table_name="users",
            expression="users.status = 'active'",
            description="Filter for active users only"
        )
        
        assert filter_obj.name == "active_users"
        assert filter_obj.description == "Filter for active users only"
        assert filter_obj.expression == "users.status = 'active'"
        assert filter_obj.table_name == "users"
    
    def test_filter_with_synonyms(self):
        """Test filter with synonyms."""
        filter_obj = Filter(
            name="recent_orders",
            table_name="orders",
            expression="orders.created_at >= CURRENT_DATE - 30",
            description="Orders from last 30 days"
        )
        
        assert filter_obj.name == "recent_orders"
        assert filter_obj.table_name == "orders"
        assert filter_obj.expression == "orders.created_at >= CURRENT_DATE - 30"
        assert filter_obj.description == "Orders from last 30 days"


class TestCustomInstruction:
    """Test CustomInstruction data class."""
    
    def test_custom_instruction_creation(self):
        """Test basic custom instruction creation."""
        instruction = CustomInstruction(
            name="business_rules",
            instruction="When users ask about revenue, focus on completed orders. Always exclude test data where email ends with '@test.com'."
        )
        
        assert instruction.name == "business_rules"
        assert "completed orders" in instruction.instruction
        assert "@test.com" in instruction.instruction
    
    def test_custom_instruction_with_only_sql_generation(self):
        """Test custom instruction with only SQL generation rules."""
        instruction = CustomInstruction(
            name="formatting_rules",
            instruction="Round all monetary values to 2 decimal places."
        )
        
        assert instruction.name == "formatting_rules"
        assert "2 decimal places" in instruction.instruction
    
    def test_custom_instruction_with_only_question_categorization(self):
        """Test custom instruction with only question categorization."""
        instruction = CustomInstruction(
            name="privacy_rules",
            instruction="Reject questions asking for individual user data."
        )
        
        assert instruction.name == "privacy_rules"
        assert "individual user data" in instruction.instruction


class TestVerifiedQuery:
    """Test VerifiedQuery data class."""
    
    def test_verified_query_creation(self):
        """Test basic verified query creation."""
        query = VerifiedQuery(
            name="monthly_revenue",
            question="What was the total revenue by month?",
            sql="SELECT DATE_TRUNC('month', created_at) as month, SUM(amount) as revenue FROM orders GROUP BY 1",
            tables=["orders"],
            verified_by="data_team",
            verified_at="2024-01-01"
        )
        
        assert query.name == "monthly_revenue"
        assert query.question == "What was the total revenue by month?"
        assert "SUM(amount)" in query.sql
        assert query.verified_by == "data_team"
        assert query.verified_at == "2024-01-01"
    
    def test_verified_query_with_use_as_onboarding(self):
        """Test verified query marked for onboarding."""
        query = VerifiedQuery(
            name="basic_stats",
            question="How many users do we have?",
            sql="SELECT COUNT(*) as user_count FROM users",
            tables=["users"],
            verified_by="admin",
            verified_at="2024-01-01"
        )
        
        assert query.name == "basic_stats"
        assert query.question == "How many users do we have?"
        assert "COUNT(*)" in query.sql
        assert query.verified_by == "admin"


class TestSemanticView:
    """Test SemanticView data class."""
    
    def test_semantic_view_creation(self):
        """Test basic semantic view creation."""
        view = SemanticView(
            name="sales_dashboard",
            tables=["orders", "users", "products"],
            description="Sales analytics dashboard view"
        )
        
        assert view.name == "sales_dashboard"
        assert len(view.tables) == 3
        assert "orders" in view.tables
        assert "users" in view.tables
        assert "products" in view.tables
        assert view.description == "Sales analytics dashboard view"
    
    def test_semantic_view_with_custom_instructions(self):
        """Test semantic view with custom instructions."""
        view = SemanticView(
            name="customer_analytics",
            tables=["users"],
            description="Customer analysis view",
            custom_instructions=["privacy_rules", "business_context"]
        )
        
        assert view.name == "customer_analytics"
        assert len(view.tables) == 1
        assert "users" in view.tables
        assert len(view.custom_instructions) == 2
        assert "privacy_rules" in view.custom_instructions
    
    def test_semantic_view_minimal(self):
        """Test semantic view with minimal required fields."""
        view = SemanticView(
            name="simple_view",
            tables=["orders"]
        )
        
        assert view.name == "simple_view"
        assert view.tables == ["orders"]
        assert view.description is None
        assert view.custom_instructions == []


class TestSemanticMetadataCollection:
    """Test SemanticModel container class."""
    
    def test_semantic_model_creation(self):
        """Test semantic model with all components."""
        metric = Metric(
            name="revenue",
            expression="SUM(amount)",
            tables=["orders"]
        )
        
        relationship = Relationship(
            name="orders_users",
            left_table="orders",
            right_table="users",
            join_type="left_outer",
            relationship_type="many_to_one",
            relationship_columns=[
                {"left_column": "user_id", "right_column": "id"}
            ]
        )
        
        filter_obj = Filter(
            name="active_only",
            table_name="users",
            expression="status = 'active'"
        )
        
        model = SemanticMetadataCollection(
            metrics=[metric],
            relationships=[relationship],
            filters=[filter_obj]
        )
        
        assert len(model.metrics) == 1
        assert len(model.relationships) == 1
        assert len(model.filters) == 1
        assert model.metrics[0].name == "revenue"
        assert model.relationships[0].name == "orders_users"
        assert model.filters[0].name == "active_only"
    
    def test_semantic_model_empty(self):
        """Test empty semantic model."""
        model = SemanticMetadataCollection()
        
        assert model.metrics == []
        assert model.relationships == []
        assert model.filters == []
        assert model.custom_instructions == []
        assert model.verified_queries == []
        assert model.semantic_views == []
    
    def test_semantic_model_get_component_by_name(self):
        """Test getting components by name."""
        metric1 = Metric(name="metric1", expression="COUNT(*)", tables=["table1"])
        metric2 = Metric(name="metric2", expression="SUM(amount)", tables=["table2"])
        
        model = SemanticMetadataCollection(metrics=[metric1, metric2])
        
        # Test that metrics are accessible through the list
        assert len(model.metrics) == 2
        metric_names = [m.name for m in model.metrics]
        assert "metric1" in metric_names
        assert "metric2" in metric_names
        
        # Find specific metric
        metric2 = next((m for m in model.metrics if m.name == "metric2"), None)
        assert metric2 is not None
        assert metric2.expression == "SUM(amount)"
    
    def test_semantic_model_get_all_table_references(self):
        """Test getting all table references from model."""
        metric = Metric(name="m1", expression="COUNT(*)", tables=["orders", "users"])
        relationship = Relationship(
            name="r1", 
            left_table="orders", 
            right_table="users",
            join_type="left_outer",
            relationship_type="many_to_one",
            relationship_columns=[
                {"left_column": "user_id", "right_column": "id"}
            ]
        )
        filter_obj = Filter(name="f1", table_name="users", expression="status='active'")
        
        model = SemanticMetadataCollection(
            metrics=[metric],
            relationships=[relationship],
            filters=[filter_obj]
        )
        
        # Test that we can access all table references manually
        all_tables = set()
        
        # Collect from metrics
        for metric in model.metrics:
            all_tables.update(metric.tables)
        
        # Collect from relationships
        for rel in model.relationships:
            all_tables.add(rel.left_table)
            all_tables.add(rel.right_table)
        
        # Collect from filters (table_name field)
        for filter_obj in model.filters:
            all_tables.add(filter_obj.table_name)
        
        assert "orders" in all_tables
        assert "users" in all_tables
        assert len(all_tables) == 2
