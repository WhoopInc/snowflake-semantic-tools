import pytest

from snowflake_semantic_tools.core.diagnostics import DiagnosticRenderer
from snowflake_semantic_tools.core.validation.rules.dbt_models import DbtModelValidator
from snowflake_semantic_tools.core.validation.rules.dependencies import DependencyValidator
from snowflake_semantic_tools.core.validation.rules.duplicates import DuplicateValidator
from snowflake_semantic_tools.core.validation.rules.quoted_templates import QuotedTemplateValidator
from snowflake_semantic_tools.core.validation.rules.references import ReferenceValidator
from snowflake_semantic_tools.core.validation.rules.semantic_models import SemanticModelValidator


def _assert_fires(result, code):
    matching = [i for i in result.issues if i.rule_id == code]
    assert len(matching) > 0, f"{code} never fired. Issues: {[i.rule_id for i in result.issues]}"
    issue = matching[0]
    assert issue.suggestion, f"{code} has no suggestion"
    assert issue.rule_id == code
    renderer = DiagnosticRenderer(use_colors=False)
    output = renderer.render(issue)
    assert code in output
    assert "= help:" in output
    return issue


class TestEveryErrorCode:

    def test_SST_V001_missing_required_field(self):
        validator = SemanticModelValidator()
        data = {"metrics": {"items": [{"expr": "SUM(amount)", "tables": ["orders"]}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V001")

    def test_SST_V002_unknown_table_ref(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": [{"name": "revenue", "tables": ["nonexistent_table"], "expr": "SUM(amount)"}]},
            "relationships": {"items": [], "relationship_columns": []},
        }
        dbt_catalog = {"orders": {"columns": ["amount"], "database": "DB", "schema": "SCH"}}
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V002")

    def test_SST_V003_unknown_column_ref(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "revenue",
                        "tables": ["orders"],
                        "expr": "SUM(orders.nonexistent_col)",
                    }
                ]
            },
            "relationships": {"items": [], "relationship_columns": []},
        }
        dbt_catalog = {"orders": {"columns": {"amount": True, "id": True}, "database": "DB", "schema": "SCH"}}
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V003")

    def test_SST_V004_duplicate_name(self):
        validator = DuplicateValidator()
        semantic_data = {
            "metrics": {
                "items": [
                    {"name": "revenue", "expr": "SUM(amount)", "tables": ["orders"]},
                    {"name": "revenue", "expr": "COUNT(*)", "tables": ["orders"]},
                ]
            }
        }
        result = validator.validate(semantic_data)
        _assert_fires(result, "SST-V004")

    def test_SST_V005_wrong_field_type(self):
        validator = SemanticModelValidator()
        data = {"metrics": {"items": [{"name": "revenue", "expr": "SUM(amount)", "tables": "orders"}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V005")

    def test_SST_V006_empty_required_field(self):
        validator = SemanticModelValidator()
        data = {"metrics": {"items": [{"name": "revenue", "expr": "", "tables": ["orders"]}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V006")

    def test_SST_V007_invalid_column_type(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "status",
                    "table_name": "orders",
                    "column_type": "measure",
                    "data_type": "text",
                    "description": "Status",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V007")

    def test_SST_V008_invalid_data_type(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "BLAH",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V008")

    def test_SST_V010_missing_primary_key(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": [], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V010")

    def test_SST_V011_pk_column_not_found(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "orders",
                    "primary_key": ["nonexistent"],
                    "description": "Orders",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V011")

    def test_SST_V012_missing_description(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V012")

    def test_SST_V013_synonyms_wrong_type(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [{"name": "revenue", "expr": "SUM(amount)", "tables": ["orders"], "synonyms": "not a list"}]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V013")

    def test_SST_V014_problematic_chars_in_synonyms(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "orders",
                    "primary_key": ["id"],
                    "description": "Orders",
                    "synonyms": ["it's orders", "order's table"],
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V014")

    def test_SST_V015_invalid_constraints(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "events",
                    "primary_key": ["id"],
                    "description": "Events",
                    "source_file": "/tmp/t.yml",
                    "constraints": [
                        {"type": "distinct_range", "name": "dr1", "start_column": "", "end_column": "end_date"}
                    ],
                }
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "events",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [
                {
                    "name": "end_date",
                    "table_name": "events",
                    "column_type": "time_dimension",
                    "data_type": "date",
                    "description": "End",
                    "source_file": "/tmp/t.yml",
                }
            ],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V015")

    def test_SST_V016_invalid_tags(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [{"name": "revenue", "expr": "SUM(amount)", "tables": ["orders"], "tags": "not a dict"}]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V016")

    def test_SST_V020_column_missing_description(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V020")

    def test_SST_V021_column_missing_column_type(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V021")

    def test_SST_V022_column_missing_data_type(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V022")

    def test_SST_V023_fact_non_numeric(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [
                {
                    "name": "status",
                    "table_name": "orders",
                    "column_type": "fact",
                    "data_type": "text",
                    "description": "Status",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V023")

    def test_SST_V024_time_dimension_non_temporal(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                }
            ],
            "sm_facts": [],
            "sm_time_dimensions": [
                {
                    "name": "amount",
                    "table_name": "orders",
                    "column_type": "time_dimension",
                    "data_type": "number",
                    "description": "Amount",
                    "source_file": "/tmp/t.yml",
                }
            ],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V024")

    def test_SST_V025_enum_without_samples(self):
        validator = DbtModelValidator()
        dbt_data = {
            "sm_tables": [
                {"table_name": "orders", "primary_key": ["id"], "description": "Orders", "source_file": "/tmp/t.yml"}
            ],
            "sm_dimensions": [
                {
                    "name": "id",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "number",
                    "description": "ID",
                    "source_file": "/tmp/t.yml",
                },
                {
                    "name": "status",
                    "table_name": "orders",
                    "column_type": "dimension",
                    "data_type": "text",
                    "description": "Status",
                    "source_file": "/tmp/t.yml",
                    "is_enum": True,
                },
            ],
            "sm_facts": [],
            "sm_time_dimensions": [],
        }
        result = validator.validate(dbt_data)
        _assert_fires(result, "SST-V025")

    def test_SST_V032_metric_missing_tables_field(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": [{"name": "revenue", "expr": "SUM(amount)"}]},
            "relationships": {"items": [], "relationship_columns": []},
        }
        dbt_catalog = {"orders": {"columns": ["amount"], "database": "DB", "schema": "SCH"}}
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V032")

    def test_SST_V033_expr_wrong_type(self):
        validator = SemanticModelValidator()
        data = {"metrics": {"items": [{"name": "revenue", "expr": 123, "tables": ["orders"]}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V033")

    def test_SST_V034_empty_tables_list(self):
        validator = SemanticModelValidator()
        data = {"metrics": {"items": [{"name": "revenue", "expr": "SUM(amount)", "tables": []}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V034")

    def test_SST_V035_invalid_visibility(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [{"name": "revenue", "expr": "SUM(amount)", "tables": ["orders"], "visibility": "secret"}]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V035")

    def test_SST_V036_invalid_non_additive_by(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [
                    {
                        "name": "revenue",
                        "expr": "SUM(amount)",
                        "tables": ["orders"],
                        "non_additive_by": [{"no_dimension": True}],
                    }
                ]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V036")

    def test_SST_V037_invalid_window_config(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [{"name": "revenue", "expr": "SUM(amount)", "tables": ["orders"], "window": "not a dict"}]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V037")

    def test_SST_V038_using_relationships_wrong_type(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [
                    {
                        "name": "revenue",
                        "expr": "SUM(amount)",
                        "tables": ["orders"],
                        "using_relationships": "not a list",
                    }
                ]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V038")

    def test_SST_V040_relationship_missing_field(self):
        validator = SemanticModelValidator()
        data = {
            "relationships": {
                "items": [
                    {
                        "name": "order_customer",
                        "left_table": "orders",
                        "right_table": "customers",
                        "relationship_conditions": ["BAD CONDITION NO EQUALS"],
                    }
                ]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V040")

    def test_SST_V041_relationship_unknown_table(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": []},
            "relationships": {
                "items": [
                    {"relationship_name": "order_bad", "left_table_name": "orders", "right_table_name": "bad_table"}
                ],
                "relationship_columns": [
                    {"relationship_name": "order_bad", "left_column": "id", "right_column": "order_id"}
                ],
            },
        }
        dbt_catalog = {
            "orders": {"columns": ["id", "amount"], "database": "DB", "schema": "SCH", "primary_key": ["id"]}
        }
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V041")

    def test_SST_V042_relationship_missing_pk(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": []},
            "relationships": {
                "items": [
                    {
                        "relationship_name": "order_customer",
                        "left_table_name": "orders",
                        "right_table_name": "customers",
                    }
                ],
                "relationship_columns": [
                    {"relationship_name": "order_customer", "left_column": "customer_id", "right_column": "email"}
                ],
            },
        }
        dbt_catalog = {
            "orders": {
                "columns": ["id", "customer_id", "amount"],
                "database": "DB",
                "schema": "SCH",
                "primary_key": ["id"],
            },
            "customers": {"columns": ["id", "email", "name"], "database": "DB", "schema": "SCH", "primary_key": ["id"]},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V042")

    def test_SST_V043_relationship_column_not_found(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": []},
            "relationships": {
                "items": [
                    {
                        "relationship_name": "order_customer",
                        "left_table_name": "orders",
                        "right_table_name": "customers",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "order_customer",
                        "left_column": "customer_id",
                        "right_column": "customer_id",
                    },
                    {
                        "relationship_name": "order_customer",
                        "left_column": "customer_id",
                        "right_column": "customer_id",
                    },
                ],
            },
        }
        dbt_catalog = {
            "orders": {"columns": ["id", "customer_id"], "database": "DB", "schema": "SCH", "primary_key": ["id"]},
            "customers": {"columns": ["id", "customer_id"], "database": "DB", "schema": "SCH", "primary_key": ["id"]},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V043")

    def test_SST_V049_multi_column_expression_in_relationship(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": []},
            "relationships": {
                "items": [
                    {
                        "relationship_name": "order_customer",
                        "left_table_name": "orders",
                        "right_table_name": "customers",
                    }
                ],
                "relationship_columns": [
                    {
                        "relationship_name": "order_customer",
                        "left_column": "ORDERS.CUSTOMER_ID",
                        "right_column": "CUSTOMERS.CUSTOMER_ID",
                        "left_unresolved_expression": "COALESCE(ORDERS.CUSTOMER_ID, ORDERS.ORDER_ID)",
                    },
                ],
            },
        }
        dbt_catalog = {
            "orders": {
                "columns": {"customer_id": {}, "order_id": {}},
                "database": "DB",
                "schema": "SCH",
                "primary_key": "customer_id",
            },
            "customers": {
                "columns": {"customer_id": {}},
                "database": "DB",
                "schema": "SCH",
                "primary_key": "customer_id",
            },
        }
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V049")

        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "revenue",
                        "tables": ["orders"],
                        "expr": "SUM(amount)",
                        "using_relationships": ["nonexistent_rel"],
                    }
                ]
            },
            "relationships": {
                "items": [
                    {
                        "relationship_name": "order_customer",
                        "left_table_name": "orders",
                        "right_table_name": "customers",
                    }
                ],
                "relationship_columns": [
                    {"relationship_name": "order_customer", "left_column": "customer_id", "right_column": "id"}
                ],
            },
        }
        dbt_catalog = {
            "orders": {
                "columns": ["id", "customer_id", "amount"],
                "database": "DB",
                "schema": "SCH",
                "primary_key": ["id"],
            },
            "customers": {"columns": ["id", "name"], "database": "DB", "schema": "SCH", "primary_key": ["id"]},
        }
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V044")

    def test_SST_V050_filter_missing_field(self):
        validator = SemanticModelValidator()
        data = {"filters": {"items": [{"expr": "status = 'active'"}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V050")

    def test_SST_V051_filter_invalid_expr(self):
        validator = SemanticModelValidator()
        data = {"filters": {"items": [{"name": "active_filter", "expr": 123}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V051")

    def test_SST_V060_vqr_missing_field(self):
        validator = SemanticModelValidator()
        data = {"verified_queries": {"items": [{"name": "test_query", "question": "How many orders?"}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V060")

    def test_SST_V061_vqr_sql_file_not_found(self):
        validator = SemanticModelValidator()
        data = {
            "verified_queries": {
                "items": [
                    {
                        "name": "test_query",
                        "question": "How many?",
                        "sql_file": "nonexistent.sql",
                        "source_file": "/tmp/vqr.yml",
                    }
                ]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V061")

    def test_SST_V062_vqr_mutual_exclusivity(self):
        validator = SemanticModelValidator()
        data = {
            "verified_queries": {
                "items": [{"name": "test_query", "question": "How many?", "sql": "SELECT 1", "sql_file": "query.sql"}]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V062")

    def test_SST_V070_semantic_view_missing_field(self):
        validator = SemanticModelValidator()
        data = {"semantic_views": {"items": [{"name": "my_view", "tables": "{invalid json"}]}}
        result = validator.validate(data)
        _assert_fires(result, "SST-V070")

    def test_SST_V071_semantic_view_unknown_table(self):
        validator = ReferenceValidator()
        semantic_data = {
            "metrics": {"items": []},
            "relationships": {"items": [], "relationship_columns": []},
            "semantic_views": {"items": [{"name": "my_view", "tables": '["bad_table"]', "source_file": "/tmp/sv.yml"}]},
            "custom_instructions": {"items": []},
        }
        dbt_catalog = {"orders": {"columns": ["id"], "database": "DB", "schema": "SCH", "primary_key": ["id"]}}
        result = validator.validate(semantic_data, dbt_catalog)
        _assert_fires(result, "SST-V071")

    def test_SST_V081_quoted_template(self):
        from snowflake_semantic_tools.core.models.validation import ValidationResult

        validator = QuotedTemplateValidator()
        metrics_data = {
            "snowflake_metrics": [{"name": "revenue", "tables": ["\"{{ ref('orders') }}\""], "expr": "SUM(amount)"}]
        }
        relationships_data = {"snowflake_relationships": []}
        semantic_views_data = {"semantic_views": []}
        filters_data = {"snowflake_filters": []}
        result = ValidationResult()
        validator.validate(metrics_data, relationships_data, semantic_views_data, filters_data, result)
        _assert_fires(result, "SST-V081")

    def test_SST_V090_circular_dependency(self):
        validator = SemanticModelValidator()
        data = {
            "metrics": {
                "items": [
                    {"name": "metric_a", "expr": "{{ metric('metric_b') }}", "tables": ["orders"]},
                    {"name": "metric_b", "expr": "{{ metric('metric_a') }}", "tables": ["orders"]},
                ]
            }
        }
        result = validator.validate(data)
        _assert_fires(result, "SST-V090")

    def test_SST_V091_duplicate_expressions(self):
        validator = DuplicateValidator()
        semantic_data = {
            "metrics": {
                "items": [
                    {"name": "revenue", "expr": "SUM(amount)", "tables": ["orders"]},
                    {"name": "total_amount", "expr": "SUM(amount)", "tables": ["orders"]},
                ]
            }
        }
        result = validator.validate(semantic_data)
        _assert_fires(result, "SST-V091")
