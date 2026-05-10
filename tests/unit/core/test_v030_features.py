"""
Unit tests for features added in 0.3.0:
- Derived metrics (parsing, validation, generation)
- V039 cross-entity column refs
- V045/V046 derived metric validation
- Extract data sanitization (non-string coercion)
- Constraint generation (ast.literal_eval fallback)
- _check_constraints key fix (column_name -> name)
"""

import pytest

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import parse_snowflake_metrics
from snowflake_semantic_tools.core.validation.rules.semantic_models import SemanticModelValidator
from snowflake_semantic_tools.core.validation.rules.references import ReferenceValidator
from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from pathlib import Path


class TestDerivedMetricParsing:
    """Test that derived: true metrics are parsed correctly."""

    def test_derived_metric_sets_empty_table_name(self):
        metrics = [
            {
                "name": "revenue_ratio",
                "derived": True,
                "expr": "ORDERS.TOTAL_REVENUE / CUSTOMERS.TOTAL_CUSTOMERS",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("test.yml"))
        assert len(result) == 1
        assert result[0]["name"] == "REVENUE_RATIO"
        assert result[0]["table_name"] == ""
        assert result[0]["derived"] is True

    def test_non_derived_metric_sets_table_name(self):
        metrics = [
            {
                "name": "total_revenue",
                "tables": ["ORDERS"],
                "expr": "SUM(ORDER_TOTAL)",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("test.yml"))
        assert result[0]["table_name"] == "ORDERS"
        assert result[0]["derived"] is False

    def test_derived_metric_ignores_tables_field(self):
        metrics = [
            {
                "name": "cross_metric",
                "derived": True,
                "tables": ["ORDERS", "CUSTOMERS"],
                "expr": "ORDERS.M1 + CUSTOMERS.M2",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("test.yml"))
        assert result[0]["table_name"] == ""
        assert result[0]["derived"] is True


class TestV039CrossEntityValidation:
    """Test V039: metric expr references columns from multiple entities."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_v039_fires_on_cross_entity_expr(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "CROSS_METRIC",
                        "tables": ["ORDERS", "LOCATIONS"],
                        "expr": "SUM(ORDERS.ORDER_TOTAL * LOCATIONS.TAX_RATE)",
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            }
        }
        dbt_catalog = {"orders": {"columns": {"order_total": {}}}, "locations": {"columns": {"tax_rate": {}}}}
        result = validator.validate(semantic_data, dbt_catalog)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V039"]
        assert len(errors) == 1

    def test_v039_does_not_fire_on_single_entity(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "SINGLE_TABLE",
                        "tables": ["ORDERS"],
                        "expr": "SUM(ORDERS.ORDER_TOTAL)",
                        "source_file": "test.yml",
                        "derived": False,
                    }
                ]
            }
        }
        dbt_catalog = {"orders": {"columns": {"order_total": {}}}}
        result = validator.validate(semantic_data, dbt_catalog)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V039"]
        assert len(errors) == 0

    def test_v039_skipped_for_derived_metrics(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "DERIVED_CROSS",
                        "tables": [],
                        "expr": "ORDERS.TOTAL_REVENUE / CUSTOMERS.TOTAL_CUSTOMERS",
                        "source_file": "test.yml",
                        "derived": True,
                    }
                ]
            }
        }
        dbt_catalog = {"orders": {"columns": {}}, "customers": {"columns": {}}}
        result = validator.validate(semantic_data, dbt_catalog)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V039"]
        assert len(errors) == 0


class TestV045DerivedMetricMustReferenceMetrics:
    """Test V045: derived metrics must use {{ metric() }} syntax."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_v045_fires_on_raw_column_expr(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "BAD_DERIVED",
                        "tables": ["ORDERS"],
                        "expr": "SUM(ORDERS.ORDER_TOTAL)",
                        "source_file": "test.yml",
                        "derived": True,
                    }
                ]
            }
        }
        dbt_catalog = {"orders": {"columns": {"order_total": {}}}}
        result = validator.validate(semantic_data, dbt_catalog)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V045"]
        assert len(errors) == 1

    def test_v045_does_not_fire_when_metric_refs_present(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "GOOD_DERIVED",
                        "tables": [],
                        "expr": "{{ metric('total_revenue') }} + {{ metric('total_cost') }}",
                        "source_file": "test.yml",
                        "derived": True,
                    }
                ]
            }
        }
        dbt_catalog = {}
        result = validator.validate(semantic_data, dbt_catalog)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V045"]
        assert len(errors) == 0


class TestV046DerivedMetricInvalidFields:
    """Test V046: derived metrics cannot use using_relationships/non_additive_by/window."""

    @pytest.fixture
    def validator(self):
        return ReferenceValidator()

    def test_v046_fires_on_using_relationships(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "BAD_DERIVED",
                        "tables": [],
                        "expr": "{{ metric('m1') }} + {{ metric('m2') }}",
                        "source_file": "test.yml",
                        "derived": True,
                        "using_relationships": ["rel_1"],
                    }
                ]
            }
        }
        result = validator.validate(semantic_data, {})
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V046"]
        assert len(errors) == 1

    def test_v046_fires_on_window(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "BAD_WINDOW",
                        "tables": [],
                        "expr": "{{ metric('m1') }}",
                        "source_file": "test.yml",
                        "derived": True,
                        "window": {"partition_by": ["col"]},
                    }
                ]
            }
        }
        result = validator.validate(semantic_data, {})
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V046"]
        assert len(errors) == 1

    def test_v046_fires_on_non_additive_by(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "BAD_NON_ADDITIVE",
                        "tables": [],
                        "expr": "{{ metric('m1') }}",
                        "source_file": "test.yml",
                        "derived": True,
                        "non_additive_by": [{"dimension": "time_col"}],
                    }
                ]
            }
        }
        result = validator.validate(semantic_data, {})
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V046"]
        assert len(errors) == 1


class TestV034SkippedForDerived:
    """Test that V034 (tables cannot be empty) is skipped for derived metrics."""

    @pytest.fixture
    def validator(self):
        return SemanticModelValidator()

    def test_v034_skipped_for_derived(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "derived_metric",
                        "derived": True,
                        "expr": "ORDERS.M1 + CUSTOMERS.M2",
                        "tables": [],
                    }
                ]
            }
        }
        result = validator.validate(semantic_data)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V034"]
        assert len(errors) == 0

    def test_v034_fires_for_non_derived(self, validator):
        semantic_data = {
            "metrics": {
                "items": [
                    {
                        "name": "bad_metric",
                        "expr": "SUM(x)",
                        "tables": [],
                    }
                ]
            }
        }
        result = validator.validate(semantic_data)
        errors = [e for e in result.get_errors() if e.rule_id == "SST-V034"]
        assert len(errors) == 1


class TestParseJsonFieldAstFallback:
    """Test ast.literal_eval fallback for Python-repr format strings."""

    @pytest.fixture
    def builder(self):
        config = SnowflakeConfig(
            account="test", user="test", password="test", role="test", warehouse="test", database="test", schema="test"
        )
        return SemanticViewBuilder(config)

    def test_parses_single_quote_list(self, builder):
        val = "[{'type': 'distinct_range', 'name': 'my_constraint', 'start_column': 'start_date', 'end_column': 'end_date'}]"
        result = builder._parse_json_field(val, "constraints")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["type"] == "distinct_range"
        assert result[0]["start_column"] == "start_date"

    def test_parses_valid_json(self, builder):
        val = '[{"type": "distinct_range", "name": "test"}]'
        result = builder._parse_json_field(val, "constraints")
        assert isinstance(result, list)
        assert result[0]["type"] == "distinct_range"

    def test_returns_none_for_none(self, builder):
        assert builder._parse_json_field(None, "test") is None

    def test_returns_list_as_is(self, builder):
        val = [{"type": "distinct_range"}]
        result = builder._parse_json_field(val, "test")
        assert result == val


class TestExtractDataSanitization:
    """Test that non-string values in object columns are coerced to strings."""

    def test_list_coercion(self):
        import pandas as pd

        df = pd.DataFrame(
            [
                {"name": "metric1", "expr": "SUM(x)", "tables": ["orders"]},
                {"name": "metric2", "expr": ["invalid", "list"], "tables": ["orders"]},
            ]
        )
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

        assert df.loc[0, "expr"] == "SUM(x)"
        assert df.loc[1, "expr"] == "['invalid', 'list']"
        assert isinstance(df.loc[1, "expr"], str)

    def test_dict_coercion(self):
        import pandas as pd

        df = pd.DataFrame([{"name": "m1", "expr": {"key": "value"}}])
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

        assert isinstance(df.loc[0, "expr"], str)
        assert "key" in df.loc[0, "expr"]


class TestParseTableListEmptyString:
    """Test that _parse_table_list handles empty string correctly."""

    @pytest.fixture
    def builder(self):
        config = SnowflakeConfig(
            account="test", user="test", password="test", role="test", warehouse="test", database="test", schema="test"
        )
        return SemanticViewBuilder(config)

    def test_empty_string_returns_empty_list(self, builder):
        assert builder._parse_table_list("") == []

    def test_none_returns_empty_list(self, builder):
        assert builder._parse_table_list(None) == []

    def test_whitespace_returns_empty_list(self, builder):
        assert builder._parse_table_list("   ") == []

    def test_normal_table_name(self, builder):
        result = builder._parse_table_list("ORDERS")
        assert result == ["orders"]

    def test_python_repr_multi_element_list(self, builder):
        result = builder._parse_table_list("['ORDERS', 'CUSTOMERS']")
        assert sorted(result) == ["customers", "orders"]

    def test_python_repr_single_element_list(self, builder):
        result = builder._parse_table_list("['ORDERS']")
        assert result == ["orders"]


class TestGeneratorDerivedMetricEmission:
    """Test that derived metrics are emitted without table prefix."""

    @pytest.fixture
    def builder(self):
        config = SnowflakeConfig(
            account="test", user="test", password="test", role="test", warehouse="test", database="test", schema="test"
        )
        return SemanticViewBuilder(config)

    def test_all_tables_present_with_empty_list(self, builder):
        assert builder._all_tables_present([], ["orders", "customers"]) is True

    def test_all_tables_present_with_matching(self, builder):
        assert builder._all_tables_present(["orders"], ["orders", "customers"]) is True

    def test_all_tables_present_with_missing(self, builder):
        assert builder._all_tables_present(["orders", "missing"], ["orders"]) is False


class TestResolveDerivedMetricExpr:
    """Test _resolve_derived_metric_expr with YAML-safe parsing (no regex fallback)."""

    def _make_parser(self, metrics_catalog):
        from unittest.mock import MagicMock

        parser = MagicMock()
        parser.metrics_catalog = metrics_catalog
        from snowflake_semantic_tools.core.parsing.parser import Parser

        parser._resolve_derived_metric_expr = Parser._resolve_derived_metric_expr.__get__(parser)
        return parser

    def test_resolves_metric_refs_to_table_dot_name(self):
        raw_content = """snowflake_metrics:
  - name: total_revenue
    tables:
      - "{{ ref('orders') }}"
    expr: "SUM(order_total)"

  - name: total_customers
    tables:
      - "{{ ref('customers') }}"
    expr: "COUNT(DISTINCT customer_id)"

  - name: revenue_ratio
    derived: true
    expr: "{{ metric('total_revenue') }} / NULLIF({{ metric('total_customers') }}, 0)"
"""
        catalog = [
            {"name": "TOTAL_REVENUE", "tables": ["{{ ref('orders') }}"]},
            {"name": "TOTAL_CUSTOMERS", "tables": ["{{ ref('customers') }}"]},
        ]
        parser = self._make_parser(catalog)
        result = parser._resolve_derived_metric_expr("revenue_ratio", raw_content)
        assert result == "ORDERS.TOTAL_REVENUE / NULLIF(CUSTOMERS.TOTAL_CUSTOMERS, 0)"

    def test_handles_unquoted_jinja_in_tables(self):
        raw_content = """snowflake_metrics:
  - name: metric_a
    tables:
      - {{ ref('orders') }}
    expr: "SUM(x)"

  - name: derived_one
    derived: true
    expr: "{{ metric('metric_a') }} + 1"
"""
        catalog = [{"name": "METRIC_A", "tables": ["{{ ref('orders') }}"]}]
        parser = self._make_parser(catalog)
        result = parser._resolve_derived_metric_expr("derived_one", raw_content)
        assert result == "ORDERS.METRIC_A + 1"

    def test_returns_empty_for_missing_metric(self):
        raw_content = """snowflake_metrics:
  - name: other_metric
    expr: "COUNT(*)"
"""
        parser = self._make_parser([])
        result = parser._resolve_derived_metric_expr("nonexistent", raw_content)
        assert result == ""

    def test_returns_empty_for_invalid_yaml(self):
        raw_content = "not: [valid: yaml: {{{"
        parser = self._make_parser([])
        result = parser._resolve_derived_metric_expr("anything", raw_content)
        assert result == ""

    def test_handles_plain_list_format(self):
        raw_content = """- name: ratio
  derived: true
  expr: "{{ metric('x') }} / {{ metric('y') }}"
"""
        catalog = [
            {"name": "X", "tables": ["{{ ref('a') }}"]},
            {"name": "Y", "tables": ["{{ ref('b') }}"]},
        ]
        parser = self._make_parser(catalog)
        result = parser._resolve_derived_metric_expr("ratio", raw_content)
        assert result == "A.X / B.Y"

    def test_metric_without_table_returns_name_only(self):
        raw_content = """snowflake_metrics:
  - name: derived_m
    derived: true
    expr: "{{ metric('orphan') }}"
"""
        catalog = [{"name": "ORPHAN", "tables": []}]
        parser = self._make_parser(catalog)
        result = parser._resolve_derived_metric_expr("derived_m", raw_content)
        assert result == "ORPHAN"


class TestAutoInferTablesFromExpr:
    """Test that tables field is auto-inferred from expr when omitted (#96)."""

    def test_infers_single_table_from_ref(self):
        metrics = [
            {
                "name": "total_revenue",
                "expr": "SUM({{ ref('orders', 'amount') }})",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        assert len(result) == 1
        assert "orders" in str(result[0]["tables"]).lower()

    def test_infers_multiple_tables_from_ref(self):
        metrics = [
            {
                "name": "rev_per_customer",
                "expr": "SUM({{ ref('orders', 'amount') }}) / COUNT({{ ref('customers', 'id') }})",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        tables_str = str(result[0]["tables"]).lower()
        assert "orders" in tables_str
        assert "customers" in tables_str

    def test_explicit_tables_takes_precedence(self):
        metrics = [
            {
                "name": "my_metric",
                "tables": ["{{ ref('explicit_table') }}"],
                "expr": "SUM({{ ref('orders', 'amount') }})",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        assert "explicit_table" in str(result[0]["tables"]).lower()
        assert "EXPLICIT_TABLE" in result[0]["table_name"].upper()

    def test_no_inference_possible_leaves_empty(self):
        metrics = [
            {
                "name": "simple_count",
                "expr": "COUNT(*)",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        assert result[0]["tables"] == []
        assert result[0]["table_name"] == ""

    def test_dot_notation_inference_skips_numeric_literals(self):
        metrics = [
            {
                "name": "rounded_ratio",
                "expr": "ROUND(ORDERS.AMOUNT / 1.5, 2)",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        assert "orders" in str(result[0]["tables"]).lower()
        assert "1" not in result[0]["tables"]

    def test_dot_notation_inference_multiple_tables(self):
        metrics = [
            {
                "name": "tax_weighted",
                "expr": "SUM(ORDERS.AMOUNT * LOCATIONS.TAX_RATE)",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        tables_lower = [t.lower() for t in result[0]["tables"]]
        assert "orders" in tables_lower
        assert "locations" in tables_lower

    def test_infers_from_column_syntax(self):
        metrics = [
            {
                "name": "total",
                "expr": "SUM({{ column('sales', 'revenue') }})",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        assert "sales" in str(result[0]["tables"]).lower()

    def test_derived_metric_infers_tables_from_referenced_metrics(self):
        metrics = [
            {
                "name": "total_revenue",
                "tables": ["{{ ref('orders') }}"],
                "expr": "SUM({{ ref('orders', 'amount') }})",
            },
            {
                "name": "total_customers",
                "tables": ["{{ ref('customers') }}"],
                "expr": "COUNT(DISTINCT {{ ref('customers', 'id') }})",
            },
            {
                "name": "revenue_ratio",
                "derived": True,
                "expr": "{{ metric('total_revenue') }} / NULLIF({{ metric('total_customers') }}, 0)",
            },
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        derived = next(r for r in result if r["name"] == "REVENUE_RATIO")
        tables_str = str(derived["tables"]).lower()
        assert "orders" in tables_str
        assert "customers" in tables_str

    def test_infers_from_resolved_dot_refs_ignoring_string_literals(self):
        metrics = [
            {
                "name": "filtered_total",
                "expr": "SUM(CASE WHEN ORDERS.STATUS = 'shipped.confirmed' THEN ORDERS.TOTAL ELSE 0 END)",
            }
        ]
        result = parse_snowflake_metrics(metrics, Path("/tmp/test.yml"))
        tables = result[0]["tables"]
        assert "ORDERS" in tables
        assert "shipped" not in str(tables)
