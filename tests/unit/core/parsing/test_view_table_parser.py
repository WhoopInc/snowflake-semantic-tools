"""Tests for the shared view_table_parser utility."""

from pathlib import Path

import pytest

from snowflake_semantic_tools.core.parsing.view_table_parser import parse_view_tables


@pytest.fixture
def sem_dir(tmp_path):
    """Create a temporary semantic models directory."""
    d = tmp_path / "snowflake_semantic_models"
    d.mkdir()
    return d


class TestParseViewTables:

    def test_empty_directory(self, sem_dir):
        view_map, source_map = parse_view_tables(sem_dir)
        assert view_map == {}
        assert source_map == {}

    def test_nonexistent_directory(self, tmp_path):
        view_map, source_map = parse_view_tables(tmp_path / "nonexistent")
        assert view_map == {}
        assert source_map == {}

    def test_yaml_with_ref_templates(self, sem_dir):
        yaml_content = """\
semantic_views:
  - name: customer_360
    tables:
      - "{{ ref('customers') }}"
      - "{{ ref('orders') }}"
  - name: sales_summary
    tables:
      - "{{ table('orders') }}"
"""
        (sem_dir / "views.yml").write_text(yaml_content)

        view_map, source_map = parse_view_tables(sem_dir)

        assert view_map == {
            "customer_360": ["customers", "orders"],
            "sales_summary": ["orders"],
        }
        assert "customer_360" in source_map
        assert "sales_summary" in source_map
        assert source_map["customer_360"].endswith("views.yml")

    def test_yaml_with_plain_table_names(self, sem_dir):
        yaml_content = """\
semantic_views:
  - name: simple_view
    tables:
      - db.schema.my_table
"""
        (sem_dir / "views.yml").write_text(yaml_content)

        view_map, source_map = parse_view_tables(sem_dir)

        assert view_map == {"simple_view": ["my_table"]}

    def test_skips_files_without_semantic_views(self, sem_dir):
        yaml_content = """\
snowflake_metrics:
  - name: total_revenue
    expr: SUM(amount)
"""
        (sem_dir / "metrics.yml").write_text(yaml_content)

        view_map, source_map = parse_view_tables(sem_dir)
        assert view_map == {}
        assert source_map == {}

    def test_jinja_template_fallback(self, sem_dir):
        yaml_content = """\
semantic_views:
  - name: analytics_view
    tables:
      - {{ ref('customers') }}
      - {{ table('orders') }}
"""
        (sem_dir / "views.yml").write_text(yaml_content)

        view_map, source_map = parse_view_tables(sem_dir)

        assert "analytics_view" in view_map
        assert set(view_map["analytics_view"]) == {"customers", "orders"}

    def test_multiple_files(self, sem_dir):
        (sem_dir / "a.yml").write_text(
            """\
semantic_views:
  - name: view_a
    tables:
      - "{{ ref('table_a') }}"
"""
        )
        (sem_dir / "b.yml").write_text(
            """\
semantic_views:
  - name: view_b
    tables:
      - "{{ ref('table_b') }}"
"""
        )

        view_map, source_map = parse_view_tables(sem_dir)

        assert "view_a" in view_map
        assert "view_b" in view_map
        assert view_map["view_a"] == ["table_a"]
        assert view_map["view_b"] == ["table_b"]
        assert source_map["view_a"].endswith("a.yml")
        assert source_map["view_b"].endswith("b.yml")

    def test_nested_directory(self, sem_dir):
        nested = sem_dir / "subdirectory"
        nested.mkdir()
        (nested / "nested_views.yaml").write_text(
            """\
semantic_views:
  - name: nested_view
    tables:
      - "{{ ref('nested_table') }}"
"""
        )

        view_map, source_map = parse_view_tables(sem_dir)

        assert view_map == {"nested_view": ["nested_table"]}
        assert "nested_views.yaml" in source_map["nested_view"]

    def test_malformed_yaml_falls_back_to_regex(self, sem_dir):
        yaml_content = """\
semantic_views:
  - name: broken_view
    tables:
      - {{ ref('table1') }}
    description: This has: a colon that breaks YAML
"""
        (sem_dir / "views.yml").write_text(yaml_content)

        view_map, source_map = parse_view_tables(sem_dir)

        assert "broken_view" in view_map
        assert "table1" in view_map["broken_view"]

    def test_view_without_tables_gets_empty_list(self, sem_dir):
        yaml_content = """\
semantic_views:
  - name: empty_view
    description: No tables defined
"""
        (sem_dir / "views.yml").write_text(yaml_content)

        view_map, source_map = parse_view_tables(sem_dir)

        assert view_map == {"empty_view": []}
