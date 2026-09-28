"""YAML load-boundary tests: neutralization, duplicate keys, and streams."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.project import ProjectError
from snowflake_semantic_tools.adapters.yaml.loader import _parse_yaml_bytes, _read_yaml


def write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_unquoted_and_quoted_templates_converge_on_the_same_tree(tmp_path: Path) -> None:
    unquoted = _read_yaml(write(tmp_path / "unquoted.yml", "tables:\n  - {{ ref('orders') }}\n"))
    quoted = _read_yaml(write(tmp_path / "quoted.yml", "tables:\n  - \"{{ ref('orders') }}\"\n"))
    assert unquoted == quoted == {"tables": ["{{ ref('orders') }}"]}


def test_embedded_templates_survive_inside_expression_scalars(tmp_path: Path) -> None:
    document = _read_yaml(
        write(
            tmp_path / "expression.yml",
            "expr: COUNT(DISTINCT {{ ref('orders', 'order_id') }})\n",
        )
    )
    assert document["expr"] == "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"


def test_duplicate_mapping_key_is_a_structured_diagnostic(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as exc_info:
        _read_yaml(write(tmp_path / "duplicate.yml", "metrics: []\nmetrics: []\n"))
    assert exc_info.value.diagnostics[0].code == "SST-LOD005"
    assert "duplicate key 'metrics'" in str(exc_info.value)


def test_multi_document_stream_is_a_structured_diagnostic(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as exc_info:
        _read_yaml(write(tmp_path / "stream.yml", "a: 1\n---\nb: 2\n"))
    assert exc_info.value.diagnostics[0].code == "SST-LOD008"
    assert "contains 2 documents" in str(exc_info.value)


def test_yaml_syntax_error_retains_line_and_column(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as exc_info:
        _read_yaml(write(tmp_path / "bad.yml", "a: [\n"))
    diagnostic = exc_info.value.diagnostics[0]
    assert diagnostic.code == "SST-LOD001"
    assert diagnostic.context["line"] == 2
    assert diagnostic.context["col"] == 1


def test_parsed_yaml_retains_node_and_template_positions() -> None:
    parsed = _parse_yaml_bytes(
        b"snowflake_metrics:\n  - name: orders\n    expr: \"COUNT({{ ref('orders', 'id') }})\"\n",
        "semantic_models/metrics/orders.yml",
    )
    assert parsed.line_index[("snowflake_metrics", 0, "expr")].line == 3
    template = next(iter(parsed.templates.values()))
    assert template.raw == "{{ ref('orders', 'id') }}"
    assert (template.line, template.col) == (3, 18)
