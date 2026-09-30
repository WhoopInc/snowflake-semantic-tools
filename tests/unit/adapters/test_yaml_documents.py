"""YAML load-boundary tests: neutralization, duplicate keys, and streams."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from snowflake_semantic_tools.adapters.project import ProjectError
from snowflake_semantic_tools.adapters.yaml.loader import _parse_yaml_bytes, _read_yaml

TEMPLATE = "{{ ref('orders', 'amount') }}"


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


def test_merge_keys_expand_and_explicit_keys_override_them() -> None:
    parsed = _parse_yaml_bytes(
        b"base: &base {a: 1, b: 2}\nother: &other {c: 3}\nuse:\n  <<: [*base, *other]\n  b: 20\n",
        "anchors.yml",
    )
    assert parsed.tree["use"] == {"a": 1, "b": 20, "c": 3}


def test_a_duplicate_explicit_key_beside_a_merge_is_still_reported() -> None:
    with pytest.raises(ProjectError) as exc_info:
        _parse_yaml_bytes(b"base: &base {a: 1}\nuse:\n  <<: *base\n  b: 1\n  b: 2\n", "dup.yml")
    assert exc_info.value.diagnostics[0].code == "SST-LOD005"


@pytest.mark.parametrize(
    ("text", "line", "detail"),
    (
        (b"a: 1\nx: !custom 1\n", 2, "could not determine a constructor for the tag '!custom'"),
        (b"a: 1\nb: 2\nx: 2024-13-01\n", 3, "month must be in 1..12"),
        (b"use:\n  <<: 1\n", 2, "expected a mapping or list of mappings for merging"),
    ),
    ids=("custom-tag", "invalid-date", "merge-of-a-scalar"),
)
def test_values_yaml_cannot_construct_are_located_syntax_diagnostics(text: bytes, line: int, detail: str) -> None:
    with pytest.raises(ProjectError) as exc_info:
        _parse_yaml_bytes(text, "bad.yml")
    diagnostic = exc_info.value.diagnostics[0]
    assert diagnostic.code == "SST-LOD001"
    assert (diagnostic.context["file"], diagnostic.context["line"]) == ("bad.yml", line)
    assert detail in diagnostic.context["detail"]


@pytest.mark.parametrize(
    ("text", "prequoted"),
    (
        (
            f"items:\n  - description: |\n      Literal text.\n    expr: {TEMPLATE}\n",
            f'items:\n  - description: |\n      Literal text.\n    expr: "{TEMPLATE}"\n',
        ),
        (
            f"items:\n  - description: >-  # folded\n      Folded\n      text.\n    expr: {TEMPLATE}\n",
            f'items:\n  - description: >-  # folded\n      Folded\n      text.\n    expr: "{TEMPLATE}"\n',
        ),
        (
            f"items:\n  - description: |\n      Text.\n    expr: SUM(\n      {TEMPLATE})\n",
            f'items:\n  - description: |\n      Text.\n    expr: "SUM(\n      {TEMPLATE})"\n',
        ),
        (
            f"items:\n  - description: |\n      Text.\n    tables:\n      - {TEMPLATE}\n",
            f'items:\n  - description: |\n      Text.\n    tables:\n      - "{TEMPLATE}"\n',
        ),
        (
            f"views:\n  - name: v\n    tables:\n      - description: |\n          Text.\n        table: {TEMPLATE}\n",
            f'views:\n  - name: v\n    tables:\n      - description: |\n          Text.\n        table: "{TEMPLATE}"\n',
        ),
        (
            f"rows:\n  - - note: |\n        Text.\n      value: {TEMPLATE}\n",
            f'rows:\n  - - note: |\n        Text.\n      value: "{TEMPLATE}"\n',
        ),
    ),
    ids=("literal", "folded", "plain-continuation", "list-entry", "nested-list", "compact-nested-sequence"),
)
def test_templates_after_a_block_scalar_in_a_list_item_are_neutralized(text: str, prequoted: str) -> None:
    parsed = _parse_yaml_bytes(text.encode(), "items.yml")
    assert dict(parsed.tree) == yaml.safe_load(prequoted)
    assert [template.raw for template in parsed.templates.values()] == [TEMPLATE]


@pytest.mark.parametrize(
    "text",
    (
        "items:\n  - description: |\n      Write {{ ref( to name a model.\n    name: q\n",
        "items:\n  - |\n    Write {{ ref( to name a model.\n  - other\n",
        "items:\n  - - >-\n      Write {{ ref( to name a model.\n    - other\n",
    ),
    ids=("keyed-block", "entry-block", "nested-entry-block"),
)
def test_block_scalars_in_list_items_are_read_verbatim(text: str) -> None:
    assert dict(_parse_yaml_bytes(text.encode(), "items.yml").tree) == yaml.safe_load(text)
