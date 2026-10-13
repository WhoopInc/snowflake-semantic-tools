"""Enrich's in-place YAML writers: the round trip and dbt model files."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_model_updates
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.roundtrip import CommentedMap, insert_key, load_editable, scalar
from snowflake_semantic_tools.domain.enrich import ColumnUpdate

DBT = """version: 2

models:
  - name: orders  # the fact table
    description: "Orders."
    columns:
      - name: order_id
        meta:
          sst:
            column_type: dimension
            sample_values: ['a', 'b']
      - name: total
        config:
          meta:
            sst:
              data_type: NUMBER
      - name: note
        description: 'it''s free text'
"""


def test_a_file_round_trips_byte_for_byte_in_its_own_layout() -> None:
    flat = "---\n# head\nmodels:\n- name: a\n  columns:\n  - name: b\n    description: 'it''s'\n"
    for text in (DBT, flat, "models: []", "models: []\n\n\n"):
        document = load_editable(text, "f.yml")
        assert document.dump() == text and not document.reformats()
    mixed = "models:\n  - name: a\n    tests:\n    - x\n"
    assert load_editable(mixed, "f.yml").reformats()
    with pytest.raises(ProjectError, match="f.yml: cannot parse YAML"):
        load_editable("a: [", "f.yml")


def test_text_dbt_would_read_as_another_type_is_quoted() -> None:
    assert [type(scalar(value)).__name__ for value in ("no", "On", "1:20", "012", "plain", "2024")] == [
        "SingleQuotedScalarString",
        "SingleQuotedScalarString",
        "SingleQuotedScalarString",
        "SingleQuotedScalarString",
        "str",
        "SingleQuotedScalarString",
    ]
    assert type(scalar("a: [")).__name__ == "SingleQuotedScalarString"


def test_keys_are_inserted_in_the_order_enrich_writes_them() -> None:
    order = ("column_type", "data_type", "synonyms", "sample_values", "is_enum")
    mapping = CommentedMap([("other", 1), ("sample_values", []), ("is_enum", True)])
    insert_key(mapping, "data_type", "TEXT", order)
    insert_key(mapping, "is_enum", False, order)
    insert_key(mapping, "unlisted", 2, order)
    insert_key(mapping, "column_type", "fact", ("data_type",))
    assert list(mapping.items()) == [
        ("other", 1),
        ("data_type", "TEXT"),
        ("sample_values", []),
        ("is_enum", False),
        ("unlisted", 2),
        ("column_type", "fact"),
    ]


def test_column_updates_edit_each_sst_block_where_it_is_and_append_new_columns() -> None:
    updates = {
        "orders": [
            ColumnUpdate("ORDER_ID", (("column_type", "dimension"), ("is_enum", True))),
            ColumnUpdate("total", (("column_type", "fact"),)),
            ColumnUpdate("note", (("synonyms", ("remark", "no")),)),
            ColumnUpdate("added", (("data_type", "TEXT"), ("sample_values", ("on", "x"))), added=True),
        ]
    }
    written = write_model_updates(DBT, "orders.yml", updates)
    assert not written.reformatted
    expected = (
        DBT.replace(
            "            sample_values: ['a', 'b']\n",
            "            sample_values: ['a', 'b']\n            is_enum: true\n",
        )
        .replace(
            "            sst:\n              data_type: NUMBER\n",
            "            sst:\n              column_type: fact\n              data_type: NUMBER\n",
        )
        .replace(
            "        description: 'it''s free text'\n",
            "        description: 'it''s free text'\n        config:\n          meta:\n            sst:\n"
            "              synonyms:\n                - remark\n                - 'no'\n"
            "      - name: added\n        config:\n          meta:\n            sst:\n"
            "              data_type: TEXT\n              sample_values:\n"
            "                - 'on'\n                - x\n",
        )
    )
    assert written.text == expected


def test_a_model_or_file_the_project_lacks_is_created_in_dbts_layout() -> None:
    created = write_model_updates(None, "new.yml", {"m": [ColumnUpdate("a", (("column_type", "fact"),), added=True)]})
    assert created.text == (
        "version: 2\nmodels:\n  - name: m\n    columns:\n      - name: a\n        config:\n          meta:\n"
        "            sst:\n              column_type: fact\n"
    )
    appended = write_model_updates("version: 2\nmodels:\n  - name: x\n", "f.yml", {"m": []})
    assert appended.text == "version: 2\nmodels:\n  - name: x\n  - name: m\n    columns: []\n"


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("- not a mapping\n", "a dbt YAML file must be a mapping"),
        ("models: {}\n", "models must be a list"),
        ("models:\n  - name: m\n    columns:\n      - name: a\n        config: text\n", "config must be a mapping"),
    ],
)
def test_a_file_enrich_cannot_edit_is_refused_with_what_is_wrong(text: str, problem: str) -> None:
    with pytest.raises(ProjectError, match=problem):
        write_model_updates(text, "f.yml", {"m": [ColumnUpdate("a", (("column_type", "fact"),))]})


MODELS = """version: 2

models:
  - name: orders
    description: Orders.
    columns:
      - name: order_id
  - name: customers
    config:
      meta:
        sst:
          primary_key: [customer_id]
          distinct_range: {start: a, end: b}
  - name: products
    meta:
      sst:
        synonyms:
          - item
"""


def test_table_synonyms_go_into_each_models_sst_block() -> None:
    synonyms = {"orders": ("sales order", "yes"), "customers": ("buyer",), "products": ("menu item",)}
    written = write_model_updates(MODELS, "m.yml", {}, synonyms)
    assert not written.reformatted
    assert written.text == (
        MODELS.replace(
            "    description: Orders.\n",
            "    description: Orders.\n    config:\n      meta:\n        sst:\n          synonyms:\n"
            "            - sales order\n            - 'yes'\n",
        )
        .replace("[customer_id]\n", "[customer_id]\n          synonyms:\n            - buyer\n")
        .replace("          - item\n", "          - menu item\n")
    )


def test_a_model_gains_table_synonyms_and_column_updates_together() -> None:
    written = write_model_updates(
        None, "new.yml", {"m": [ColumnUpdate("a", (("column_type", "fact"),), added=True)]}, {"m": ("s",), "n": ("t",)}
    )
    assert written.text == (
        "version: 2\nmodels:\n  - name: m\n    config:\n      meta:\n        sst:\n          synonyms:\n"
        "            - s\n    columns:\n      - name: a\n        config:\n          meta:\n            sst:\n"
        "              column_type: fact\n  - name: n\n    config:\n      meta:\n        sst:\n"
        "          synonyms:\n            - t\n"
    )


def test_text_is_replaced_atomically(tmp_path: Path) -> None:
    target = tmp_path / "models" / "orders.yml"
    write_within(tmp_path, target, "first\n")
    write_within(tmp_path, target, "second ✓\n")
    assert target.read_text(encoding="utf-8") == "second ✓\n"
    assert [path.name for path in target.parent.iterdir()] == ["orders.yml"]
