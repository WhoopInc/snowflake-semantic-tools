"""Mapping tools, skills, plugins and evals, and writing what an import makes: YAML and dbt edits."""

from __future__ import annotations

import json

from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_import_edits
from snowflake_semantic_tools.adapters.yaml.emit import emit_document
from snowflake_semantic_tools.domain.importing.evals import import_eval
from snowflake_semantic_tools.domain.importing.model import ColumnMeta, ModelEdit
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.skill import BundlePlacement, LiveBundle, import_plugin, import_skill
from snowflake_semantic_tools.domain.importing.tool import LiveTool, ToolPlacement, import_tool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import index

_MODELS = {("DB", "MARTS", "ORDERS"): "orders", ("DB", "MARTS", "PRODUCTS"): "products"}
_PLACE = ToolPlacement("tools", Destination(("sales",)))


def _search(**described: str) -> LiveTool:
    properties = {
        "definition": "SELECT name, kind, body FROM DB.MARTS.PRODUCTS",
        "search_column": "BODY",
        "attribute_columns": "KIND",
        "columns": "NAME,KIND,BODY",
        **described,
    }
    return LiveTool(QualifiedName.parse("DB.T.MENU_SEARCH"), "cortex_search_service", "Menu.", properties)


def test_a_tool_member_references_its_object_unless_it_defines_it() -> None:
    referenced = import_tool(_search(), index(models=_MODELS), _PLACE, define=False)
    assert referenced.files[0].document == {
        "tool": {
            "name": "menu_search",
            "type": "cortex_search_service",
            "description": "Menu.",
            "columns": [
                {"column": "name", "searchable": False, "filterable": False},
                {"column": "kind", "searchable": False, "filterable": True},
                {"column": "body", "searchable": True, "filterable": False},
            ],
            "relations": {"dev": "DB.T.MENU_SEARCH"},
        }
    }
    defined = import_tool(_search(warehouse="WH"), index(models=_MODELS), _PLACE, define=True)
    body = (defined.files[0].document or {})["tool"]
    assert body["on"] == {  # type: ignore[index]
        "table": "{{ ref('products') }}",
        "search_column": "{{ ref('products', 'body') }}",
        "attributes": ["{{ ref('products', 'kind') }}"],
    }
    assert body["columns"] == [{"column": "{{ ref('products', 'name') }}"}]  # type: ignore[index]
    assert body["config"] == {"warehouse": "WH"}  # type: ignore[index]


def test_a_search_query_on_can_not_hold_is_referenced_and_reported() -> None:
    filtered = _search(definition="SELECT name FROM DB.MARTS.PRODUCTS WHERE kind = 'food'")
    imported = import_tool(filtered, index(models=_MODELS), _PLACE, define=True)
    assert "relations" in (imported.files[0].document or {})["tool"]  # type: ignore[operator]
    assert [item.code for item in imported.diagnostics] == ["SST-IMP013"]


def test_a_defined_routine_keeps_its_body_in_a_file_with_model_relations_as_refs() -> None:
    routine = LiveTool(
        QualifiedName.parse("DB.T.ORDER_COUNT"),
        "procedure",
        "Counts orders.",
        {
            "body": "SELECT COUNT(*) FROM DB.MARTS.ORDERS JOIN OTHER.S.T ON TRUE",
            "language": "SQL",
            "returns": "NUMBER",
            "execute as": "OWNER",
        },
        (("LOCATION", "VARCHAR"),),
    )
    imported = import_tool(routine, index(models=_MODELS), _PLACE, define=True)
    yaml_file, body_file = imported.files
    assert body_file.path == "tools/sales/order_count/order_count.sql"
    assert body_file.text == "SELECT COUNT(*) FROM {{ ref('orders') }} JOIN OTHER.S.T ON TRUE\n"
    assert yaml_file.document == {
        "tool": {
            "name": "order_count",
            "type": "procedure",
            "description": "Counts orders.",
            "body": "{{ file('order_count.sql') }}",
            "language": "sql",
            "returns": "NUMBER",
            "config": {"execute_as": "owner"},
            "signature": [{"name": "location", "type": "VARCHAR"}],
        }
    }


_SKILLS = BundlePlacement("skills", Destination(("sales",)), "skills")


def test_a_skill_version_is_its_folder_copied_byte_for_byte() -> None:
    files = {"skills/check/SKILL.md": b"---\nname: check\n---\n", "skills/check/ref/a.md": b"\x00raw"}
    imported = import_skill(
        LiveBundle(QualifiedName.parse("DB.SK.CHECK"), files, source="DB.SK.CHECK"), index(), _SKILLS
    )
    assert [(file.path, file.data) for file in imported.files] == [
        ("skills/sales/check/SKILL.md", b"---\nname: check\n---\n"),
        ("skills/sales/check/ref/a.md", b"\x00raw"),
    ]
    assert imported.diagnostics == ()


def test_a_skill_the_project_publishes_or_a_version_of_two_skills_is_not_imported() -> None:
    files = {"skills/check/SKILL.md": b"x"}
    bundle = LiveBundle(QualifiedName.parse("DB.SK.CHECK"), files, source="DB.SK.CHECK")
    assert [
        item.code for item in import_skill(bundle, index(skill_names=frozenset({"check"})), _SKILLS).diagnostics
    ] == ["SST-IMP007"]
    two = LiveBundle(QualifiedName.parse("DB.SK.X"), {**files, "skills/other/SKILL.md": b"y"}, source="DB.SK.X")
    assert [item.code for item in import_skill(two, index(), _SKILLS).diagnostics] == ["SST-IMP013"]


def test_a_plugin_lists_its_members_and_writes_those_the_project_lacks() -> None:
    manifest = {"name": "toolkit", "description": "Tools.", "author": {"name": "Analytics"}, "skills": "./skills/"}
    files = {
        ".cortex-plugin/plugin.json": json.dumps(manifest).encode(),
        "skills/one/SKILL.md": b"1",
        "skills/two/SKILL.md": b"2",
    }
    bundle = LiveBundle(QualifiedName.parse("DB.SK.TOOLKIT"), files, manifest=manifest, source="DB.SK.TOOLKIT")
    imported = import_plugin(
        bundle, index(skill_names=frozenset({"one"})), BundlePlacement("plugins", Destination(()), "skills")
    )
    assert imported.files[0].path == "plugins/toolkit/plugin.yml"
    assert imported.files[0].document == {
        "name": "toolkit",
        "description": "Tools.",
        "owner_team": "Analytics",
        "skills": ["{{ skill('one') }}", "{{ skill('two') }}"],
    }
    assert [file.path for file in imported.files[1:]] == ["skills/two/SKILL.md"]


def test_an_eval_table_becomes_a_dataset_in_its_agent_folder() -> None:
    rows = (("Revenue?", {"ground_truth_output": "A figure."}), ("Weather?", None))
    imported = import_eval(QualifiedName.parse("DB.S.EVAL_SRC_BOT"), rows, "agents/sales/bot")
    assert imported.files[0].path == "agents/sales/bot/evals/eval_src_bot.yml"
    assert imported.files[0].document == {
        "name": "eval_src_bot",
        "questions": [
            {"question": "Revenue?", "ground_truth": {"ground_truth_output": "A figure."}},
            {"question": "Weather?"},
        ],
    }


def test_the_emitter_writes_blocks_templates_and_yaml_1_1_words_as_dbt_reads_them() -> None:
    text = emit_document(
        {"name": "x", "body": "one\ntwo", "ref": "{{ ref('orders') }}", "words": ["no", "yes", "plain"]}, "x.yml"
    )
    assert text == (
        "name: x\nbody: |-\n  one\n  two\nref: \"{{ ref('orders') }}\"\nwords:\n  - 'no'\n  - 'yes'\n  - plain\n"
    )


def test_model_edits_set_keys_where_enrich_would_and_keep_the_file_otherwise() -> None:
    text = (
        "version: 2\nmodels:\n  - name: orders\n    # kept\n    columns:\n      - name: id\n        description: Id.\n"
    )
    edit = ModelEdit(
        "orders",
        "models/orders.yml",
        meta=(("primary_key", ("id",)), ("unique_keys", (("a", "b"),))),
        description="Orders.",
        columns=(ColumnMeta("id", (("column_type", "dimension"),), "ignored, the project has one"),),
    )
    written = write_import_edits(text, "models/orders.yml", [edit]).text
    assert "# kept" in written
    assert "    description: Orders.\n" in written
    assert "      - name: id\n        description: Id.\n" in written
    assert "          primary_key:\n            - id\n          unique_keys:\n            - [a, b]\n" in written
    assert "              column_type: dimension\n" in written
