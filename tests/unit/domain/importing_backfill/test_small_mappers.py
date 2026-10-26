"""Edge cases of the small import mappers: evals, placement, the project index, skills and plugins."""

from __future__ import annotations

from snowflake_semantic_tools.domain.importing.evals import import_eval
from snowflake_semantic_tools.domain.importing.placement import Destination, destination, placement
from snowflake_semantic_tools.domain.importing.references import Placed, ReferencedTool
from snowflake_semantic_tools.domain.importing.skill import BundlePlacement, LiveBundle, import_plugin, import_skill
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import TARGET, index

_PLACE = BundlePlacement("plugins", Destination(("team",)), "skills")


def test_an_eval_row_whose_ground_truth_is_not_an_object_is_written_without_one_and_reported() -> None:
    rows = (("Revenue?", {"ground_truth_output": "42"}), ("Orders?", "not json"), ("Users?", None))
    imported = import_eval(QualifiedName.parse("DB.S.SALES_EVAL"), rows, "agents/sales")
    assert imported.files[0].path == "agents/sales/evals/sales_eval.yml"
    assert imported.files[0].document == {
        "name": "sales_eval",
        "questions": [
            {"question": "Revenue?", "ground_truth": {"ground_truth_output": "42"}},
            {"question": "Orders?"},
            {"question": "Users?"},
        ],
    }
    assert [item.code for item in imported.diagnostics] == ["SST-IMP013", "SST-IMP013"]
    assert "1 row(s) hold a ground truth that is not a JSON object" in imported.diagnostics[1].message


def test_a_used_folder_that_publishes_elsewhere_is_passed_over_for_a_route_that_matches() -> None:
    routes = {"sales": {"+database": "SALES_DB"}, "cx": {"+database": "CX_DB"}}
    live = QualifiedName.parse("CX_DB.S.BOT")
    assert destination(routes, TARGET, live, (("sales",),)) == Destination(("cx",))


def test_a_rendered_location_that_is_not_an_identifier_is_upper_cased_as_written() -> None:
    assert placement({"+database": "my db"}, (), TARGET) == ("MY DB", "S")


def test_the_index_finds_a_tool_the_project_defines_before_one_it_references() -> None:
    relation = QualifiedName.parse("DB.T.MENU")
    project = index(
        Placed("tool", "menu_defined", relation, ()),
        tools={relation.folded: ReferencedTool("menu_referenced", "cortex_search_service", relation)},
    )
    assert project.tool(relation) == "menu_defined"
    assert index().tool(relation) is None


def test_the_index_lists_the_folders_publishing_in_a_schema_most_used_first() -> None:
    def placed(name: str, where: str, folder: tuple[str, ...]) -> Placed:
        return Placed("agent", name, QualifiedName.parse(where), folder)

    project = index(
        placed("a", "DB.S.A", ("one",)),
        placed("b", "DB.S.B", ("two",)),
        placed("c", "DB.S.C", ("two",)),
        placed("d", "DB.OTHER.D", ("three",)),
        Placed("tool", "e", QualifiedName.parse("DB.S.E"), ("four",)),
    )
    assert project.folders("agent", "DB", "S") == (("two",), ("one",))


def test_a_plugin_the_project_already_publishes_is_skipped_with_nothing_written() -> None:
    bundle = LiveBundle(QualifiedName.parse("DB.S.HELPERS"), {}, manifest={"name": "helpers"}, source="DB.S.HELPERS")
    imported = import_plugin(bundle, index(Placed("plugin", "Helpers", QualifiedName.parse("DB.S.X"), ())), _PLACE)
    assert imported.files == ()
    assert [item.code for item in imported.diagnostics] == ["SST-IMP007"]
    assert imported.diagnostics[0].context["value"] == "DB.S.HELPERS"


def test_a_plugin_without_a_description_or_named_author_writes_only_its_name_and_skills() -> None:
    files = {"skills/lookup/SKILL.md": b"# Lookup\n"}
    bundle = LiveBundle(
        QualifiedName.parse("DB.S.HELPERS"), files, manifest={"author": "someone"}, source="DB.S.HELPERS"
    )
    imported = import_plugin(bundle, index(), _PLACE)
    assert imported.files[0].document == {"name": "helpers", "skills": ["{{ skill('lookup') }}"]}
    assert imported.diagnostics == ()
    assert [file.path for file in imported.files] == ["plugins/team/helpers/plugin.yml", "skills/team/lookup/SKILL.md"]


def test_a_skill_whose_extension_name_differs_from_its_folder_says_which_name_it_publishes_as() -> None:
    files = {"skills/order-lookup/SKILL.md": b"# Lookup\n"}
    bundle = LiveBundle(QualifiedName.parse("DB.S.LEGACY_LOOKUP"), files, comment="Old.", source="DB.S.LEGACY_LOOKUP")
    imported = import_skill(bundle, index(), _PLACE)
    assert imported.files[0].data == b"# Lookup\n"
    assert [item.code for item in imported.diagnostics] == ["SST-IMP013"]
    assert "publishes as ORDER_LOOKUP" in imported.diagnostics[0].message
    assert "not as the live LEGACY_LOOKUP" in imported.diagnostics[0].message
