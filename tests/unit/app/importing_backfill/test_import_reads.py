"""`ImportObjects.run`: each type's reader when Snowflake refuses, hides, or half-answers a read."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, cast

import pytest

from snowflake_semantic_tools.app.importing import ImportScope
from snowflake_semantic_tools.domain.importing.references import Placed, ReferencedTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from tests.helpers.import_fakes import ANALYST_SPEC, FakeImportPort, codes, index, run, setup, staged_agent

_MARKER = "Owned. [sst:" + "a" * 64 + ":" + "b" * 64 + "]"


def _tool_body(document: Mapping[str, object] | None) -> dict[str, Any]:
    assert document is not None
    return cast(dict[str, Any], document["tool"])


def _context(report: Any, position: int = 0) -> dict[str, Any]:
    return dict(report.objects[0].diagnostics[position].context)


def _extension(port: FakeImportPort, name: str, kind: str, *, versions: list[dict[str, Any]] | None = None) -> None:
    location = f"snow://cortex_extension/{name}/versions/VERSION$1/"
    listed = [
        {
            "name": "VERSION$1",
            "alias": None,
            "location": location,
            "is_default": True,
            "certification": None,
            "files": ["SKILL.md"],
            "sources": {"SKILL.md": "@DB.SK.SRC/SKILL.md"},
        }
    ]
    port.extensions[name] = {"type": kind, "comment": "Checks.", "versions": listed if versions is None else versions}


# -- scopes -------------------------------------------------------------------------------------------


def test_a_scope_of_four_parts_is_refused() -> None:
    with pytest.raises(ValueError, match="expected DB, DB.SCHEMA or DB.SCHEMA.NAME"):
        ImportScope.parse("A.B.C.D")


def test_a_dot_inside_double_quotes_does_not_split_the_scope() -> None:
    scope = ImportScope.parse('DB."A.B"')
    assert scope.schema is not None and scope.schema.sql == '"A.B"'
    assert scope.name is None and scope.qualified is None


def test_a_database_whose_listing_is_refused_reports_the_database_unseen() -> None:
    port = FakeImportPort()
    port.refuse("show_objects_in")
    report = run(port, "agent", "DB")
    [diagnostic] = report.diagnostics
    assert diagnostic.code == "SST-IMP001"
    assert diagnostic.context["type"] == "database" and diagnostic.context["value"] == "DB"
    assert report.has_errors and not report.objects


def test_a_schema_whose_listing_is_refused_reports_the_schema_unseen() -> None:
    port = FakeImportPort()
    port.refuse("show_objects_in")
    [diagnostic] = run(port, "tool", "DB.S").diagnostics
    assert diagnostic.code == "SST-IMP001" and diagnostic.context["type"] == "schema"


# -- semantic views -----------------------------------------------------------------------------------


def test_a_view_that_is_not_shown_is_reported_unseen() -> None:
    report = run(FakeImportPort(), "semantic_view", "DB.SV.SALES")
    assert codes(report) == ["SST-IMP001"] and report.has_errors


def test_a_view_sst_owns_is_refused_as_owned() -> None:
    port = FakeImportPort()
    port.show_rows["SEMANTIC VIEW DB.SV.SALES"] = {"name": "SALES", "comment": _MARKER}
    assert codes(run(port, "semantic_view", "DB.SV.SALES")) == ["SST-IMP003"]


def test_a_view_the_project_publishes_is_adopted_not_imported() -> None:
    port = FakeImportPort()
    port.show_rows["SEMANTIC VIEW DB.SV.SALES"] = {"name": "SALES", "comment": "Sales."}
    placed = Placed("semantic_view", "sales", QualifiedName.parse("DB.SV.SALES"), (), "semantic_models/sales.yml")
    report = run(port, "semantic_view", "DB.SV.SALES", setup(index(placed)))
    assert codes(report) == ["SST-IMP007"]
    assert _context(report)["artifact"] == "semantic_view:sales"


def test_a_view_whose_yaml_read_is_refused_names_the_function() -> None:
    port = FakeImportPort()
    port.show_rows["SEMANTIC VIEW DB.SV.SALES"] = {"name": "SALES", "comment": ""}
    port.refuse("read_semantic_view_yaml")
    report = run(port, "semantic_view", "DB.SV.SALES")
    assert codes(report) == ["SST-IMP005"]
    assert _context(report)["step"] == "SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW"
    assert _context(report)["detail"] == "read_semantic_view_yaml refused"


def test_a_view_whose_show_is_refused_names_show() -> None:
    port = FakeImportPort()
    port.refuse("show_row")
    report = run(port, "semantic_view", "DB.SV.SALES")
    assert codes(report) == ["SST-IMP005"] and _context(report)["step"] == "SHOW SEMANTIC VIEWS"


def test_a_view_snowflake_returns_no_definition_for_is_refused() -> None:
    port = FakeImportPort()
    port.show_rows["SEMANTIC VIEW DB.SV.SALES"] = {"name": "SALES", "comment": ""}
    report = run(port, "semantic_view", "DB.SV.SALES")
    assert codes(report) == ["SST-IMP005"]
    assert "Snowflake returned no definition" in _context(report)["detail"]


# -- tools ---------------------------------------------------------------------------------------------


def _search_service(port: FakeImportPort, comment: str = "FAQ answers.") -> None:
    port.show_rows["CORTEX SEARCH SERVICE DB.T.FAQ"] = {"name": "FAQ", "comment": comment}
    port.descriptions["CORTEX SEARCH SERVICE DB.T.FAQ"] = {
        "search_column": "ANSWER",
        "attribute_columns": "TOPIC",
        "columns": "ANSWER,TOPIC",
    }


def test_a_tool_no_type_shows_is_reported_unseen() -> None:
    report = run(FakeImportPort(), "tool", "DB.T.FAQ")
    assert codes(report) == ["SST-IMP001"] and _context(report)["type"] == "tool"


def test_a_tool_sst_owns_is_refused_as_owned() -> None:
    port = FakeImportPort()
    _search_service(port, comment=_MARKER)
    assert codes(run(port, "tool", "DB.T.FAQ")) == ["SST-IMP003"]


def test_a_tool_a_member_references_is_adopted_under_that_member() -> None:
    port = FakeImportPort()
    _search_service(port)
    relation = QualifiedName.parse("DB.T.FAQ")
    project = setup(index(tools={relation.folded: ReferencedTool("Faq_Member", "cortex_search_service", relation)}))
    report = run(port, "tool", "DB.T.FAQ", project)
    assert codes(report) == ["SST-IMP007"]
    assert _context(report)["artifact"] == "tool:faq_member"


def test_a_tool_the_project_defines_is_adopted_under_its_artifact() -> None:
    port = FakeImportPort()
    _search_service(port)
    placed = Placed("tool", "faq", QualifiedName.parse("DB.T.FAQ"), (), "tools/faq/faq.yml")
    report = run(port, "tool", "DB.T.FAQ", setup(index(placed)))
    assert codes(report) == ["SST-IMP007"] and _context(report)["artifact"] == "tool:faq"


def test_a_search_service_is_referenced_with_its_columns() -> None:
    port = FakeImportPort()
    _search_service(port)
    report = run(port, "tool", "DB.T.FAQ")
    [file] = report.files
    assert file.path == "tools/faq/faq.yml"
    body = _tool_body(file.document)
    assert body["type"] == "cortex_search_service" and body["description"] == "FAQ answers."
    assert body["relations"] == {"dev": "DB.T.FAQ"}


def test_a_search_service_that_describes_nothing_is_still_referenced() -> None:
    port = FakeImportPort()
    port.show_rows["CORTEX SEARCH SERVICE DB.T.FAQ"] = {"name": "FAQ", "comment": ""}
    [file] = run(port, "tool", "DB.T.FAQ").files
    assert "description" not in _tool_body(file.document)


def test_a_procedure_is_described_by_its_argument_types_and_keeps_its_signature() -> None:
    port = FakeImportPort()
    port.show_rows["PROCEDURE DB.T.LOOKUP"] = {
        "name": "LOOKUP",
        "description": "Finds an order.",
        "arguments": "LOOKUP(VARCHAR) RETURN VARCHAR",
    }
    port.routine_descriptions["PROCEDURE DB.T.LOOKUP(VARCHAR)"] = {"signature": "(ORDER_ID VARCHAR)"}
    report = run(port, "tool", "DB.T.LOOKUP")
    assert port.routine_reads == ["PROCEDURE DB.T.LOOKUP(VARCHAR)"]
    [file] = report.files
    body = _tool_body(file.document)
    assert body["type"] == "procedure" and body["description"] == "Finds an order."
    assert body["signature"] == [{"name": "order_id", "type": "VARCHAR"}]


def test_a_routine_whose_describe_is_refused_names_its_describe() -> None:
    port = FakeImportPort()
    port.show_rows["FUNCTION DB.T.SCORE"] = {"name": "SCORE", "arguments": "SCORE() RETURN NUMBER"}
    report = run(port, "tool", "DB.T.SCORE")
    assert codes(report) == ["SST-IMP005"]
    assert _context(report)["step"] == "the function's DESCRIBE" and _context(report)["type"] == "tool"


def test_a_defined_tool_runs_on_the_targets_warehouse_and_names_where_it_lives() -> None:
    port = FakeImportPort()
    _search_service(port)
    report = run(port, "tool", "DB.T.FAQ", define=True, to_folder=("team",))
    [obj] = report.objects
    assert obj.files[0].path == "tools/team/faq/faq.yml"
    assert "SST-IMP014" in [item.code for item in obj.diagnostics]


def test_a_scope_of_tools_lists_every_tool_type() -> None:
    port = FakeImportPort()
    _search_service(port)
    port.show(ShowRow("FAQ", "DB", "T", "OWNER", "now", "", object_type="CORTEX SEARCH SERVICE"))
    report = run(port, "tool", "DB.T")
    assert [item.artifact_key for item in report.objects] == ["tool:faq"]


# -- extensions ----------------------------------------------------------------------------------------


def test_an_extension_that_is_not_shown_is_reported_unseen() -> None:
    report = run(FakeImportPort(), "plugin", "DB.SK.OPS")
    assert codes(report) == ["SST-IMP001"] and _context(report)["type"] == "plugin"


def test_a_plugin_named_alone_as_a_skill_is_refused_as_the_other_kind() -> None:
    port = FakeImportPort()
    _extension(port, "DB.SK.OPS", "PLUGIN")
    report = run(port, "skill", "DB.SK.OPS")
    assert codes(report) == ["SST-IMP002"]
    assert _context(report)["found"] == "plugin" and _context(report)["expected"] == "skill"


def test_a_scope_of_skills_passes_over_a_plugin() -> None:
    port = FakeImportPort()
    _extension(port, "DB.SK.OPS", "PLUGIN")
    port.show(ShowRow("OPS", "DB", "SK", "OWNER", "now", "", object_type="CORTEX EXTENSION"))
    report = run(port, "skill", "DB.SK")
    assert report.objects == () and [name.sql for name in report.skipped] == ["DB.SK.OPS"]


def test_an_extension_the_project_publishes_is_adopted() -> None:
    port = FakeImportPort()
    _extension(port, "DB.SK.CHECK", "SKILL")
    placed = Placed("skill", "check", QualifiedName.parse("DB.SK.CHECK"), (), "skills/check/SKILL.md")
    assert codes(run(port, "skill", "DB.SK.CHECK", setup(index(placed)))) == ["SST-IMP007"]


def test_an_extension_serving_no_version_is_reported_unseen() -> None:
    port = FakeImportPort()
    _extension(port, "DB.SK.CHECK", "SKILL", versions=[])
    report = run(port, "skill", "DB.SK.CHECK")
    assert codes(report) == ["SST-IMP001"] and _context(report)["type"] == "skill version"


def test_an_extension_whose_versions_are_refused_names_the_listing() -> None:
    port = FakeImportPort()
    _extension(port, "DB.SK.CHECK", "SKILL")
    port.refuse("extension_versions")
    report = run(port, "skill", "DB.SK.CHECK")
    assert codes(report) == ["SST-IMP005"] and _context(report)["step"] == "SHOW VERSIONS IN CORTEX EXTENSION"


def test_an_extension_file_that_cannot_be_read_refuses_the_get() -> None:
    port = FakeImportPort()
    _extension(port, "DB.SK.CHECK", "SKILL")
    report = run(port, "skill", "DB.SK.CHECK")
    assert codes(report) == ["SST-IMP005"]
    assert _context(report)["step"] == "GET" and "SKILL.md could not be read" in _context(report)["detail"]


def test_a_plugin_is_read_with_its_manifest() -> None:
    port = FakeImportPort()
    version = {
        "name": "VERSION$1",
        "alias": None,
        "location": "snow://cortex_extension/DB.SK.OPS/versions/VERSION$1/",
        "is_default": True,
        "certification": None,
        "files": [".cortex-plugin/plugin.json"],
        "sources": {".cortex-plugin/plugin.json": "@DB.SK.SRC/plugin.json"},
    }
    _extension(port, "DB.SK.OPS", "PLUGIN", versions=[version])
    port.stage_file("@DB.SK.SRC/plugin.json", b'{"name": "ops"}')
    report = run(port, "plugin", "DB.SK.OPS")
    assert [item.artifact_key for item in report.objects] == ["plugin:ops"]
    assert not report.has_errors and report.files


# -- stage skills --------------------------------------------------------------------------------------


def test_a_stage_skill_whose_listing_is_refused_is_reported_by_its_path() -> None:
    port = FakeImportPort()
    port.refuse("list_location")
    [obj] = run(port, "skill", "DB.SK.SRC", stage_path="@DB.SK.SRC/check").objects
    assert [item.code for item in obj.diagnostics] == ["SST-IMP005"]
    assert obj.artifact_key == "skill:check" and obj.shown == "@DB.SK.SRC/check/"
    assert obj.diagnostics[0].context["step"] == "LIST"


def test_an_empty_stage_path_is_reported_unseen() -> None:
    [obj] = run(FakeImportPort(), "skill", "DB.SK.SRC", stage_path="@DB.SK.SRC/check/").objects
    assert [item.code for item in obj.diagnostics] == ["SST-IMP001"]
    assert obj.diagnostics[0].context["type"] == "stage path"


def test_a_stage_path_that_names_no_stage_still_reads_its_folder() -> None:
    port = FakeImportPort()
    port.stage_file("@nostage/check/SKILL.md", b"---\nname: check\n---\n")
    [obj] = run(port, "skill", "DB.SK.SRC", stage_path="@nostage/check/").objects
    assert obj.live_name.sql == "UNKNOWN.UNKNOWN.UNKNOWN"
    assert [file.path for file in obj.files] == ["skills/check/SKILL.md"]


def test_a_stage_skill_is_read_with_its_dependencies_too() -> None:
    port = FakeImportPort()
    port.stage_file("@DB.SK.SRC/check/SKILL.md", b"x")
    report = run(port, "skill", "DB.SK.SRC", stage_path="@DB.SK.SRC/check/", with_dependencies=True)
    assert [item.artifact_key for item in report.objects] == ["skill:check"]


# -- evals ---------------------------------------------------------------------------------------------


def test_an_eval_table_is_mapped_into_the_to_agents_folder() -> None:
    port = FakeImportPort()
    port.eval_tables["DB.EV.CASES"] = (("Revenue?", {"answer": "1"}),)
    report = run(port, "eval", "DB.EV.CASES", to_folder=("sales_bot",))
    [file] = report.files
    assert file.path == "agents/sales_bot/evals/cases.yml"
    assert file.document == {"name": "cases", "questions": [{"question": "Revenue?", "ground_truth": {"answer": "1"}}]}


def test_an_eval_table_that_is_not_there_is_reported_unseen() -> None:
    report = run(FakeImportPort(), "eval", "DB.EV.CASES")
    assert codes(report) == ["SST-IMP001"] and _context(report)["type"] == "table"


def test_an_eval_table_whose_select_is_refused_names_the_select() -> None:
    port = FakeImportPort()
    port.refuse("eval_rows")
    report = run(port, "eval", "DB.EV.CASES")
    assert codes(report) == ["SST-IMP005"] and _context(report)["step"] == "SELECT"


# -- agents --------------------------------------------------------------------------------------------


def test_an_agent_version_asked_for_that_is_not_listed_is_reported_unseen() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    report = run(port, "agent", "DB.S.SALES_BOT", version="VERSION$9")
    assert codes(report) == ["SST-IMP001"]
    assert _context(report)["value"] == "DB.S.SALES_BOT version VERSION$9"


def test_an_agent_with_no_versions_is_reported_unseen_by_name() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    port.agent_version_rows["DB.S.SALES_BOT"] = ()
    report = run(port, "agent", "DB.S.SALES_BOT")
    assert codes(report) == ["SST-IMP001"] and _context(report)["value"] == "DB.S.SALES_BOT"


@pytest.mark.parametrize(
    ("specification", "detail"),
    [
        (None, "the role may not read the specification of the version it serves"),
        ("{not json", "not JSON"),
        ("[1]", "not a JSON object"),
    ],
)
def test_an_agent_spec_that_cannot_be_read_is_refused(specification: str | None, detail: str) -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    port.agent_version_rows["DB.S.SALES_BOT"] = (AgentVersion("VERSION$1", None, True, None, specification),)
    report = run(port, "agent", "DB.S.SALES_BOT")
    assert codes(report) == ["SST-IMP005"]
    assert _context(report)["step"] == "agent_spec" and _context(report)["detail"].startswith(detail)


# -- dependencies --------------------------------------------------------------------------------------


def _with_skill(port: FakeImportPort, name: str, path: str, source_type: str = "STAGE") -> None:
    spec = copy.deepcopy(ANALYST_SPEC)
    spec["skills"] = [{"name": "check", "source": {"type": source_type, "path": path}}]
    staged_agent(port, name, spec)


def test_a_stage_skill_an_agent_loads_is_read_from_its_stage_path() -> None:
    port = FakeImportPort()
    port.stage_file("@DB.SK.SRC/check/SKILL.md", b"---\nname: check\n---\n")
    port.show_rows["SEMANTIC VIEW DB.SV.SALES"] = {"name": "SALES", "comment": ""}
    _with_skill(port, "DB.S.SALES_BOT", "@DB.SK.SRC/check/")
    report = run(port, "agent", "DB.S.SALES_BOT", with_dependencies=True)
    assert [item.artifact_key for item in report.objects] == ["skill:check", "semantic_view:sales", "agent:sales_bot"]
    # The view could not be read, so its report stays; the skill was imported, so its report goes.
    [agent] = [item for item in report.objects if item.artifact_key == "agent:sales_bot"]
    assert [dependency.name for dependency in agent.dependencies] == ["DB.SV.SALES"]


def test_a_dependency_whose_name_does_not_parse_is_left_named() -> None:
    port = FakeImportPort()
    port.view_documents["DB.SV.SALES"] = {}
    _with_skill(port, "DB.S.SALES_BOT", "skills/check/")
    report = run(port, "agent", "DB.S.SALES_BOT", with_dependencies=True)
    [agent] = [item for item in report.objects if item.artifact_key == "agent:sales_bot"]
    assert "skills/check/" in [dependency.name for dependency in agent.dependencies]


def test_two_agents_naming_one_view_read_it_once() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.ONE")
    staged_agent(port, "DB.S.TWO")
    report = run(port, "agent", "DB.S", with_dependencies=True)
    keys = [item.artifact_key for item in report.objects]
    assert keys.count("semantic_view:sales") == 1
    assert keys[-2:] == ["agent:one", "agent:two"]
