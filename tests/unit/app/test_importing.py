"""`ImportObjects.run`: a scope's listing, dependencies imported first, shared files, and extension reads."""

from __future__ import annotations

import copy

from snowflake_semantic_tools.cli.import_wizard import _picked
from snowflake_semantic_tools.domain.importing.references import Placed
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import ANALYST_SPEC, FakeImportPort, codes, dbt_model, index, run, setup, staged_agent

_MODELS = {("DB", "MARTS", "ORDERS"): "orders"}
_VIEW_DOCUMENT = {
    "name": "SALES",
    "tables": [{"name": "ORDERS", "base_table": {"database": "DB", "schema": "MARTS", "table": "ORDERS"}}],
}


def _view_on(port: FakeImportPort) -> None:
    port.show_rows["SEMANTIC VIEW DB.SV.SALES"] = {"name": "SALES", "comment": "Sales."}
    port.view_documents["DB.SV.SALES"] = _VIEW_DOCUMENT


def test_an_agents_view_is_imported_first_with_dependencies_and_its_report_resolves() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    _view_on(port)
    project = setup(index(models=_MODELS, dbt_models={"orders": dbt_model("orders", "DB.MARTS.ORDERS")}))
    report = run(port, "agent", "DB.S.SALES_BOT", project, with_dependencies=True)
    assert [item.artifact_key for item in report.objects] == ["semantic_view:sales", "agent:sales_bot"]
    assert "SST-IMP012" not in codes(report)
    assert report.objects[1].dependencies == ()


def test_two_agents_share_a_member_file_they_write_alike() -> None:
    spec = copy.deepcopy(ANALYST_SPEC)
    spec["tools"] = [{"tool_spec": {"type": "cortex_search", "name": "faq", "description": "FAQ."}}]
    spec["tool_resources"] = {"faq": {"search_service": "DB.T.FAQ"}}
    port = FakeImportPort()
    staged_agent(port, "DB.S.ONE", spec)
    staged_agent(port, "DB.S.TWO", spec)
    report = run(port, "agent", "DB.S")
    paths = [file.path for file in report.files]
    assert paths.count("tools/faq/faq.yml") == 1
    assert "SST-IMP004" not in codes(report)


def test_the_wizards_picks_narrow_a_schema_to_the_objects_named() -> None:
    port = FakeImportPort()
    for name in ("DB.S.ONE", "DB.S.TWO", "DB.S.THREE"):
        staged_agent(port, name)
    report = run(port, "agent", "DB.S", only=("ONE", "THREE"))
    assert [item.live_name.sql for item in report.objects] == ["DB.S.ONE", "DB.S.THREE"]
    assert _picked("1,3", 3) == (0, 2) and _picked("2-3", 3) == (1, 2) and _picked("all", 2) == (0, 1)
    assert _picked("4", 3) is None and _picked("x", 3) is None


def test_an_object_the_project_publishes_is_adopted_not_imported() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    published = Placed("agent", "sales_bot", QualifiedName.parse("DB.S.SALES_BOT"), (), "agents/sales_bot/agent.yml")
    [obj] = run(port, "agent", "DB.S.SALES_BOT", setup(index(published))).objects
    assert [item.code for item in obj.diagnostics] == ["SST-IMP007"]
    assert not obj.refused and not obj.files


def test_a_skill_extension_is_read_from_the_version_it_serves() -> None:
    port = FakeImportPort()
    location = "snow://cortex_extension/DB.SK.CHECK/versions/VERSION$1/"
    version = {
        "name": "VERSION$1",
        "alias": None,
        "location": location,
        "is_default": True,
        "certification": None,
        "files": ["skills/check/SKILL.md"],
        "sources": {"skills/check/SKILL.md": "@DB.SK.SRC/check/SKILL.md"},
    }
    port.extensions["DB.SK.CHECK"] = {"type": "SKILL", "comment": "Checks.", "versions": [version]}
    port.stage_file("@DB.SK.SRC/check/SKILL.md", b"---\nname: check\n---\n")
    report = run(port, "skill", "DB.SK.CHECK")
    assert [(file.path, file.data) for file in report.files] == [("skills/check/SKILL.md", b"---\nname: check\n---\n")]


def test_a_skill_is_read_from_a_stage_folder_and_reported_by_its_path() -> None:
    port = FakeImportPort()
    port.stage_file("@DB.SK.SRC/skills/ops/check/SKILL.md", b"x")
    report = run(port, "skill", "DB.SK.SRC", stage_path="@DB.SK.SRC/skills/ops/check/")
    [obj] = report.objects
    assert obj.shown == "@DB.SK.SRC/skills/ops/check/" and obj.artifact_key == "skill:check"
    assert [file.path for file in report.files] == ["skills/check/SKILL.md"]


def test_an_agent_tool_on_the_target_warehouse_keeps_it_since_an_agent_inherits_none() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    report = run(port, "agent", "DB.S.SALES_BOT")
    [agent] = [file.document for file in report.files if file.path == "agents/sales_bot/agent.yml"]
    assert agent is not None
    [tool] = agent["spec"]["tools"]  # type: ignore[index]
    assert tool["warehouse"] == "WH"
