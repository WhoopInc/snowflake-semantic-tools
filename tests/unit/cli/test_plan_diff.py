"""`sst plan` shows what each update changes member by member, writes it to a diff file, and `--no-diff` skips it."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.published_target import PublishingSnowflake, describe_published, publishing_port
from tests.helpers.reference_project import project_copy

_METRIC = ("semantic_models/metrics/line_item_count.yml", "COUNT({{", "COUNT(DISTINCT {{")


def _published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, edit: tuple[str, str, str] = _METRIC, *, agents: bool = False
) -> tuple[Path, PublishingSnowflake]:
    """The reference project applied to a fake target that describes each object as published, then one edit made.

    Each view and search service DESCRIBEs as it was published. With `agents`, each agent serves
    one version, the default, holding the specification published; the fake then reads each
    agent's versions as drifted, so every agent plans an update.
    """
    project = project_copy(tmp_path)
    compile_project(project)
    port = publishing_port(project)
    applied = invoke_with_port(monkeypatch, port, ["apply", *common(project), "--target", "dev", "--yes"])
    assert applied.exit_code == 0, applied.output
    describe_published(port, project, "dev", agents=agents)
    relative, before, after = edit
    path = project / relative
    text = path.read_text(encoding="utf-8")
    assert before in text
    path.write_text(text.replace(before, after, 1), encoding="utf-8")
    compile_project(project)
    return project, port


def _plan(monkeypatch: pytest.MonkeyPatch, port: PublishingSnowflake, project: Path, *flags: str) -> dict[str, Any]:
    result = invoke_with_port(
        monkeypatch, port, ["plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out", *flags]
    )
    assert result.exit_code == 2, result.output
    data: dict[str, Any] = json.loads(result.output)["data"]
    return data


def _change(data: dict[str, Any], key: str) -> dict[str, Any]:
    found: dict[str, Any] = next(item for item in data["changes"] if item["artifact_key"] == key)
    return found


def test_an_update_lists_the_members_it_changes_in_json_and_in_the_diff_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    data = _plan(monkeypatch, port, project)
    change = _change(data, "semantic_view:jaffle_menu")
    assert change["diff_status"] == "compared"
    [member] = change["diff"]
    assert (member["kind"], member["op"]) == ("metric", "changed")
    assert member["member"].endswith("line_item_count")
    [prop] = member["properties"]
    assert prop["property"] == "expression"
    assert prop["before"].startswith("COUNT(") and prop["after"].startswith("COUNT(DISTINCT ")
    # Every other change is not an update, so none is compared.
    assert {item["diff_status"] for item in data["changes"] if item is not change} == {"not_compared"}

    diff_path = project / "sst_target" / "plan.dev.diff"
    assert data["diff_path"] == str(diff_path)
    text = diff_path.read_text(encoding="utf-8")
    assert text.startswith("# sst plan: target dev (")
    assert "--- live/semantic_view:jaffle_menu  " in text
    assert "+++ desired/semantic_view:jaffle_menu\n" in text
    assert f"@@ changed metric {member['member']} @@\n   expression:\n-  COUNT(" in text
    assert "\n+  COUNT(DISTINCT " in text


def test_the_plan_a_person_reads_shows_each_member_under_its_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    result = invoke_with_port(monkeypatch, port, ["plan", *common(project), "--target", "dev", "--no-plan-out"])
    assert result.exit_code == 2, result.output
    lines = result.output.splitlines()
    row = next(index for index, line in enumerate(lines) if "semantic_view:jaffle_menu" in line)
    assert "metric " in lines[row] and "definition" not in lines[row]
    assert lines[row + 1].startswith("      ~ metric ") and lines[row + 1].endswith("line_item_count")
    assert lines[row + 2] == "          expression:"
    assert lines[row + 3].startswith("            - COUNT(")
    assert lines[row + 4].startswith("            + COUNT(DISTINCT ")


def test_agent_output_names_the_members_an_update_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, port = _published(tmp_path, monkeypatch)
    result = invoke_with_port(
        monkeypatch, port, ["plan", *common(project), "--target", "dev", "--output", "agent", "--no-plan-out"]
    )
    line = next(line for line in result.output.splitlines() if line.startswith("~ semantic_view:jaffle_menu "))
    assert " changes=metric:" in line and line.endswith("line_item_count~")


def test_no_diff_reads_no_live_definition_and_says_so_in_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    port.fail("describe_semantic_view", AssertionError("--no-diff must not describe a view"))
    data = _plan(monkeypatch, port, project, "--no-diff")
    change = _change(data, "semantic_view:jaffle_menu")
    # What apply published recorded each member's fingerprint, so the one edited is still named.
    assert (change["diff_status"], change["changed_members"]) == ("from_state", ["metric:order_items.line_item_count"])
    text = (project / "sst_target" / "plan.dev.diff").read_text(encoding="utf-8")
    assert text.splitlines()[1:] == [
        "# member diffs not read: --no-diff",
        "# semantic_view:jaffle_menu: not read; the fingerprints state records name metric:order_items.line_item_count",
    ]


def test_a_plan_that_reads_no_definition_names_the_changed_member_under_its_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    result = invoke_with_port(
        monkeypatch, port, ["plan", *common(project), "--target", "dev", "--no-diff", "--no-plan-out", "-o", "plain"]
    )
    assert result.exit_code == 2, result.output
    lines = result.output.splitlines()
    row = next(index for index, line in enumerate(lines) if "semantic_view:jaffle_menu" in line and "update" in line)
    assert lines[row + 1 : row + 3] == [
        "      ~ metric order_items.line_item_count",
        "      (named from the fingerprints state records; not read)",
    ]


def test_a_refused_describe_leaves_the_update_unavailable_with_a_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    port.fail("describe_semantic_view", SnowflakePortError("insufficient privileges"))
    result = invoke_with_port(
        monkeypatch,
        port,
        [
            *("plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out"),
            *("--diff-out", str(project / "out" / "p.diff")),
        ],
    )
    payload = json.loads(result.output)
    assert result.exit_code == 2, result.output
    assert _change(payload["data"], "semantic_view:jaffle_menu")["diff_status"] == "unavailable"
    assert [item["code"] for item in payload["diagnostics"] if item["code"] == "SST-PLN041"] == ["SST-PLN041"]
    written = Path(payload["data"]["diff_path"])
    assert written.name == "p.diff"
    assert (
        "# semantic_view:jaffle_menu: not read; the fingerprints state records name metric:order_items.line_item_count"
        in written.read_text(encoding="utf-8")
    )


def test_a_diff_path_with_no_diff_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch,
        publishing_port_for(project),
        ["plan", *common(project), "--target", "dev", "--no-diff", "--diff-out", "x.diff", "--output", "json"],
    )
    assert result.exit_code == 3, result.output
    assert "SST-PRT104" in result.output


def publishing_port_for(project: Path) -> PublishingSnowflake:
    compile_project(project)
    return publishing_port(project)


def test_an_agent_update_names_the_instruction_it_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    edit = (
        "agents/jaffle_analytics/instructions/orchestration.md",
        "## Tool selection\n",
        "## Tool selection\n\nPrefer the semantic views over the documents.\n",
    )
    project, port = _published(tmp_path, monkeypatch, edit, agents=True)
    change = _change(_plan(monkeypatch, port, project), "agent:jaffle_analytics_agent")
    assert change["diff_status"] == "compared"
    [member] = change["diff"]
    assert (member["kind"], member["member"], member["op"]) == ("instruction", "orchestration", "changed")
    [prop] = member["properties"]
    assert "Prefer the semantic views over the documents." in prop["after"]
    assert "Prefer the semantic views over the documents." not in prop["before"]


def test_a_search_service_update_names_the_setting_it_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    edit = ("tools/platform/menu_docs_search.yml", "target_lag: 1 hour", "target_lag: 2 hours")
    project, port = _published(tmp_path, monkeypatch, edit)
    data = _plan(monkeypatch, port, project)
    change = _change(data, "tool:menu_docs_search")
    [member] = change["diff"]
    assert (member["kind"], member["op"]) == ("search_service", "changed")
    assert [(prop["property"], prop["after"]) for prop in member["properties"]] == [("target_lag", "2 hours")]


def test_an_agent_whose_served_specification_cannot_be_read_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    edit = ("agents/jaffle_analytics/instructions/orchestration.md", "## Tool selection\n", "## Tool choice\n")
    project, port = _published(tmp_path, monkeypatch, edit, agents=True)
    for target, rows in port.agent_version_rows.items():
        port.agent_version_rows[target] = tuple(dataclasses.replace(row, specification=None) for row in rows)
    result = invoke_with_port(
        monkeypatch, port, ["plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out"]
    )
    payload = json.loads(result.output)
    assert _change(payload["data"], "agent:jaffle_analytics_agent")["diff_status"] == "unavailable"
    notes = [item for item in payload["diagnostics"] if item["code"] == "SST-PLN041"]
    [note] = [item for item in notes if "JAFFLE_ANALYTICS_AGENT" in item["message"].upper()]
    assert "the role may not read the specification of the version the agent serves" in note["message"]


def test_a_skill_edit_shows_as_the_file_it_changes_in_the_skill_its_plugin_and_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    edit = (
        "skills/jaffle-semantics/SKILL.md",
        "# Jaffle semantics\n",
        "# Jaffle semantics\n\nRead the join paths first.\n",
    )
    project, port = _published(tmp_path, monkeypatch, edit)
    data = _plan(monkeypatch, port, project)
    changed = {
        item["artifact_key"]: [(member["kind"], member["member"], member["op"]) for member in item["diff"]]
        for item in data["changes"]
        if item["artifact_type"] in ("skill", "plugin")
        if item["action"] == "update"
    }
    assert changed == {
        "skill:jaffle-semantics": [("file", "skills/jaffle-semantics/SKILL.md", "changed")],
        "plugin:jaffle-toolkit": [("file", "skills/jaffle-semantics/SKILL.md", "changed")],
    }
    # Each Cortex Code profile pins the skill, itself or through the plugin, so it adds a version.
    assert {
        item["artifact_key"]: item["action"] for item in data["changes"] if item["artifact_type"] == "coco_profile"
    } == {"coco_profile:jaffle_analyst": "update", "coco_profile:jaffle_operator": "update"}
    skill = _change(data, "skill:jaffle-semantics")
    [prop] = skill["diff"][0]["properties"]
    assert "Read the join paths first." in prop["after"] and "Read the join paths first." not in prop["before"]


def test_an_eval_edit_shows_as_the_question_it_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    asked = "in each location?"
    edit = ("agents/jaffle_analytics/evals/dataset.yml", asked, "in every location?")
    project, port = _published(tmp_path, monkeypatch, edit)
    change = _change(_plan(monkeypatch, port, project), "eval:jaffle_analytics_agent")
    assert change["diff_status"] == "compared"
    # The config names the new dataset; a question is matched by its text, so an edited one is
    # one added and one removed.
    assert [(member["kind"], member["op"]) for member in change["diff"]] == [
        ("config", "changed"),
        ("question", "added"),
        ("question", "removed"),
    ]
    [config] = change["diff"][0]["properties"]
    assert "dataset_name:" in config["before"] and config["before"] != config["after"]
    assert change["diff"][1]["member"].endswith("in every location?")
    assert change["diff"][2]["member"].endswith(asked)


def _two_targets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict[str, PublishingSnowflake]]:
    """The reference project applied to dev and to prod, each its own fake account, every view described."""
    project = project_copy(tmp_path)
    ports: dict[str, PublishingSnowflake] = {}
    for target in ("dev", "prod"):
        CliRunner().invoke(cli, ["compile", *common(project), "--target", target])
        port = ports[target] = publishing_port(project, target)
        applied = invoke_with_port(monkeypatch, port, ["apply", *common(project), "--target", target, "--yes"])
        assert applied.exit_code == 0, applied.output
        describe_published(port, project, target, agents=True)
    by_database = {"SST_REF_DEV": ports["dev"], "SST_REF_PROD": ports["prod"]}
    for port in ports.values():
        port.close = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setattr(
        "snowflake_semantic_tools.cli.main.SnowflakeConnector",
        lambda params: by_database[str(params["database"]).upper()],
    )
    return project, ports


def _diff_targets(project: Path, *flags: str) -> tuple[int, dict[str, Any]]:
    result = CliRunner().invoke(cli, ["diff", *common(project), "--from", "dev", "--to", "prod", "-o", "json", *flags])
    return result.exit_code, json.loads(result.output)["data"]


def test_two_targets_that_publish_one_project_to_different_databases_agree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, _ = _two_targets(tmp_path, monkeypatch)
    code, data = _diff_targets(project)
    # Every fingerprint differs, the databases being baked in; no member does once names are labels.
    assert (code, data["differences"]) == (0, [])
    # Fingerprints alone call every view modified.
    code, data = _diff_targets(project, "--no-diff")
    assert code == 2
    assert {item["status"] for item in data["differences"] if item["artifact_type"] == "semantic_view"} == {"modified"}


def test_one_member_edited_on_one_target_is_the_one_difference(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, ports = _two_targets(tmp_path, monkeypatch)
    prod = ports["prod"]
    target, rows = next((name, rows) for name, rows in prod.view_descriptions.items() if "JAFFLE_MENU" in name)
    at = next(index for index, row in enumerate(rows) if (row.kind, row.property) == ("METRIC", "COMMENT"))
    prod.view_descriptions[target] = (
        *rows[:at],
        dataclasses.replace(rows[at], value="Edited in prod."),
        *rows[at + 1 :],
    )
    code, data = _diff_targets(project, "--diff-out", str(project / "dev-prod.diff"))
    [view] = [item for item in data["differences"] if item["artifact_type"] == "semantic_view"]
    assert (code, view["key"], view["diff_status"]) == (2, "semantic_view:jaffle_menu", "compared")
    [member] = view["diff"]
    assert (member["kind"], member["op"], [prop["property"] for prop in member["properties"]]) == (
        "metric",
        "changed",
        ["comment"],
    )
    assert member["properties"][0]["after"] == "Edited in prod."
    text = (project / "dev-prod.diff").read_text(encoding="utf-8")
    assert text.startswith("--- dev/semantic_view:jaffle_menu\n+++ prod/semantic_view:jaffle_menu\n")


def test_local_against_its_target_shows_each_member_and_a_refused_read_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    human = invoke_with_port(monkeypatch, port, ["diff", *common(project), "--to", "dev"])
    assert human.exit_code == 2, human.output
    row = next(line for line in human.output.splitlines() if line.startswith("modified  semantic_view:jaffle_menu"))
    assert row.endswith("metric order_items.line_item_count")
    assert "          expression:" in human.output
    port.fail("describe_semantic_view", SnowflakePortError("denied"))
    data = json.loads(
        invoke_with_port(monkeypatch, port, ["diff", *common(project), "--to", "dev", "-o", "json"]).output
    )
    view = next(item for item in data["data"]["differences"] if item["key"] == "semantic_view:jaffle_menu")
    assert (view["status"], view["diff_status"], view["diff_unavailable"]) == ("modified", "unavailable", "denied")
    fallback = invoke_with_port(monkeypatch, port, ["diff", *common(project), "--to", "dev"])
    assert "semantic_view:jaffle_menu  (not compared member by member: denied)" in fallback.output


def test_diff_refuses_a_diff_path_with_no_diff(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["diff", *common(project), "--no-diff", "--diff-out", "x.diff"])
    assert result.exit_code == 3
    assert "SST-PRT104" in result.output


def test_the_sql_directory_holds_this_plans_statements_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 2026-10-18-plan-sql-dir-keeps-stale-files: an earlier plan's files used to stay.
    project, port = _published(tmp_path, monkeypatch)
    sql = project / "sst_target" / "sql"
    sql.mkdir(parents=True, exist_ok=True)
    (sql / "agent__gone.json").write_text("{}", encoding="utf-8")
    (sql / "skill__gone.json").write_text("{}", encoding="utf-8")
    (sql / "notes.md").write_text("mine", encoding="utf-8")
    (sql / "widget__mine.sql").write_text("mine", encoding="utf-8")
    elsewhere = tmp_path / "elsewhere.sql"
    elsewhere.write_text("kept", encoding="utf-8")
    (sql / "tool__linked.sql").symlink_to(elsewhere)
    _plan(monkeypatch, port, project)
    assert sorted(path.name for path in sql.iterdir()) == [
        "notes.md",
        "semantic_view__jaffle_menu.sql",
        "tool__linked.sql",
        "widget__mine.sql",
    ]
    assert elsewhere.read_text(encoding="utf-8") == "kept"


def test_a_side_that_does_not_compile_is_compared_by_fingerprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)

    def refused(*_args: object) -> None:
        raise ProjectError("the dbt manifest cannot be read", diagnostics=())

    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.diff.compile_result", refused)
    result = invoke_with_port(monkeypatch, port, ["diff", *common(project), "--to", "dev", "-o", "json"])
    assert result.exit_code == 2, result.output
    view = next(
        item for item in json.loads(result.output)["data"]["differences"] if item["key"] == "semantic_view:jaffle_menu"
    )
    assert (view["status"], view["diff_status"], view["diff"]) == ("modified", "not_compared", [])
