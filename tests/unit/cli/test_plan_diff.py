"""`sst plan` shows what each update changes member by member, writes it to a diff file, and `--no-diff` skips it."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.plan.definitions.tool import search_query
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.described_objects import described_search, described_view
from tests.helpers.published_target import PublishingSnowflake, publishing_port
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

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
    for item in compile_result(locate_project(project), "dev", DBT_MANIFEST).compiled:
        target = item.rendered_artifact.target.sql
        if isinstance(item, CompiledView):
            port.view_descriptions[target] = described_view(item.view)
        elif isinstance(item, CompiledAgent) and agents:
            alias = item.rendered_artifact.desired_alias
            port.agent_version_rows[target] = (AgentVersion("VERSION$1", alias, True, specification=item.payload),)
        elif isinstance(item, CompiledTool) and item.member.type == "cortex_search_service":
            described = described_search(item.member, search_query(item.rendered.ddl))
            port.descriptions[f"CORTEX SEARCH SERVICE {target}"] = described
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

    diff_path = project / "target" / "sst" / "plan.dev.diff"
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
    assert _change(data, "semantic_view:jaffle_menu")["diff_status"] == "not_compared"
    text = (project / "target" / "sst" / "plan.dev.diff").read_text(encoding="utf-8")
    assert text.splitlines()[1] == "# member diffs not read: --no-diff"


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
    assert "# semantic_view:jaffle_menu: not compared member by member" in written.read_text(encoding="utf-8")


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
    [change] = [item for item in data["changes"] if item["artifact_key"].startswith("tool:")]
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
