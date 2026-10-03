"""`sst baseline`: add, prune, show and renew, and the refusals that keep it a migration tool."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.adapters.fs.baseline import baseline_text, read_baseline
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry, Renewal
from tests.helpers.cli_projects import common, project_copy

BASELINE = Path(".sst") / "baseline.json"


def _run(project: Path, *args: str) -> Result:
    return CliRunner().invoke(cli, ["baseline", *args, *common(project)])


def _file(project: Path) -> dict[str, Any]:
    document = json.loads((project / BASELINE).read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def test_the_file_round_trips_with_its_audit_record(tmp_path: Path) -> None:
    entry = BaselineEntry("ab", "SST-CFG003", "config:k", "sst_config.yml", "known")
    written = Baseline("b.json", "2027-01-01", (entry,), "2026-01-01T00:00:00Z", "1.0.0", (Renewal("a", "b", "c"),))
    path = tmp_path / "b.json"
    path.write_text(baseline_text(written), encoding="utf-8")
    assert read_baseline(path, "b.json") == written
    path.write_text('{"version": 1, "expires_on": "x", "entries": [], "renewals": [1, {"reason": "r"}]}')
    assert read_baseline(path, "b.json").renewals == (Renewal("", "r", ""),)
    path.write_text('{"version": 1, "expires_on": "x", "entries": [], "renewals": "no"}')
    assert read_baseline(path, "b.json").renewals == ()


def test_add_one_code_writes_a_versioned_expiring_file_then_validate_suppresses_it(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    added = _run(project, "add", "sst-cfg018", "--expires-in", "30", "--note", "tracked", "--output", "json")
    assert added.exit_code == 0, added.output
    data = json.loads(added.output)["data"]
    assert [item["code"] for item in data["added"]] == ["SST-CFG018"] * 2
    assert data["counts"] == {"SST-CFG018": 2}
    document = _file(project)
    assert (document["version"], document["generated_by_sst"]) == (1, document["generated_by_sst"])
    assert all(entry["note"] == "tracked" for entry in document["entries"])
    validated = CliRunner().invoke(cli, ["validate", *common(project), "--output", "json"])
    summary = json.loads(validated.output)["summary"]
    assert summary["baselined"] == 2
    again = _run(project, "add", "SST-CFG018", "--output", "json")
    assert json.loads(again.output)["data"]["added"] == []


def test_add_all_warnings_needs_yes_off_a_terminal_and_prompts_on_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    refused = _run(project, "add", "--all-warnings")
    assert refused.exit_code == 3
    assert "error[SST-PRT109]: sst baseline add --all-warnings requires --yes" in refused.output
    assert not (project / BASELINE).exists()
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.baseline._interactive", lambda: True)
    declined = CliRunner().invoke(cli, ["baseline", "add", "--all-warnings", *common(project)], input="n\n")
    assert declined.exit_code == 130
    accepted = CliRunner().invoke(cli, ["baseline", "add", "--all-warnings", *common(project)], input="y\n")
    assert accepted.exit_code == 0, accepted.output
    assert "baseline all 5 current warning(s)?" in accepted.output
    assert len(_file(project)["entries"]) == 5


def test_add_refuses_errors_unregistered_codes_and_bad_combinations(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    error = _run(project, "add", "SST-VAL116")
    assert error.exit_code == 1
    assert "SST-VAL116 is an error; a baseline suppresses and never demotes" in error.output
    assert _run(project, "add", "SST-NOPE01").exit_code == 3
    assert _run(project, "add").exit_code == 3
    both = _run(project, "add", "SST-CFG018", "--all-warnings", "--yes")
    assert (both.exit_code, "SST-PRT104" in both.output) == (3, True)
    assert _run(project, "add", "SST-CFG018", "--expires-in", "366").exit_code == 3


def test_prune_removes_what_no_longer_matches_within_the_selection(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert _run(project, "prune").exit_code == 1
    assert _run(project, "add", "--all-warnings", "--yes").exit_code == 0
    document = _file(project)
    stale = {"fingerprint": "0" * 16, "code": "SST-VAL528", "artifact": "agent:gone", "file": "", "note": ""}
    document["entries"] = [*document["entries"], stale]
    (project / BASELINE).write_text(json.dumps(document), encoding="utf-8")
    scoped = _run(project, "prune", "--select", "type:semantic_view", "--output", "json")
    assert json.loads(scoped.output)["data"]["pruned"] == []
    pruned = _run(project, "prune", "--output", "json")
    assert pruned.exit_code == 0, pruned.output
    assert [item["artifact"] for item in json.loads(pruned.output)["data"]["pruned"]] == ["agent:gone"]
    assert len(_file(project)["entries"]) == 5


def test_show_lists_entries_by_code_and_expiry(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert "no baseline at" in _run(project, "show").output
    assert _run(project, "add", "--all-warnings", "--yes").exit_code == 0
    shown = _run(project, "show", "--code", "sst-cfg018")
    assert shown.exit_code == 0
    assert shown.output.count("SST-CFG018 tool_group:jaffle_partner") == 2
    assert "SST-VAL528" not in shown.output
    assert json.loads(_run(project, "show", "--expired", "--output", "json").output)["data"]["entries"] == []
    document = _file(project)
    document["expires_on"] = "2020-01-01"
    (project / BASELINE).write_text(json.dumps(document), encoding="utf-8")
    lapsed = _run(project, "show", "--expired")
    assert "expired on 2020-01-01" in lapsed.output
    assert lapsed.output.count("SST-") == 5


def test_renew_needs_a_reason_records_it_and_caps_the_expiry(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert _run(project, "renew").exit_code == 3
    assert _run(project, "renew", "--reason", "x").exit_code == 1
    assert _run(project, "add", "SST-CFG018").exit_code == 0
    too_long = _run(project, "renew", "--reason", "x", "--expires-in", "400")
    assert (too_long.exit_code, "more than 365 days" in too_long.output) == (1, True)
    renewed = _run(project, "renew", "--reason", "descriptions tracked", "--expires-in", "10")
    assert renewed.exit_code == 0, renewed.output
    [renewal] = _file(project)["renewals"]
    assert renewal["reason"] == "descriptions tracked"
    assert renewal["expires_on"] == _file(project)["expires_on"]


def test_the_global_baseline_path_is_where_the_group_writes(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(
        cli, ["--baseline", "elsewhere.json", "baseline", "add", "SST-CFG018", *common(project)]
    )
    assert result.exit_code == 0, result.output
    assert (project / "elsewhere.json").is_file()
    assert not (project / BASELINE).exists()
