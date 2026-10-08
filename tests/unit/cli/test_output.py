"""What `sst` prints: the JSON envelope, and human output across commands."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools import __version__
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.output import (
    RenderPolicy,
    counted,
    json_envelope,
    render_diagnostics,
    resolve_invocation,
    use_render_policy,
)
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def test_validate_json_emits_one_v2_envelope(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    result = CliRunner().invoke(
        cli,
        [
            "validate",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--output",
            "json",
            "--no-strict",
            "--no-snowflake-syntax-check",
        ],
    )
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["tool"] == "sst"
    assert envelope["schema_version"] == 2
    assert envelope["sst_version"] == __version__
    assert envelope["invocation"]["argv"][1] == "validate"
    assert envelope["invocation"]["project_dir"] == str(project.resolve())
    assert envelope["invocation"]["config_file"] == str((project / "sst_config.yml").resolve())
    assert envelope["invocation"]["started_at"]
    assert envelope["invocation"]["duration_s"] >= 0
    assert envelope["status"] == "ok"
    # Infos: SST-VAL854 (the fixture's profile registry is not Desktop's), SST-VAL020 once per
    # connected rule (4), the eval notes SST-VAL711/712/725, the skill notes SST-VAL816 (3) and
    # SST-VAL831 (4), the dbt seam's notes SST-DBT016 for orders and products and SST-DBT025,
    # eighteen that report
    # what attached where (SST-MEM011 for each of the fifteen members both jaffle views hold,
    # SST-MEM103 for each of the three views), and the compiled views' shape: SST-VAL125 (9),
    # SST-VAL216, SST-VAL217 (2) and SST-VAL319 (3). The warnings are SST-CFG034, as --no-strict
    # contradicts the fixture's `validation.strict: true`; SST-RND013 for the generic tool's
    # resources; SST-VAL528; SST-RND010 for the minimal agent's empty tool list; and SST-CFG018
    # for each partner tool member nothing references (the delivery agent's toolset names the
    # third). The plugin has a consumer, the operator profile.
    assert [item["code"] for item in envelope["diagnostics"] if item["severity"] == "warning"] == [
        "SST-CFG034",
        "SST-RND013",
        "SST-VAL528",
        "SST-RND010",
        "SST-CFG018",
        "SST-CFG018",
    ]
    assert envelope["summary"] == {
        "error": 0,
        "warning": 6,
        "info": 51,
        "promoted": 0,
        "suppressed_cascade": 0,
        "baselined": 0,
    }


def _promoted_warning(name: str = "lookup") -> Diagnostic:
    found = D("SST-CFG018", subject="tool_group:partner", group="partner", name=name)
    return dataclasses.replace(found, severity=Severity.ERROR)


def _summary_line(tmp_path: Path, capsys: pytest.CaptureFixture[str], *found: Diagnostic, held: int = 0) -> str:
    """Render `found` with its first `held` diagnostics baselined, and return the counts line."""
    baselined = frozenset(stable_fingerprint(item) for item in found[:held])
    resolve_invocation(project_dir=tmp_path, config_file=None, target=None, baselined=baselined)
    use_render_policy(RenderPolicy())
    capsys.readouterr()
    render_diagnostics(found)
    return capsys.readouterr().err.splitlines()[-1]


def test_one_baselined_promoted_warning_is_counted_apart_and_counts_are_pluralised(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    promoted = _promoted_warning()
    assert _summary_line(tmp_path, capsys, promoted, held=1) == (
        "0 errors; 0 warnings (1 baselined); 0 info; 1 baselined not shown (--show-baselined)"
    )
    assert _summary_line(tmp_path, capsys, promoted) == "1 error; 0 warnings; 0 info"
    warning = D("SST-CFG018", subject="tool_group:partner", group="partner", name="other")
    assert _summary_line(tmp_path, capsys, promoted, _promoted_warning("third"), warning) == (
        "2 errors; 1 warning; 0 info"
    )
    assert [counted(count, noun) for count, noun in ((1, "warning"), (2, "warning"), (1, "info"), (3, "info"))] == [
        "1 warning",
        "2 warnings",
        "1 info",
        "3 info",
    ]
    resolve_invocation(project_dir=tmp_path, config_file=None, target=None, baselined=frozenset())


def test_the_envelope_counts_a_baselined_diagnostic_under_baselined_and_under_no_severity(tmp_path: Path) -> None:
    promoted = _promoted_warning()
    resolve_invocation(project_dir=tmp_path, config_file=None, target=None, baselined=frozenset())
    assert json_envelope("validate", DiagnosticBag((promoted,)))["exit_code"] == 1
    resolve_invocation(
        project_dir=tmp_path, config_file=None, target=None, baselined=frozenset((stable_fingerprint(promoted),))
    )
    envelope = json_envelope("validate", DiagnosticBag((promoted,)), promoted=1)
    assert (envelope["exit_code"], envelope["status"]) == (0, "ok")
    assert envelope["summary"] == {
        "error": 0,
        "warning": 0,
        "info": 0,
        "promoted": 1,
        "suppressed_cascade": 0,
        "baselined": 1,
    }
    listed = envelope["diagnostics"]
    assert isinstance(listed, list) and [(item["baselined"], item["severity"]) for item in listed] == [(True, "error")]
    resolve_invocation(project_dir=tmp_path, config_file=None, target=None, baselined=frozenset())


def test_human_output_and_usage_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    (tmp_path / "human").mkdir()
    (tmp_path / "human" / "dbt_project.yml").write_text("name: human\nprofile: human\n", encoding="utf-8")
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(tmp_path / "human")])
    assert initialized.exit_code == 0 and "initialized" in initialized.output
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect"])
    assert debugged.exit_code == 0 and "state_table:" in debugged.output
    assert "candidates:" in debugged.output and "sst_config.yaml" in debugged.output
    compiled = CliRunner().invoke(cli, ["compile", *common(project)])
    assert compiled.exit_code == 0 and "compiled 14 artifact(s)" in compiled.output

    conflict = CliRunner().invoke(
        cli,
        ["plan", *common(project), "--plan-out", str(tmp_path / "p.json"), "--no-plan-out"],
    )
    assert conflict.exit_code == 3
    prune = CliRunner().invoke(cli, ["apply", *common(project), "--prune"])
    assert prune.exit_code == 3

    port = FakeSnowflake(state={})
    planned = invoke_with_port(monkeypatch, port, ["plan", *common(project), "--target", "dev"])
    assert planned.exit_code == 2 and "Plan:" in planned.output
    cleaned = CliRunner().invoke(cli, ["clean", "--project-dir", str(project)])
    assert cleaned.exit_code == 0 and "removed" in cleaned.output
    nothing = CliRunner().invoke(cli, ["clean", "--project-dir", str(project)])
    assert nothing.exit_code == 0 and "nothing to remove" in nothing.output
