"""`sst test`: smoke probes of what `sst apply` published, and the eval options it does not take."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.cli_projects import (
    applied_port,
    common,
    compile_project,
    invoke_with_port,
)
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def test_smoke_suite_is_separate_from_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    manifest = json.loads((project / "sst_target" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker

    applied_state = {
        key: AppliedEntry(
            value["fingerprint"],
            value["publish_target"]["qualified_name"],
            "now",
            "run",
            "applied",
            value["fingerprint"],
            manifest_id,
        )
        for key, value in manifest["artifacts"].items()
    }
    markers = {
        value["publish_target"]["qualified_name"]: OwnershipMarker(
            manifest_id,
            value["fingerprint"],
        )
        for value in manifest["artifacts"].values()
    }
    port = FakeSnowflake(state=applied_state, markers=markers)
    result = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--output", "json"],
    )
    assert result.exit_code == 0
    attempted = json.loads(result.output)["data"]["attempted"]
    assert attempted > 3
    assert len(port.queries) == attempted

    unapplied = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["test", *common(project), "--target", "dev", "--output", "json"],
    )
    assert unapplied.exit_code == 1
    assert [item["code"] for item in json.loads(unapplied.output)["diagnostics"]] == ["SST-APL037"]

    views = project / "semantic_models" / "semantic_views" / "jaffle_minimal.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("Menu products only", "Changed after compile"),
        encoding="utf-8",
    )
    stale = invoke_with_port(
        monkeypatch,
        FakeSnowflake(),
        ["test", *common(project), "--target", "dev", "--output", "json"],
    )
    assert stale.exit_code == 4
    assert "compiled SST manifest is stale" in json.loads(stale.output)["data"]["error"]


@pytest.mark.parametrize("args", [["--suite", "golden"], ["--update-golden"], ["--golden-dir", "expected/ddl"]])
def test_the_golden_suite_is_not_part_of_sst_test(tmp_path: Path, args: list[str]) -> None:
    result = CliRunner().invoke(cli, ["test", *common(project_copy(tmp_path)), *args])
    assert result.exit_code == 3, result.output


def test_smoke_failure_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = applied_port(project)
    port.query = lambda sql, params=None: (_ for _ in ()).throw(SnowflakePortError("broken"))  # type: ignore[method-assign]
    smoke = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--fail-fast"],
    )
    assert smoke.exit_code == 1 and "SST-APL100" in smoke.output


def test_smoke_on_a_target_nothing_was_applied_to_says_the_probes_were_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["test", *common(project), "--target", "dev"])
    assert result.exit_code == 1
    assert "smoke probes skipped: " in result.output and " not published as compiled" in result.output
    assert "ran smoke probes" not in result.output
    assert "error[SST-APL037]: nothing `sst test` checks is published on this target" in result.output
    assert "run `sst plan`, then `sst apply`" in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["--suite", "smoke"],
        ["--capture-baseline", "--reason", "why"],
        ["--unpublished"],
        ["--compare-models"],
        ["--refresh-cost", "compare.json"],
        ["--break-stale-lock"],
    ],
)
def test_eval_options_are_not_sst_test_options(tmp_path: Path, args: list[str]) -> None:
    result = CliRunner().invoke(cli, ["test", *common(project_copy(tmp_path)), *args])
    assert result.exit_code == 3, result.output
    assert "No such option" in result.output
