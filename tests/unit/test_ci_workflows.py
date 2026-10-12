"""The GitHub workflows and issue forms, read offline: what runs where, and what the Snowflake checks need."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.helpers import session_recorder, sweep_scratch
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.manifest_capture import FIXTURES as MANIFEST_FIXTURES
from tests.helpers.reference_project import FIXTURE, REPO_ROOT, project_copy

WORKFLOWS = REPO_ROOT / ".github" / "workflows"
COMMITTED_MANIFESTS = tuple(MANIFEST_FIXTURES.glob("v*/dbt-*/manifest.json"))
MANUAL = "slow.yml"
EVALS = "inputs.suite == 'evals'"
# Only the manual workflow reaches Snowflake; these name what it needs to.
SNOWFLAKE_ONLY = ("secrets.SNOWFLAKE_", "SST_TEST_SNOWFLAKE_", "tests.helpers.sweep_scratch", "-m live")


def _workflow(name: str) -> dict[Any, Any]:
    document = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def _triggers(document: dict[Any, Any]) -> dict[str, Any]:
    # YAML 1.1 reads the bare key `on` as True.
    found = document.get("on", document.get(True))
    return {found: None} if isinstance(found, str) else dict(found)


def _evals_runs() -> list[str]:
    steps = _workflow(MANUAL)["jobs"]["run"]["steps"]
    return [" ".join(str(step.get("run", "")).split()) for step in steps if step.get("if") == EVALS]


def test_every_workflow_parses_and_only_the_manual_one_reaches_snowflake() -> None:
    names = sorted(path.name for path in WORKFLOWS.glob("*.yml"))
    assert MANUAL in names
    for name in names:
        assert _workflow(name)["jobs"], name
        if name != MANUAL:
            text = (WORKFLOWS / name).read_text(encoding="utf-8")
            assert [needle for needle in SNOWFLAKE_ONLY if needle in text] == [], name
    assert set(_triggers(_workflow(MANUAL))) == {"workflow_dispatch"}


def test_the_evals_suite_builds_compiles_and_applies_verify_before_it_runs() -> None:
    runs = _evals_runs()

    def first(fragment: str) -> int:
        return next(index for index, run in enumerate(runs) if fragment in run)

    order = [
        first("dbt build --target verify"),
        first("compile --target verify"),
        first("apply --target verify --yes --no-strict"),
        first("test --suite evals --target verify"),
    ]
    assert order == sorted(order) and len(set(order)) == 4
    manifests = {found[1] for run in runs if "poetry run sst" in run if (found := re.search(r"--manifest (\S+)", run))}
    assert manifests == {"tests/fixtures/reference_project/target/manifest.json"}


def _admitted_minors() -> list[str]:
    """The dbt minors the reference project's `require-dbt-version` admits, as `>=1.a.0, <1.b.0`."""
    project = yaml.safe_load((FIXTURE / "dbt_project.yml").read_text(encoding="utf-8"))
    lower, upper = project["require-dbt-version"]
    low = re.fullmatch(r">=(\d+)\.(\d+)\.0", lower)
    high = re.fullmatch(r"<(\d+)\.(\d+)\.0", upper)
    assert low and high and low.group(1) == high.group(1), project["require-dbt-version"]
    return [f"{low.group(1)}.{minor}" for minor in range(int(low.group(2)), int(high.group(2)))]


def test_the_required_gate_parses_the_reference_project_with_every_admitted_dbt_minor() -> None:
    gate = _workflow("test-and-lint.yml")
    assert set(_triggers(gate)) == {"push", "pull_request", "workflow_call"}
    job = gate["jobs"]["dbt"]
    assert job["strategy"]["matrix"]["dbt"] == _admitted_minors()
    captured = sorted({path.parent.name.removeprefix("dbt-").rsplit(".", 1)[0] for path in COMMITTED_MANIFESTS})
    assert captured == _admitted_minors(), "capture a manifest for each minor (tests/helpers/manifest_capture.py)"
    # Each minor runs the exact dbt Core release a committed manifest was written by, and a
    # dbt-snowflake release of the same minor; no version floats to a newer patch.
    pins = {entry["dbt"]: entry for entry in job["strategy"]["matrix"]["include"]}
    releases = {path.parent.name.removeprefix("dbt-") for path in COMMITTED_MANIFESTS}
    assert sorted(pins) == _admitted_minors()
    for minor, entry in pins.items():
        assert entry["dbt_core"] in releases and entry["dbt_core"].rsplit(".", 1)[0] == minor, entry
        assert re.fullmatch(rf"{re.escape(minor)}\.\d+", entry["dbt_snowflake"]), entry
    runs = [" ".join(str(step.get("run", "")).split()) for step in job["steps"]]
    assert any("--extras dbt" in run for run in runs)
    assert any('"dbt-core==$DBT_CORE" "dbt-snowflake==$DBT_SNOWFLAKE"' in run for run in runs)
    assert not any("--constraint" in run or ".*" in run for run in runs)
    assert any("dbt deps" in run for run in runs)
    golden = next(run for run in runs if "test --suite golden" in run)
    assert "--manifest" not in golden, "SST must run dbt itself here, not read a manifest"
    assert any(
        "tests/contract/test_dbt_manifest_contract.py --dbt-manifest" in run and "target/manifest.json" in run
        for run in runs
    )


def test_the_live_suites_run_by_hand_before_each_release_and_on_no_schedule() -> None:
    for name in sorted(path.name for path in WORKFLOWS.glob("*.yml")):
        text = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert name == MANUAL or "-m live" not in text, name
    assert set(_triggers(_workflow(MANUAL))) == {"workflow_dispatch"}
    contributing = (REPO_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    releasing = contributing.split("### Releasing", 1)[1].split("\n## ", 1)[0]
    recording = session_recorder.RECORDING.relative_to(REPO_ROOT).as_posix()
    for needle in ("`live` suite", "`recording` suite", "-m live --require-snowflake", recording):
        assert needle in releasing, needle
    guide = (REPO_ROOT / "docs" / "guides" / "testing-live.md").read_text(encoding="utf-8")
    assert "## When the live suites run" in guide and "CONTRIBUTING.md#releasing" in guide


def test_the_verify_target_resolves_every_partner_reference_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in ("ACCOUNT", "USER", "ROLE", "WAREHOUSE", "DATABASE", "SCHEMA"):
        monkeypatch.setenv(f"SST_TEST_SNOWFLAKE_{key}", f"PLACEHOLDER_{key}")
    project = project_copy(tmp_path)
    _, diagnostics = invoke_json(["compile", *common(project), "--target", "verify"])
    unresolved = [item["message"] for item in diagnostics if item["code"] in ("SST-REF018", "SST-REF019")]
    assert [message for message in unresolved if "jaffle_partner" in message] == []


def test_the_grantee_role_is_a_test_harness_grant_only() -> None:
    engine = REPO_ROOT / "snowflake_semantic_tools"
    assert [path for path in engine.rglob("*.py") if "GRANTEE_ROLE" in path.read_text(encoding="utf-8")] == []
    readers = sorted(
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / "tests").rglob("*.py")
        if path != Path(__file__).resolve() and ".grantee_role" in path.read_text(encoding="utf-8")
    )
    assert readers == ["tests/integration/test_apply_live.py", "tests/unit/test_live_config.py"]
    guide = (REPO_ROOT / "docs" / "guides" / "testing-live.md").read_text(encoding="utf-8")
    assert "The `GRANTEE_ROLE` grant is the test harness's own." in guide


def test_the_scratch_sweep_is_described_as_the_manual_run_it_is() -> None:
    assert "sweep_scratch" not in (WORKFLOWS / "nightly.yml").read_text(encoding="utf-8")
    assert "tests.helpers.sweep_scratch --older-than-hours" in (WORKFLOWS / MANUAL).read_text(encoding="utf-8")
    described = (sweep_scratch.__doc__ or "").casefold()
    assert "nightly" not in described and "by hand" in described


def test_the_bug_report_form_sends_vulnerabilities_to_the_private_security_policy() -> None:
    form = yaml.safe_load((REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml").read_text(encoding="utf-8"))
    intro = form["body"][0]["attributes"]["value"]
    assert "vulnerability" in intro and "SECURITY.md" in intro and "Do not open an issue" in intro
    assert "Do not open a public GitHub issue" in (REPO_ROOT / "SECURITY.md").read_text(encoding="utf-8")
