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
        first("eval --target verify"),
    ]
    assert order == sorted(order) and len(set(order)) == 4
    manifests = {found[1] for run in runs if "poetry run sst" in run if (found := re.search(r"--manifest (\S+)", run))}
    assert manifests == {"tests/fixtures/reference_project/target/manifest.json"}


# dbt Core 1.13 is the last 1.x minor (dbt Labs); from 2.0, dbt ships as `dbt` and `dbt-oss`.
_LAST_CORE_MINOR = 13
_V2_DISTRIBUTIONS = {"": "dbt", "-oss": "dbt-oss"}
_RELEASE = re.compile(r"(\d+)\.(\d+)\.\d+(?:[ab]\d+|rc\d+)?")


def _admitted_lines() -> list[str]:
    """The dbt release lines the reference project's `require-dbt-version` admits.

    `>=1.a.0, <2.b.0` admits the dbt Core minors from 1.a to 1.13, then each dbt v2 minor below
    2.b in both its distributions: `2.0` for `dbt`, `2.0-oss` for `dbt-oss`.
    """
    project = yaml.safe_load((FIXTURE / "dbt_project.yml").read_text(encoding="utf-8"))
    lower, upper = project["require-dbt-version"]
    low = re.fullmatch(r">=(\d+)\.(\d+)\.0", lower)
    high = re.fullmatch(r"<(\d+)\.(\d+)\.0", upper)
    assert low and high and int(low.group(1)) <= int(high.group(1)) <= 2, project["require-dbt-version"]
    lines: list[str] = []
    for major in range(int(low.group(1)), int(high.group(1)) + 1):
        first = int(low.group(2)) if major == int(low.group(1)) else 0
        last = int(high.group(2)) if major == int(high.group(1)) else _LAST_CORE_MINOR + 1
        suffixes = ("",) if major == 1 else tuple(_V2_DISTRIBUTIONS)
        lines.extend(f"{major}.{minor}{suffix}" for minor in range(first, last) for suffix in suffixes)
    return lines


def _line(release: str) -> str:
    """The release line, `1.12` or `2.0`, a release such as `1.12.5` or `1.13.0b1` belongs to."""
    found = _RELEASE.fullmatch(release)
    assert found, release
    return f"{found.group(1)}.{found.group(2)}"


def test_the_required_gate_parses_the_reference_project_with_every_admitted_dbt_line() -> None:
    gate = _workflow("test-and-lint.yml")
    assert set(_triggers(gate)) == {"push", "pull_request", "workflow_call"}
    job = gate["jobs"]["dbt"]
    admitted = _admitted_lines()
    assert job["strategy"]["matrix"]["dbt"] == admitted
    releases = {path.parent.name.removeprefix("dbt-") for path in COMMITTED_MANIFESTS}
    captured = sorted({_line(release) for release in releases})
    assert captured == sorted({line.removesuffix("-oss") for line in admitted}), (
        "capture a manifest for each line (tests/helpers/manifest_capture.py)"
    )
    # Each line runs the exact release a committed manifest was written by -- dbt Core with a
    # dbt-snowflake release no newer than its minor, or dbt v2 on its own -- and nothing floats
    # to a newer patch.
    pins = {entry["dbt"]: entry for entry in job["strategy"]["matrix"]["include"]}
    assert sorted(pins) == sorted(admitted)
    for line, entry in pins.items():
        release = entry["release"]
        assert release in releases and _line(release) == line.removesuffix("-oss"), entry
        packages = entry["install"].split()
        if line.startswith("1."):
            core, adapter = packages
            assert core == f"dbt-core=={release}", entry
            snowflake = re.fullmatch(r"dbt-snowflake==1\.(\d+)\.\d+", adapter)
            assert snowflake and int(snowflake.group(1)) <= int(line.split(".")[1]), entry
        else:
            assert packages == [f"{_V2_DISTRIBUTIONS[line.removeprefix(_line(release))]}=={release}"], entry
    runs = [" ".join(str(step.get("run", "")).split()) for step in job["steps"]]
    # dbt Core comes with the dbt extra; dbt v2, which brings its own `dbt`, without it.
    assert any("--extras dbt" in run and "startsWith(matrix.dbt, '1.')" in run for run in runs)
    install = next(step for step in job["steps"] if "$DBT_INSTALL" in str(step.get("run", "")))
    assert install["env"]["DBT_INSTALL"] == "${{ matrix.install }}"
    assert not any("--constraint" in run or ".*" in run for run in runs)
    assert any("dbt deps" in run for run in runs)
    goldens = next(step for step in job["steps"] if "tests/unit/test_goldens.py" in str(step.get("run", "")))
    assert goldens["env"]["SST_GOLDEN_PROJECT"] == "${{ env.REFERENCE_PROJECT }}"
    assert "--manifest" not in goldens["run"], "SST must run dbt itself here, not read a manifest"
    # The contract step reads the manifest that `dbt parse` wrote while the goldens rendered.
    assert runs.index("poetry run pytest tests/unit/test_goldens.py") < next(
        index for index, run in enumerate(runs) if "test_dbt_manifest_contract.py" in run
    )
    assert any(
        "tests/contract/test_dbt_manifest_contract.py --dbt-manifest" in run and "target/manifest.json" in run
        for run in runs
    )


def _admitted_pythons() -> list[str]:
    """The `3.<minor>` releases pyproject.toml's `python = ">=3.A,<3.B"` admits, in order."""
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    found = re.search(r'^python = ">=3\.(\d+),<3\.(\d+)"$', pyproject, re.MULTILINE)
    assert found, "pyproject.toml declares python as >=3.A,<3.B"
    return [f"3.{minor}" for minor in range(int(found.group(1)), int(found.group(2)))]


def test_the_required_gate_tests_and_installs_on_every_python_pyproject_admits() -> None:
    # The README and getting-started promise these versions; CI must run each of them.
    admitted = _admitted_pythons()
    jobs = _workflow("test-and-lint.yml")["jobs"]
    for name in ("test", "wheel"):
        job = jobs[name]
        assert job["strategy"]["matrix"]["python"] == admitted, name
        assert job["strategy"]["fail-fast"] is False, name
        setup = next(step for step in job["steps"] if str(step.get("uses", "")).startswith("actions/setup-python@"))
        assert setup["with"]["python-version"] == "${{ matrix.python }}", name
    cache = next(step for step in jobs["test"]["steps"] if step.get("id") == "cached-poetry-dependencies")
    assert "${{ matrix.python }}" in cache["with"]["key"]
    # The coverage numbers are one version's: the ratchet and the floors run on the oldest only.
    coverage = [
        step
        for step in jobs["test"]["steps"]
        if str(step.get("name", "")).startswith("Check") and "coverage" in str(step.get("name", ""))
    ]
    assert len(coverage) == 5 and all(step.get("if") == f"matrix.python == '{admitted[0]}'" for step in coverage)
    for page in ("README.md", "docs/getting-started.md", "CONTRIBUTING.md"):
        text = (REPO_ROOT / page).read_text(encoding="utf-8")
        assert f"{admitted[0]}–{admitted[-1]}" in text or ", ".join(admitted[:-1]) in text, page


def test_the_required_gate_runs_the_built_wheel_installed_alone_outside_the_checkout() -> None:
    job = _workflow("test-and-lint.yml")["jobs"]["wheel"]
    runs = [" ".join(str(step.get("run", "")).split()) for step in job["steps"]]
    joined = " ".join(runs)
    assert any(run == "poetry build --no-interaction --format wheel" for run in runs)
    # Nothing but the wheel is installed: no project install, no editable or source install.
    assert "poetry install" not in joined and "poetry run" not in joined
    assert re.findall(r"pip\"? install (?:--\S+ )*(\S+)", joined) == ["dist/*.whl"]
    smoke = next(run for run in runs if "venv/bin/sst --version" in run)
    assert smoke.index('cd "$SMOKE"') < smoke.index("venv/bin/sst --version")
    assert '"$GITHUB_WORKSPACE"/*) echo "imported from the checkout' in smoke
    for command in ("validate", "compile"):
        assert f'venv/bin/sst --project-dir project {command} --manifest "$M"' in smoke, command
    assert 'M="$GITHUB_WORKSPACE/tests/fixtures/reference_project_manifest.json"' in smoke


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
