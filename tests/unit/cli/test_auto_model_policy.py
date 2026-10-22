"""SST-VAL560 through `sst validate`: the YAML keys that keep `auto`, and the policy that sets its severity.

The reference project pins every agent's model and its CoWork profile's; each test copies it and
leaves `jaffle_minimal_agent`, which only inherits the `agents:` root's, or the profile on `auto`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.cli_projects import common, invoke_with_port, validate_json
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

PER_TARGET = "\"{{ 'error' if target.name == 'prod' else 'warning' }}\""


def _edit(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, old
    path.write_text(text.replace(old, new), encoding="utf-8")


def _project(tmp_path: Path, *, overrides: dict[str, str] | None = None) -> Path:
    """Copy the reference project with the `agents:` root's model unset, and `overrides` added."""
    project = project_copy(tmp_path)
    _edit(project / "sst_config.yml", "  +orchestration_model: claude-sonnet-4-6\n", "")
    block = "diagnostics:\n  severity_overrides:\n"
    lines = "".join(f"    {code}: {severity}\n" for code, severity in (overrides or {}).items())
    _edit(project / "sst_config.yml", block, block + lines)
    return project


def _auto(diagnostics: list[dict[str, Any]]) -> list[tuple[str, str, str | None, str | None]]:
    return [
        (item["params"]["artifact"], item["severity"], item["promoted_from"], item["demoted_from"])
        for item in diagnostics
        if item["code"] == "SST-VAL560"
    ]


def _codes(diagnostics: list[dict[str, Any]]) -> set[str]:
    return {item["code"] for item in diagnostics}


def test_an_agent_left_on_auto_is_warned_and_strict_mode_leaves_the_warning(tmp_path: Path) -> None:
    project = _project(tmp_path)
    exit_code, diagnostics = validate_json(project, "--no-strict")
    assert exit_code == 0
    assert _auto(diagnostics) == [("jaffle_minimal_agent", "warning", None, None)]
    strict_exit, strict = validate_json(project, "--strict")
    # Strict promotes the fixture's deliberate warnings, so the run fails, but not this one.
    assert strict_exit == 1 and ("error", "warning") in {(item["severity"], item["promoted_from"]) for item in strict}
    assert _auto(strict) == [("jaffle_minimal_agent", "warning", None, None)]
    assert "SST-INT007" not in _codes(diagnostics) | _codes(strict)


@pytest.mark.parametrize(
    ("severity", "exit_code", "reported"),
    [
        ("info", 0, ("info", None, "warning")),
        ("warning", 0, ("warning", None, None)),
        ("error", 1, ("error", "warning", None)),
    ],
)
def test_severity_overrides_set_what_it_reports(
    tmp_path: Path, severity: str, exit_code: int, reported: tuple[str, str | None, str | None]
) -> None:
    project = _project(tmp_path, overrides={"SST-VAL560": severity})
    for flag in ("--no-strict", "--strict"):
        found_exit, diagnostics = validate_json(project, flag)
        assert _auto(diagnostics) == [("jaffle_minimal_agent", *reported)]
        assert "SST-CFG033" not in _codes(diagnostics) and "SST-INT007" not in _codes(diagnostics)
    assert validate_json(project, "--no-strict")[0] == exit_code


def test_a_per_target_override_resolves_for_each_target(tmp_path: Path) -> None:
    project = _project(tmp_path, overrides={"SST-VAL560": PER_TARGET})
    dev_exit, dev = validate_json(project, "--no-strict")
    assert dev_exit == 0 and _auto(dev) == [("jaffle_minimal_agent", "warning", None, None)]
    prod_exit, prod = validate_json(project, "--no-strict", "--target", "prod")
    assert prod_exit == 1 and _auto(prod) == [("jaffle_minimal_agent", "error", "warning", None)]


def test_a_strict_plan_reports_the_warning_and_does_not_promote_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = [
        "plan",
        *common(_project(tmp_path)),
        "--strict",
        "--no-snowflake-syntax-check",
        "--no-plan-out",
        "-o",
        "json",
    ]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert _auto(json.loads(result.stdout)["diagnostics"]) == [("jaffle_minimal_agent", "warning", None, None)]


def test_allow_auto_model_in_a_folder_route_or_an_agents_config_keeps_auto(tmp_path: Path) -> None:
    routed = _project(tmp_path / "route")
    _edit(
        routed / "sst_config.yml",
        "  ops:\n    +schema: OPS\n",
        "  ops:\n    +schema: OPS\n    +allow_auto_model: true\n",
    )
    own = _project(tmp_path / "own")
    _edit(
        own / "agents" / "ops" / "jaffle_minimal" / "agent.yml",
        "name: jaffle_minimal_agent\n",
        ("name: jaffle_minimal_agent\nconfig:\n  allow_auto_model: true\n"),
    )
    for project in (routed, own):
        exit_code, diagnostics = validate_json(project, "--no-strict")
        assert exit_code == 0 and _auto(diagnostics) == []
        assert "SST-CFG003" not in _codes(diagnostics)


def test_a_cowork_profile_left_on_auto_is_warned_until_its_block_keeps_auto(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    profile = project / "cowork_profiles" / "jaffle_concierge" / "cowork_profile.yml"
    _edit(profile, "  orchestration_model: claude-sonnet-4-6\n", "")
    # The `cowork_profiles:` root names `auto` itself.
    exit_code, diagnostics = validate_json(project, "--no-strict")
    assert exit_code == 0
    [found] = [item for item in diagnostics if item["code"] == "SST-VAL560"]
    assert (found["params"]["type"], found["params"]["artifact"]) == ("cowork profile", "jaffle_concierge")
    assert "`cowork_profiles.+orchestration_model: claude-sonnet-4-6`" in found["suggestion"]
    _edit(
        project / "sst_config.yml",
        "  +orchestration_model: auto\n",
        "  +orchestration_model: auto\n  +allow_auto_model: true\n",
    )
    exit_code, diagnostics = validate_json(project, "--no-strict")
    assert exit_code == 0 and _auto(diagnostics) == []
