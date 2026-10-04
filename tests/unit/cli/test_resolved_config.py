"""One resolved configuration per run: every reader sees a target conditional resolve the same way."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.settings import severity_overrides_setting, strict_disagreement, validation_settings
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import common
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import project_copy

STRICT = "\"{{ 'true' if target.name == 'prod' else 'false' }}\""
OVERRIDE = "\"{{ 'error' if target.name == 'prod' else 'warning' }}\""


def _project(tmp_path: Path) -> Path:
    """The reference project, strict and overriding SST-CFG018 to an error only on `prod`."""
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    text = config.read_text(encoding="utf-8").replace("  strict: false\n", f"  strict: {STRICT}\n", 1)
    config.write_text(
        text + f"\ndiagnostics:\n  severity_overrides:\n    SST-CFG018: {OVERRIDE}\n",
        encoding="utf-8",
    )
    return project


def _validate(project: Path, *flags: str) -> tuple[int, list[dict[str, Any]]]:
    result = CliRunner().invoke(cli, ["validate", *common(project), *flags, "--output", "json"])
    return result.exit_code, json.loads(result.output)["diagnostics"]


def _severities(diagnostics: list[dict[str, Any]], code: str) -> set[str]:
    return {item["severity"] for item in diagnostics if item["code"] == code}


def test_strict_and_an_override_both_follow_the_target_the_run_names(tmp_path: Path) -> None:
    project = _project(tmp_path)
    exit_code, prod = _validate(project, "-t", "prod")
    assert exit_code == 1
    assert _severities(prod, "SST-CFG018") == {"error"} and _severities(prod, "SST-VAL528") == {"error"}
    exit_code, dev = _validate(project, "-t", "dev")
    assert exit_code == 0
    assert _severities(dev, "SST-CFG018") == {"warning"} and _severities(dev, "SST-VAL528") == {"warning"}
    assert "SST-CFG004" not in {item["code"] for item in (*prod, *dev)}


def test_the_strict_flag_disagrees_with_the_key_only_where_the_key_resolves_true(tmp_path: Path) -> None:
    project = _project(tmp_path)
    _, prod = _validate(project, "-t", "prod", "--no-strict")
    assert [item["message"] for item in prod if item["code"] == "SST-CFG034"] != []
    # The override is not strict: it still makes the warning an error on prod.
    assert _severities(prod, "SST-CFG018") == {"error"} and _severities(prod, "SST-VAL528") == {"warning"}
    _, dev = _validate(project, "-t", "dev", "--no-strict")
    assert "SST-CFG034" not in {item["code"] for item in dev}


def test_every_setting_reader_resolves_the_conditional_for_the_run_target(tmp_path: Path) -> None:
    project = _project(tmp_path)
    for target, strict, severity in (("prod", True, Severity.ERROR), ("dev", False, Severity.WARNING)):
        paths = dataclasses.replace(project_paths(project), target_name=target)
        assert validation_settings(paths, strict=None, connected=None)[0] is strict
        assert severity_overrides_setting(paths) == {"SST-CFG018": severity}
        assert bool(strict_disagreement(paths, False)) is strict


def test_the_configuration_is_read_once_per_target_and_kept(tmp_path: Path) -> None:
    project = _project(tmp_path)
    paths = project_paths(project)
    first = resolved_config(paths, "prod")
    (project / "sst_config.yml").write_text("validation:\n  snowflake_syntax_check: false\n", encoding="utf-8")
    assert resolved_config(paths, "prod") is first
    assert resolved_config(paths, "dev") is not first
    assert resolved_config(paths, "dev").tree.get("diagnostics") is None


def test_a_conditional_literal_that_is_not_the_key_type_is_reported_as_mistyped(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p\n", encoding="utf-8")
    (tmp_path / "sst_config.yml").write_text(
        "validation:\n  snowflake_syntax_check: \"{{ 'maybe' if target.name == 'x' else 'no' }}\"\n"
        "enrichment:\n  distinct_limit: \"{{ '30' if target.name == 'x' else '20' }}\"\n",
        encoding="utf-8",
    )
    config = resolved_config(project_paths(tmp_path), "x")
    assert [item.code for item in config.diagnostics] == ["SST-CFG004"]
    assert config.tree["enrichment"] == {"distinct_limit": 30}
