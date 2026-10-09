"""`--allow-stale-manifest`: a stale `--manifest` is reported as a warning, and recorded."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.seam_projects import SmallProject, manifest, model_node

SQL = "select 1 as products_id\n"
PROFILES = (
    "fixture:\n  target: dev\n  outputs:\n    dev:\n      type: snowflake\n"
    "      account: acct\n      user: someone\n      database: DB\n      schema: SCH\n"
)
CONFIG = "project:\n  semantic_models_dir: semantic_models\nvalidation:\n  snowflake_syntax_check: false\n"


def _stale(tmp_path: Path) -> list[str]:
    node = model_node("products", original_file_path="models/products.sql", package_name="fixture")
    node["checksum"] = {"name": "sha256", "checksum": sha256(SQL.encode()).hexdigest()}
    project = SmallProject(
        tmp_path,
        files={"models/products.sql": "select 2 as products_id\n", "profiles.yml": PROFILES, "sst_config.yml": CONFIG},
        document=manifest({"model.fixture.products": node}),
    )
    path = project.write()
    return ["--project-dir", str(tmp_path), "--manifest", str(path), "--no-snowflake-syntax-check", "-o", "json"]


def _run(args: list[str]) -> tuple[int, dict[str, Any]]:
    result = CliRunner().invoke(cli, args)
    return result.exit_code, json.loads(result.stdout)


def _stale_diagnostics(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in envelope["diagnostics"] if item["code"] == "SST-DBT005"]


def test_a_stale_manifest_is_an_error_by_default(tmp_path: Path) -> None:
    code, envelope = _run(["validate", "--no-strict", *_stale(tmp_path)])
    [diagnostic] = _stale_diagnostics(envelope)
    assert diagnostic["severity"] == "error"
    assert code == 1


@pytest.mark.parametrize("before", [True, False])
def test_allow_stale_manifest_reports_it_as_a_recorded_warning(tmp_path: Path, before: bool) -> None:
    args = _stale(tmp_path)
    argv = ["--allow-stale-manifest", "validate", "--no-strict", *args]
    if not before:
        argv = ["validate", "--no-strict", *args, "--allow-stale-manifest"]
    code, envelope = _run(argv)
    [diagnostic] = _stale_diagnostics(envelope)
    assert diagnostic["severity"] == "warning"
    assert "--allow-stale-manifest" in envelope["invocation"]["overrides"]
    assert not [item for item in envelope["diagnostics"] if item["severity"] == "error"]
    assert code == 0
