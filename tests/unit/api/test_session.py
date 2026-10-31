"""The library interface: a session reports what `sst` reports, and answers from what it read.

`validate` must be `sst validate --no-snowflake-syntax-check --output json` as data, so each
parity test runs the real command in-process on the same project and compares field by field.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.api import ProjectSession
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.reference_project import DBT_MANIFEST, MENU_VIEW, project_copy


def _cli_validate(project: Path, *flags: str) -> tuple[int, dict[str, Any]]:
    args = ["validate", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "--no-snowflake-syntax-check"]
    result = CliRunner().invoke(cli, [*args, *flags, "--output", "json"])
    return result.exit_code, json.loads(result.output)


def _assert_same(project: Path, *flags: str, strict: bool | None = None, baseline: bool = True) -> None:
    exit_code, envelope = _cli_validate(project, *flags)
    validation = ProjectSession.open(project, manifest_path=DBT_MANIFEST).validate(strict=strict, baseline=baseline)

    assert [dict(item) for item in validation.diagnostics] == envelope["diagnostics"]
    assert dict(validation.summary) == envelope["summary"]
    assert validation.passed == (exit_code == 0)
    assert (validation.rules_run, validation.artifacts_checked) == (
        envelope["data"]["rules_run"],
        envelope["data"]["artifacts_checked"],
    )


def test_validate_reports_what_the_command_reports(tmp_path: Path) -> None:
    _assert_same(project_copy(tmp_path))


def test_validate_reports_what_the_command_reports_for_the_project_as_committed(tmp_path: Path) -> None:
    # Strict, as the fixture commits it: warnings promoted, the strict notice given.
    _assert_same(project_copy(tmp_path, offline=False))


@pytest.mark.parametrize(("flag", "strict"), [("--strict", True), ("--no-strict", False)])
def test_validate_follows_a_strict_flag_as_the_command_does(tmp_path: Path, flag: str, strict: bool) -> None:
    _assert_same(project_copy(tmp_path, offline=False), flag, strict=strict)


def test_validate_reports_an_error_and_fails_as_the_command_does(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    view = project / MENU_VIEW
    view.write_text(view.read_text(encoding="utf-8").replace("ref('products')", "ref('no_such_model')", 1))

    _assert_same(project)
    assert not ProjectSession.open(project, manifest_path=DBT_MANIFEST).validate().passed


def test_validate_holds_what_the_baseline_holds_as_the_command_does(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    added = CliRunner().invoke(
        cli,
        ["baseline", "add", "--all-warnings", "--yes", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST)],
    )
    assert added.exit_code == 0, added.output

    _assert_same(project)
    _assert_same(project, "--no-baseline", baseline=False)
    assert ProjectSession.open(project, manifest_path=DBT_MANIFEST).validate().summary["baselined"] > 0


def test_the_catalog_lists_every_compiled_artifact_and_render_returns_what_it_publishes(tmp_path: Path) -> None:
    session = ProjectSession.open(project_copy(tmp_path), manifest_path=DBT_MANIFEST)

    catalog = session.catalog()
    assert [item.key for item in catalog] == [item.artifact_key for item in session.compiled.compiled]
    assert {item.type for item in catalog} >= {"semantic_view", "agent", "skill"}
    view = next(item for item in catalog if item.type == "semantic_view")
    assert view.source_files and view.member_keys
    rendered = session.render(view.key)
    assert (rendered.target, rendered.fingerprint) == (view.target, view.fingerprint)
    assert rendered.ddl.startswith("CREATE OR REPLACE SEMANTIC VIEW")
    with pytest.raises(KeyError):
        session.render("semantic_view:no_such_view")


def test_reload_reads_the_files_again_and_leaves_the_session_it_came_from_as_it_was(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    view = project / MENU_VIEW
    view.write_text(view.read_text(encoding="utf-8").replace("ref('products')", "ref('no_such_model')", 1))

    reloaded = session.reload(dbt=False)

    assert session.validate().passed
    assert not reloaded.validate().passed
    assert {item.key for item in reloaded.catalog()} < {item.key for item in session.catalog()}
