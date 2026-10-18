"""The reference project's every compiled payload, byte for byte against its committed golden.

This is what `sst test --suite golden` checked before the goldens became the engine's own
tests: every artifact type the reference project declares compiles to exactly its golden
under `tests/golden/expected/`, and compiling the same files twice reports the same
diagnostics, fingerprint for fingerprint.

By default the project compiles from the vendored dbt manifest. With `SST_GOLDEN_PROJECT`
naming a copy of the reference project whose dbt packages are installed, it compiles that
copy with no manifest, so SST runs `dbt parse` itself as on a user's project: CI's dbt jobs
run this module that way for each dbt release they pin. When a change to rendered output is
intended, `python -m tests.helpers.update_goldens` rewrites the goldens.
"""

from __future__ import annotations

import functools
import json
import os
import shutil
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from tests.helpers import update_goldens
from tests.helpers.goldens import (
    DBT_MANIFEST,
    GOLDEN_DDL_DIR,
    CompareGoldens,
    Compiled,
    GoldenFileStore,
    UpdateGoldens,
    compile_twice,
)
from tests.helpers.reference_project import project_copy
from tests.helpers.seam_projects import FakeDbt

# Every artifact type has a golden route, and the reference project declares each one.
_TYPES = {"agent", "eval", "plugin", "profile", "semantic_view", "skill", "tool"}


@pytest.fixture(scope="module")
def compiled(tmp_path_factory: pytest.TempPathFactory) -> Compiled:
    real = os.environ.get("SST_GOLDEN_PROJECT")
    if real:
        return compile_twice(Path(real), None)
    return compile_twice(project_copy(tmp_path_factory.mktemp("goldens"), offline=False), DBT_MANIFEST)


def test_the_reference_project_compiles_every_artifact_type(compiled: Compiled) -> None:
    assert compiled.result.success, [item.message for item in compiled.result.diagnostics]
    assert {item.artifact_type for item in compiled.result.compiled} == _TYPES
    assert len(compiled.result.compiled) == 15


def test_every_compiled_payload_equals_its_golden(compiled: Compiled) -> None:
    report = CompareGoldens(GoldenFileStore(), compiled.git_sha).run(compiled.result)
    assert report.passed, "\n\n".join(report.failures)


def test_compiling_the_same_files_twice_reports_the_same_fingerprints(compiled: Compiled) -> None:
    assert compiled.unstable_codes() == []


def test_a_golden_that_drifted_or_went_missing_on_disk_is_reported(compiled: Compiled, tmp_path: Path) -> None:
    expected = tmp_path / "expected"
    shutil.copytree(GOLDEN_DDL_DIR.parent, expected)
    view = expected / "ddl" / "jaffle_minimal.sql"
    view.write_text(view.read_text(encoding="utf-8").replace("Menu products only", "Drifted"), encoding="utf-8")
    source = expected / "eval" / "jaffle_analytics_source.sql"
    source.write_text(
        source.read_text(encoding="utf-8").replace("GROUND_TRUTH VARIANT", "GROUND_TRUTH VARCHAR"), encoding="utf-8"
    )
    tool = expected / "tool" / "menu_docs_search.sql"
    tool.unlink()
    report = CompareGoldens(GoldenFileStore(expected / "ddl"), compiled.git_sha).run(compiled.result)
    failures = "\n\n".join(report.failures)
    assert "-  COMMENT = 'Drifted" in failures
    assert "-  , GROUND_TRUTH VARCHAR NOT NULL" in failures
    assert report.missing == (str(tool),)
    assert len(report.failures) == 3


def test_a_dbt_sst_cannot_identify_starts_once_and_both_compiles_agree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path, offline=False)
    # Packages dbt has not installed are refused before dbt starts (SST-DBT026); this one needs none.
    (project / "packages.yml").unlink()
    dbt = FakeDbt(
        version=CompletedRun(0, "dbt-fusion 2.0.0-beta\n", ""),
        writes=(project / "target" / "manifest.json", json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))),
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.cli.wiring.project.YamlProjectInputs",
        functools.partial(YamlProjectInputs, dbt_runner=dbt),
    )
    compiled = compile_twice(project, None)
    # Only the first compile starts dbt, so only it says it does not know this one (SST-DBT020);
    # the second reads the manifest the first wrote, and the two still agree.
    assert compiled.result.success
    assert "SST-DBT020" in [item.code for item in compiled.result.diagnostics]
    assert compiled.unstable_codes() == []
    assert dbt.runs.count(("dbt", "--version")) == 1
    assert CompareGoldens(GoldenFileStore(), compiled.git_sha).run(compiled.result).passed


def test_update_goldens_creates_and_rewrites_only_what_differs(compiled: Compiled, tmp_path: Path) -> None:
    expected = tmp_path / "expected"
    shutil.copytree(GOLDEN_DDL_DIR.parent, expected)
    ddl = expected / "ddl"
    minimal = ddl / "jaffle_minimal.sql"
    lines = minimal.read_text(encoding="utf-8").splitlines(keepends=True)
    header = "".join(line for line in lines if line.startswith("--"))
    minimal.write_text(minimal.read_text(encoding="utf-8").replace("Menu products only", "Drifted"), encoding="utf-8")
    (ddl / "jaffle_menu.sql").unlink()
    store = GoldenFileStore(ddl)
    written = UpdateGoldens(store, compiled.git_sha).run(compiled.result)
    assert written == (str(ddl / "jaffle_menu.sql"), str(minimal))
    assert minimal.read_text(encoding="utf-8").startswith(header)
    assert CompareGoldens(store, compiled.git_sha).run(compiled.result).passed
    assert UpdateGoldens(store, compiled.git_sha).run(compiled.result) == ()


@pytest.mark.parametrize("value", ["true", "1", "yes"])
def test_update_goldens_refuses_to_run_in_ci(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("CI", value)
    assert update_goldens.main() == 1
