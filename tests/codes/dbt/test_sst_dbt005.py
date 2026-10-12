"""SST-DBT005: a manifest SST did not write is older than a model file it describes.

SST did not write a manifest given with `--manifest`, nor one read from disk under
`dbt.invoke: false`; one its own `dbt parse` just wrote is never compared.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.projects import project_paths
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest, model_node

SQL = "select 1 as products_id\n"
EDITED = "select 2 as products_id\n"
NOT_INVOKED = "project:\n  semantic_models_dir: semantic_models\ndbt:\n  invoke: false\n"


def _document() -> dict[str, object]:
    node = model_node("products", original_file_path="models/products.sql", package_name="fixture")
    node["checksum"] = {"name": "sha256", "checksum": sha256(SQL.encode()).hexdigest()}
    return manifest({"model.fixture.products": node})


def _project(tmp_path: Path, on_disk: str) -> SmallProject:
    return SmallProject(tmp_path, files={"models/products.sql": on_disk}, document=_document())


def _not_invoked(tmp_path: Path, on_disk: str) -> Path:
    """A `dbt.invoke: false` project whose manifest sits under `target-path`, as dbt left it."""
    files = {"models/products.sql": on_disk, "sst_config.yml": NOT_INVOKED}
    SmallProject(tmp_path, files=files, document=_document()).write()
    target = tmp_path / "target" / "manifest.json"
    target.parent.mkdir()
    target.write_text(json.dumps(_document()), encoding="utf-8")
    return tmp_path


def test_sst_dbt005_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(_project(tmp_path, EDITED).load().diagnostics, "SST-DBT005")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "manifest is older than models/products.sql"
    assert diagnostic.subject == "dbt_model:products"
    assert diagnostic.origin == Origin("models/products.sql")


def test_sst_dbt005_silent(tmp_path: Path) -> None:
    assert coded(_project(tmp_path, SQL).load().diagnostics, "SST-DBT005") == []


def test_sst_dbt005_fires_for_the_manifest_on_disk_under_dbt_invoke_false(tmp_path: Path) -> None:
    project = _not_invoked(tmp_path, EDITED)
    [diagnostic] = coded(YamlProjectSource(project_paths(project)).load_project().diagnostics, "SST-DBT005")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.origin == Origin("models/products.sql")


def test_sst_dbt005_is_a_recorded_warning_under_dbt_invoke_false_when_stale_is_allowed(tmp_path: Path) -> None:
    source = YamlProjectSource(project_paths(_not_invoked(tmp_path, EDITED)), allow_stale_manifest=True)
    [diagnostic] = coded(source.load_project().diagnostics, "SST-DBT005")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.override is Severity.WARNING


def test_sst_dbt005_silent_under_dbt_invoke_false_for_an_unchanged_model(tmp_path: Path) -> None:
    project = _not_invoked(tmp_path, SQL)
    assert coded(YamlProjectSource(project_paths(project)).load_project().diagnostics, "SST-DBT005") == []


def test_sst_dbt005_never_compares_a_manifest_sst_just_parsed(tmp_path: Path) -> None:
    # dbt's own checksum is what it just read, so a mismatch here could only be the fake's.
    SmallProject(tmp_path, files={"models/products.sql": EDITED}).write()
    dbt = FakeDbt(writes=(tmp_path / "target" / "manifest.json", _document()))
    loaded = YamlProjectSource(project_paths(tmp_path), dbt_runner=dbt).load_project()
    assert [run[1] for run in dbt.runs] == ["--version", "parse"]
    assert coded(loaded.diagnostics, "SST-DBT005") == []
