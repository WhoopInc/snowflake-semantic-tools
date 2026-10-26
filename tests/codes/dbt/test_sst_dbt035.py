"""SST-DBT035: the deferred target's manifest is read as it is, since nothing its parse reads changed."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.projects import project_paths
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest, model_node

PROFILES = (
    "fixture:\n  target: dev\n  outputs:\n"
    "    dev:\n      type: snowflake\n      database: DB\n      schema: SCH\n"
    "    prod:\n      type: snowflake\n      database: PROD_DB\n      schema: SCH\n"
)
DEFER = "defer:\n  target: prod\n  state_path: prod_state\n  auto_compile: true\n"


def _runs(tmp_path: Path, extra: str = "") -> tuple[FakeDbt, list[tuple[Diagnostic, ...]]]:
    config = f"project:\n  semantic_models_dir: semantic_models\n{DEFER}{extra}"
    manifest_path = SmallProject(tmp_path, files={"profiles.yml": PROFILES, "sst_config.yml": config}).write()
    document = manifest({"model.fixture.products": model_node("products", "PROD_DB.SCH.PRODUCTS")})
    dbt = FakeDbt(writes=(tmp_path / "prod_state" / "manifest.json", document))
    reports: list[tuple[Diagnostic, ...]] = []
    for _ in range(2):
        source = YamlProjectSource(project_paths(tmp_path), manifest_path=manifest_path, dbt_runner=dbt)
        reports.append(tuple(item for item in source._seam_diagnostics(source.dbt_catalog())))
    return dbt, reports


def test_sst_dbt035_fires(tmp_path: Path) -> None:
    dbt, (first, second) = _runs(tmp_path)
    assert first == ()
    [diagnostic] = second
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT035", Severity.INFO)
    assert diagnostic.message == (
        "read the dbt manifest of deferred target 'prod' as dbt wrote it just now; its inputs are unchanged"
    )
    assert [run[1] for run in dbt.runs].count("parse") == 1


def test_sst_dbt035_silent(tmp_path: Path) -> None:
    dbt, reports = _runs(tmp_path, "  reuse_manifest: false\n")
    assert reports == [(), ()]
    assert [run[1] for run in dbt.runs].count("parse") == 2
