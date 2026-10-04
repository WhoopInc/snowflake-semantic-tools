"""SST-PRT006: no dbt manifest exists where SST reads one."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import DBT_MANIFEST


def test_sst_prt006_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        load_manifest_catalog(tmp_path / "target" / "manifest.json")
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRT006", Severity.ERROR)
    assert diagnostic.message == f"no dbt manifest at {tmp_path / 'target' / 'manifest.json'}"


def test_sst_prt006_silent() -> None:
    assert load_manifest_catalog(DBT_MANIFEST).models
