"""SST-DBT018: the manifest's schema version is absent or names no version."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest


def test_sst_dbt018_fires() -> None:
    document = manifest()
    del document["metadata"]["dbt_schema_version"]
    with pytest.raises(ProjectError) as raised:
        catalog_from_document(document)
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT018", Severity.ERROR)
    assert diagnostic.message == "manifest schema version is absent"
    document["metadata"]["dbt_schema_version"] = "twelve"
    with pytest.raises(ProjectError) as raised:
        catalog_from_document(document)
    assert raised.value.diagnostics[0].message == "manifest schema version is 'twelve'"


def test_sst_dbt018_silent() -> None:
    assert catalog_from_document(manifest()).schema_version.endswith("/v12.json")
