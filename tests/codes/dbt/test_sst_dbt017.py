"""SST-DBT017: the manifest's schema version is one SST does not support."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest

V13 = "https://schemas.getdbt.com/dbt/manifest/v13.json"


def test_sst_dbt017_fires() -> None:
    document = manifest()
    document["metadata"]["dbt_schema_version"] = V13
    with pytest.raises(ProjectError) as raised:
        catalog_from_document(document)
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT017", Severity.ERROR)
    assert diagnostic.message == f"manifest schema '{V13}'; supported: v12"
    assert diagnostic.subject is None


def test_sst_dbt017_silent() -> None:
    document = manifest()
    document["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json"
    assert catalog_from_document(document).model("products") is not None
    document["metadata"]["dbt_schema_version"] = V13
    assert catalog_from_document(document, allow_unsupported_schema=True).model("products") is not None
