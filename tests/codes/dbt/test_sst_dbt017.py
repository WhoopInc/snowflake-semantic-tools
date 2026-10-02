"""SST-DBT017: the dbt manifest's schema version is not the one SST reads."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import SUPPORTED_SCHEMA, catalog_from_document
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity


def _document(schema_version: str) -> dict[str, object]:
    return {"metadata": {"dbt_schema_version": schema_version, "dbt_version": "1.9.0"}, "nodes": {}}


def test_sst_dbt017_fires() -> None:
    older = "https://schemas.getdbt.com/dbt/manifest/v11.json"
    with pytest.raises(ProjectError) as caught:
        catalog_from_document(_document(older))
    [diagnostic] = caught.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT017", Severity.ERROR)
    assert diagnostic.message == f"manifest schema '{older}'; supported: {SUPPORTED_SCHEMA}"


def test_sst_dbt017_silent() -> None:
    assert catalog_from_document(_document(SUPPORTED_SCHEMA)).schema_version == SUPPORTED_SCHEMA
