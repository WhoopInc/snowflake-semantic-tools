"""SST-DBT017: the dbt manifest's schema version is not one SST reads, unless allowed for one run."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import SUPPORTED_SCHEMA, catalog_from_document
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity

V13 = "https://schemas.getdbt.com/dbt/manifest/v13.json"


def _document(schema: str) -> dict[str, object]:
    return {"metadata": {"dbt_schema_version": schema}, "nodes": {}}


def test_sst_dbt017_fires() -> None:
    with pytest.raises(ProjectError) as raised:
        catalog_from_document(_document(V13))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT017", Severity.ERROR)
    assert diagnostic.message == f"manifest schema '{V13}'; supported: {SUPPORTED_SCHEMA}"


def test_sst_dbt017_silent() -> None:
    assert catalog_from_document(_document(SUPPORTED_SCHEMA)).models == ()
    assert catalog_from_document(_document(V13), allow_unsupported_schema=True).schema_version == V13
