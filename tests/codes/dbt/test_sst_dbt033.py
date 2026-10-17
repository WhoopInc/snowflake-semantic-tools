"""SST-DBT033: a part of the dbt manifest SST reads is not the object or list dbt writes."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest, model_node


def _refused(document: object) -> list[str]:
    with pytest.raises(ProjectError) as raised:
        catalog_from_document(document)
    assert all(item.severity is Severity.ERROR for item in raised.value.diagnostics)
    return [f"{item.code}: {item.message}" for item in raised.value.diagnostics]


def test_sst_dbt033_fires() -> None:
    assert _refused([]) == ["SST-DBT033: dbt manifest root must be an object"]
    assert _refused({}) == ["SST-DBT033: dbt manifest metadata must be an object"]


def test_sst_dbt033_fires_for_a_meta_sst_list(tmp_path: Path) -> None:
    node = model_node("products", meta={"primary_key": ["products_id"], "synonyms": "items"})
    assert _refused(manifest({"model.fixture.products": node})) == [
        "SST-DBT033: dbt manifest nodes.model.fixture.products.config.meta.sst.synonyms must be a list"
    ]


def test_sst_dbt033_silent() -> None:
    assert [model.name for model in catalog_from_document(manifest()).models] == ["products"]
