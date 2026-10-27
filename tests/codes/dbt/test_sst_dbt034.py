"""SST-DBT034: a dbt YAML file `sst enrich` updates has a part of another shape than dbt reads."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_model_updates
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import ColumnUpdate

_UPDATES = {"orders": [ColumnUpdate("id", (("column_type", "dimension"),), added=True)]}


def _refused(text: str) -> list[str]:
    with pytest.raises(ProjectError) as raised:
        write_model_updates(text, "models/orders.yml", _UPDATES)
    assert all(item.severity is Severity.ERROR for item in raised.value.diagnostics)
    return [f"{item.code}: {item.message}" for item in raised.value.diagnostics]


def test_sst_dbt034_fires() -> None:
    assert _refused("version: 2\nmodels: {orders: {}}\n") == [
        "SST-DBT034: models/orders.yml: models must be a list to hold the metadata sst enrich writes"
    ]


def test_sst_dbt034_fires_for_each_shape() -> None:
    assert _refused("- not a mapping\n") == [
        "SST-DBT034: models/orders.yml: its root must be a mapping to hold the metadata sst enrich writes"
    ]
    assert _refused("models:\n  - name: orders\n    columns:\n      - name: id\n        config: text\n") == [
        "SST-DBT034: models/orders.yml: config must be a mapping to hold the metadata sst enrich writes"
    ]


def test_sst_dbt034_silent() -> None:
    written = write_model_updates("version: 2\nmodels:\n  - name: orders\n", "models/orders.yml", _UPDATES)
    assert "column_type: dimension" in written.text
