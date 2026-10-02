"""SST-VAL222: a dimension declares access_modifier: private_access."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.semantic_projects import found, manifest, set_column_meta


def _modifier(tmp_path: Path, value: str) -> Path:
    return manifest(
        tmp_path, lambda document: set_column_meta(document, "orders", "order_state", access_modifier=value)
    )


def test_sst_val222_fires(tmp_path: Path) -> None:
    [diagnostic] = found(project_copy(tmp_path), "SST-VAL222", _modifier(tmp_path, "private_access"))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dbt_model:orders: dimension 'order_state' declares access_modifier 'private_access'; "
        "SST does not support a private dimension"
    )
    assert diagnostic.subject == "dbt_column:orders.order_state"


def test_sst_val222_silent(tmp_path: Path) -> None:
    assert found(project_copy(tmp_path), "SST-VAL222", _modifier(tmp_path, "public_access")) == []
