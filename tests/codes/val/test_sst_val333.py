"""SST-VAL333: a column's access_modifier is neither public_access nor private_access."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import edited_manifest, load, project_copy, reported, set_column_meta


def _modifier(tmp_path: Path, value: str) -> Path:
    return edited_manifest(
        tmp_path, lambda document: set_column_meta(document, "orders", "order_state", access_modifier=value)
    )


def test_sst_val333_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(project_copy(tmp_path), "SST-VAL333", _modifier(tmp_path, "bogus"))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "dbt_model:orders: column 'order_state' declares access_modifier 'bogus'"
    assert diagnostic.subject == "dbt_column:orders.order_state"


def test_sst_val333_silent(tmp_path: Path) -> None:
    found = load(project_copy(tmp_path), _modifier(tmp_path, "public_access")).diagnostics
    assert [item for item in found if item.code == "SST-VAL333"] == []
    # An explicit public modifier is read, not reported as a key SST does not read.
    assert [item for item in found if "access_modifier" in item.message] == []
