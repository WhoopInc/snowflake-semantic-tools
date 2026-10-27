"""SST-VAL334: a fact declares access_modifier: private_access."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import edited_manifest, project_copy, reported, set_column_meta


def _modifier(tmp_path: Path, column: str, value: str) -> Path:
    return edited_manifest(
        tmp_path, lambda document: set_column_meta(document, "orders", column, access_modifier=value)
    )


def test_sst_val334_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(project_copy(tmp_path), "SST-VAL334", _modifier(tmp_path, "order_total", "private_access"))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dbt_model:orders: fact 'order_total' declares access_modifier 'private_access'; "
        "SST does not render a private fact"
    )
    assert diagnostic.subject == "dbt_column:orders.order_total"
    assert (
        reported(project_copy(tmp_path / "again"), "SST-PRS004", _modifier(tmp_path, "order_total", "private_access"))
        == []
    )


def test_sst_val334_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert reported(project, "SST-VAL334", _modifier(tmp_path, "order_total", "public_access")) == []
    # A private dimension is SST-VAL222's to report, not this code's.
    assert reported(project, "SST-VAL334", _modifier(tmp_path, "order_state", "private_access")) == []


@pytest.mark.parametrize("values", [{"exclude": True}, {"column_type": None}], ids=["excluded", "untyped"])
def test_sst_val334_silent_on_a_column_that_never_renders(tmp_path: Path, values: dict[str, object]) -> None:
    # An excluded column, or one with no column_type, is left out of the view, so its
    # modifier publishes nothing.
    manifest = edited_manifest(
        tmp_path,
        lambda document: set_column_meta(document, "orders", "order_total", access_modifier="private_access", **values),
    )
    assert reported(project_copy(tmp_path), "SST-VAL334", manifest) == []
