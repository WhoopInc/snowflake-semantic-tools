"""SST-IMP011: a dbt model or column sets a key to another value than the live view; the project's is kept."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import dbt_model, index, view_document

_VIEW = QualifiedName.parse("DB.SV.ORDERS_VIEW")
_PLACE = ViewPlacement("semantic_models", Destination(()))


def _project(description: str) -> object:
    model = dbt_model("orders", "DB.MARTS.ORDERS", description=description)
    return index(models={("DB", "MARTS", "ORDERS"): "orders"}, dbt_models={"orders": model})


def test_sst_imp011_fires() -> None:
    imported = import_semantic_view(_VIEW, view_document(description="Orders."), _project("Each order."), _PLACE)  # type: ignore[arg-type]
    [diagnostic] = [item for item in imported.diagnostics if item.code == "SST-IMP011"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:orders_view: orders.(model) description is Each order. in the project and Orders. live; "
        "the project's is kept"
    )
    [edit] = imported.model_edits or (None,)
    assert edit is None or edit.description is None


def test_sst_imp011_silent() -> None:
    imported = import_semantic_view(_VIEW, view_document(description="Orders."), _project("Orders."), _PLACE)  # type: ignore[arg-type]
    assert "SST-IMP011" not in [item.code for item in imported.diagnostics]
