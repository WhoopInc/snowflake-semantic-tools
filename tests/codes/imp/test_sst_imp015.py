"""SST-IMP015: sample values of PII-tagged columns are dropped, as a PII column publishes none."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import dbt_column, dbt_model, index, view_document

_VIEW = QualifiedName.parse("DB.SV.ORDERS_VIEW")
_PLACE = ViewPlacement("semantic_models", Destination(()))
_EMAIL = {"name": "EMAIL", "expr": "ORDERS.EMAIL", "sample_values": ["a@b.com"]}


def _imported(pii: bool) -> object:
    model = dbt_model("orders", "DB.MARTS.ORDERS", dbt_column("email", pii_tagged=pii))
    project = index(models={("DB", "MARTS", "ORDERS"): "orders"}, dbt_models={"orders": model})
    return import_semantic_view(_VIEW, view_document(_EMAIL), project, _PLACE)


def test_sst_imp015_fires() -> None:
    imported = _imported(pii=True)
    [diagnostic] = [item for item in imported.diagnostics if item.code == "SST-IMP015"]  # type: ignore[attr-defined]
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "semantic_view:orders_view: dropped the sample values of 1 PII-tagged column(s)"
    [edit] = imported.model_edits  # type: ignore[attr-defined]
    assert all(key != "sample_values" for column in edit.columns for key, _ in column.meta)


def test_sst_imp015_silent() -> None:
    imported = _imported(pii=False)
    assert "SST-IMP015" not in [item.code for item in imported.diagnostics]  # type: ignore[attr-defined]
