"""SST-IMP010: a semantic view's table is no dbt model of the manifest, so the view is not imported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import dbt_model, index, view_document

_VIEW = QualifiedName.parse("DB.SV.ORDERS_VIEW")
_PLACE = ViewPlacement("semantic_models", Destination(()))


def test_sst_imp010_fires() -> None:
    imported = import_semantic_view(_VIEW, view_document(), index(), _PLACE)
    [diagnostic] = imported.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP010", Severity.ERROR)
    assert diagnostic.message == (
        "semantic_view:orders_view: table DB.MARTS.ORDERS is no model of the dbt manifest for target dev; "
        "the view was not imported"
    )
    assert imported.refused and not imported.files


def test_sst_imp010_silent() -> None:
    model = dbt_model("orders", "DB.MARTS.ORDERS")
    project = index(models={("DB", "MARTS", "ORDERS"): "orders"}, dbt_models={"orders": model})
    imported = import_semantic_view(_VIEW, view_document(), project, _PLACE)
    assert "SST-IMP010" not in [item.code for item in imported.diagnostics]
