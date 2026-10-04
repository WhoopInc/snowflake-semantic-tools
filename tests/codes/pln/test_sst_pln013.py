"""SST-PLN013: an update would replace an object that carries explicit grants."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, GrantRow, OwnershipMarker
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def _update(grants: tuple[GrantRow, ...]) -> list[Diagnostic]:
    current = view("sales")
    before = view("before")
    planned_manifest = manifest_of(current)
    recorded = entry(before, planned_manifest.manifest_id, qualified_name=current.target.sql)
    seen = live(current, marker=OwnershipMarker(planned_manifest.manifest_id, before.fingerprint), grants=grants)
    planned = plan((current,), observed=(seen,), applied={current.key: recorded}, manifest=planned_manifest)
    assert planned.changes[0].action is Action.UPDATE
    return coded(planned.diagnostics, "SST-PLN013")


def test_sst_pln013_fires() -> None:
    [diagnostic] = _update((GrantRow("SELECT", "ROLE", "ANALYST"),))
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:sales: 1 explicit grants exist on DB.SCH.SALES"


def test_sst_pln013_silent() -> None:
    assert _update(()) == []
