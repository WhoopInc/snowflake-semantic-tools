"""SST-PLN039: an update would replace a semantic view that carries tags the project does not declare."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, OwnershipMarker
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def _update(observed: tuple[str, ...], declared: tuple[str, ...] = ()) -> list[Diagnostic]:
    current = replace(view("sales"), desired_tags=declared)
    before = view("before")
    planned_manifest = manifest_of(current)
    recorded = entry(before, planned_manifest.manifest_id, qualified_name=current.target.sql)
    marker = OwnershipMarker(planned_manifest.manifest_id, before.fingerprint)
    seen = replace(live(current, marker=marker), tags=observed)
    planned = plan((current,), observed=(seen,), applied={current.key: recorded}, manifest=planned_manifest)
    assert planned.changes[0].action is Action.UPDATE
    return coded(planned.diagnostics, "SST-PLN039")


def test_sst_pln039_fires() -> None:
    [diagnostic] = _update(("GOV.T.PII", "GOV.T.OWNER"), ("gov.t.owner",))
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:sales: replacing DB.SCH.SALES removes tags the project does not declare: GOV.T.PII"
    )


def test_sst_pln039_silent() -> None:
    assert _update(()) == []
    assert _update(("GOV.T.PII",), ("GOV.T.PII",)) == []
