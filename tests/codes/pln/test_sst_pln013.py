"""SST-PLN013: an update would replace an object that carries explicit grants, and drop them."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, GrantRow, OwnershipMarker
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view

GRANT = (GrantRow("SELECT", "ROLE", "ANALYST"),)
REPLACE = "create or replace semantic view sales as select 1"


def _update(
    grants: tuple[GrantRow, ...],
    ddl: str,
    preservation: GrantPreservation = GrantPreservation.CLAUSE,
) -> list[Diagnostic]:
    current = replace(view("sales", ddl=ddl), grant_preservation=preservation)
    before = view("before")
    planned_manifest = manifest_of(current)
    recorded = entry(before, planned_manifest.manifest_id, qualified_name=current.target.sql)
    seen = live(current, marker=OwnershipMarker(planned_manifest.manifest_id, before.fingerprint), grants=grants)
    planned = plan((current,), observed=(seen,), applied={current.key: recorded}, manifest=planned_manifest)
    assert planned.changes[0].action is Action.UPDATE
    return coded(planned.diagnostics, "SST-PLN013")


def test_sst_pln013_fires() -> None:
    [diagnostic] = _update(GRANT, REPLACE)
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:sales: replacing DB.SCH.SALES would drop its explicit grants (1)"


def test_sst_pln013_silent() -> None:
    assert _update((), REPLACE) == []


def test_sst_pln013_silent_when_the_replace_says_copy_grants() -> None:
    # The statement apply runs keeps the grants, so there is nothing to confirm.
    assert _update(GRANT, f"{REPLACE}\n  COPY GRANTS") == []


def test_sst_pln013_silent_when_the_type_grants_again_or_never_replaces() -> None:
    assert _update(GRANT, REPLACE, GrantPreservation.REPLAY) == []
    assert _update(GRANT, REPLACE, GrantPreservation.NONE) == []
