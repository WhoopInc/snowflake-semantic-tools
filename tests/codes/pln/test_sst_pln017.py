"""SST-PLN017: a prune would remove an object something outside the project names."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, OwnershipMarker
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import entry, live, only, plan, view


def _pruned(referrers: tuple[QualifiedName, ...]) -> list[Diagnostic]:
    orphan = view("orphan")
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    preflight = Preflight("dev", "DEPLOYER", referenced=MappingProxyType({orphan.key: referrers} if referrers else {}))
    planned = plan(
        (), observed=(marked,), applied={orphan.key: entry(orphan, "a" * 64)}, include_prune=True, preflight=preflight
    )
    assert [change.action for change in planned.changes] == [Action.PRUNE]
    return only(planned, "SST-PLN017")


def test_sst_pln017_fires() -> None:
    [diagnostic] = _pruned((QualifiedName.parse("BI.DASH.REVENUE"),))
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "DB.SCH.ORPHAN is referenced outside this project"


def test_sst_pln017_silent() -> None:
    assert _pruned(()) == []
