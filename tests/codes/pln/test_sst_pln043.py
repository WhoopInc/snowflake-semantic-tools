"""SST-PLN043: `--overwrite` publishes the planned definition over an object changed out of band."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity, resolve_severities
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def test_sst_pln043_fires() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    recorded = {sales.key: entry(sales, planned_manifest.manifest_id)}
    planned = plan((sales,), observed=(live(sales),), applied=recorded, manifest=planned_manifest, overwrite=True)
    [diagnostic] = coded(planned.diagnostics, "SST-PLN043")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:sales was changed out of band (its comment was changed and no longer carries SST's "
        "ownership marker); --overwrite publishes the planned definition over it"
    )
    assert diagnostic.subject == "semantic_view:sales"
    assert (planned.changes[0].action, planned.changes[0].reason) == (Action.UPDATE, ChangeReason.OVERWRITE)
    assert coded(planned.diagnostics, "SST-PLN014") == []
    # The flag asked for it, so strict mode does not turn it into an error that refuses the apply.
    promoted, count = resolve_severities(DiagnosticBag((diagnostic,)), strict=True)
    assert ([item.severity for item in promoted], count) == ([Severity.WARNING], 0)


def test_sst_pln043_silent() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    recorded = {sales.key: entry(sales, planned_manifest.manifest_id)}
    # Without the flag the object is blocked, as SST-PLN014 says.
    blocked = plan((sales,), observed=(live(sales),), applied=recorded, manifest=planned_manifest)
    assert coded(blocked.diagnostics, "SST-PLN043") == []
    assert blocked.changes[0].action is Action.BLOCKED
    # With it, an object that is not blocked is planned as it would be: the flag touches nothing else.
    created = plan((sales,), overwrite=True)
    assert coded(created.diagnostics, "SST-PLN043") == []
    assert created.changes[0].action is Action.CREATE
    unmanaged = plan((sales,), observed=(live(sales),), manifest=planned_manifest, overwrite=True)
    assert coded(unmanaged.diagnostics, "SST-PLN043") == []
    assert (unmanaged.changes[0].action, unmanaged.changes[0].reason) == (
        Action.BLOCKED,
        ChangeReason.UNMANAGED_OBJECT,
    )
