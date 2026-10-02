"""Plan's preflight decisions, notices, impact scope, and order checks, decided in the domain."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason, OwnershipMarker
from snowflake_semantic_tools.domain.plan import order
from snowflake_semantic_tools.domain.plan.impact import impact_scope, modified_keys
from snowflake_semantic_tools.domain.plan.preflight import Preflight, check_preflight, required_privilege
from snowflake_semantic_tools.domain.plan.summary import plan_notices, plan_summary
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from tests.helpers.artifact_builders import change, rendered
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view
from tests.helpers.preflight import PreflightAnswers, PreflightDouble


def _codes(diagnostics: tuple[Diagnostic, ...]) -> list[str]:
    return [item.code for item in diagnostics]


def test_a_create_needs_the_create_privilege_of_its_object_type_and_nothing_else_is_checked() -> None:
    created = change(rendered("SALES"))
    assert created.rendered is not None
    assert required_privilege(created) == "CREATE SEMANTIC VIEW"
    assert required_privilege(change(rendered("SALES"), Action.UPDATE)) is None
    composite = replace(created, rendered=replace(created.rendered, object_type=""))
    assert required_privilege(composite) is None
    assert required_privilege(replace(created, rendered=None)) is None


def test_one_missing_scope_is_reported_once_and_blocks_every_write_in_it() -> None:
    first, second = change(rendered("SALES")), change(rendered("ORDERS"))
    checked, diagnostics = check_preflight(
        (first, second), Preflight("dev", "R", missing_schemas=frozenset((("DB", "SCHEMA"),)))
    )
    assert _codes(diagnostics) == ["SST-PLN010"]
    assert [item.action for item in checked] == [Action.BLOCKED, Action.BLOCKED]
    assert [_codes(item.diagnostics) for item in checked] == [["SST-PLN010"], ["SST-PLN010"]]


def test_a_blocked_write_is_not_also_reported_for_a_privilege_and_unwritten_changes_are_skipped() -> None:
    sales = change(rendered("SALES"))
    noop = replace(change(rendered("ORDERS")), action=Action.NOOP)
    preflight = Preflight(
        "dev",
        "R",
        missing_relations=MappingProxyType({sales.key: (QualifiedName.parse("DB.SCHEMA.T"),)}),
        missing_privileges=MappingProxyType({("DB", "SCHEMA"): ("CREATE SEMANTIC VIEW",)}),
    )
    checked, diagnostics = check_preflight((sales, noop), preflight)
    assert _codes(diagnostics) == ["SST-PLN007"]
    assert checked[1] is noop


def test_an_update_never_warns_of_a_taken_name_and_a_report_only_prune_never_of_references() -> None:
    update = change(rendered("SALES"), Action.UPDATE)
    report_only = replace(change(rendered("OLD"), Action.PRUNE), prune_executable=False)
    unobserved = change(rendered("GONE"), Action.PRUNE)
    preflight = Preflight(
        "dev",
        "R",
        occupied=frozenset((update.key,)),
        referenced=MappingProxyType(
            {key: (QualifiedName.parse("BI.S.X"),) for key in (report_only.key, unobserved.key)}
        ),
    )
    checked, diagnostics = check_preflight((update, report_only, unobserved), preflight)
    assert diagnostics == () and checked == (update, report_only, unobserved)


def test_a_preflight_block_propagates_to_what_depends_on_the_blocked_write() -> None:
    base = view("base")
    top = view("top", depends_on=(base.key,))
    planned = plan(
        (base, top),
        preflight=Preflight(
            "dev", "R", missing_relations=MappingProxyType({base.key: (QualifiedName.parse("A.B.C"),)})
        ),
    )
    assert {item.key: item.reason for item in planned.changes} == {
        base.key: ChangeReason.VALIDATION_ERRORS,
        top.key: ChangeReason.DEPENDENCY_BLOCKED,
    }


def test_the_summary_counts_each_type_in_name_order() -> None:
    sales = view("sales")
    orphan = view("orphan")
    planned_manifest = manifest_of(sales)
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    planned = plan(
        (sales,),
        observed=(marked,),
        applied={orphan.key: entry(orphan, "a" * 64)},
        include_prune=True,
        manifest=planned_manifest,
    )
    agent = Change("agent:a", "agent", Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, None, None, (), 1)
    widened = replace(planned, changes=(*planned.changes, agent))
    assert plan_summary(widened) == (
        "agent: 0 create, 1 replace, 0 noop, 0 prune; semantic_view: 1 create, 0 replace, 0 noop, 1 prune"
    )


def test_impact_covers_what_a_manifest_adds_or_renders_differently() -> None:
    kept, changed, added = view("kept"), view("changed"), view("added")
    previous = manifest_of(kept, changed)
    current = manifest_of(kept, replace(changed, fingerprint="f" * 64), added)
    assert modified_keys(previous, current) == {changed.key, added.key}
    keys, notice = impact_scope(previous, current)
    assert keys == frozenset((changed.key, added.key)) and notice is None


def test_an_order_that_violates_a_dependency_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    base = change(rendered("BASE"))
    top = change(rendered("TOP", depends_on=(base.key, "semantic_view:outside")))
    assert order.check_order((base, top)) is None
    monkeypatch.setattr(order, "topological_order", lambda changes: ((top, base), ()))
    ordered, diagnostic = order.order_changes((base, top))
    assert ordered == () and diagnostic is not None and diagnostic.code == "SST-PLN900"


def test_the_preflight_double_satisfies_the_port() -> None:
    port: PreflightPort = _Port()
    assert port.database_exists(rendered().target.database)


class _Port(PreflightDouble):
    def __init__(self) -> None:
        self.preflight = PreflightAnswers()

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        return False

    def current_role(self) -> str:
        return "R"


def test_each_preflight_finding_reports_its_code() -> None:
    created, locked, other_db = view("created"), view("locked"), view("elsewhere", database="gone")
    owned = view("owned")
    planned_manifest = manifest_of(created, locked, other_db)
    preflight = Preflight(
        "dev",
        "R",
        missing_databases=frozenset(("GONE",)),
        missing_privileges=MappingProxyType({("DB", "SCH"): ("CREATE SEMANTIC VIEW",)}),
        occupied=frozenset((locked.key,)),
        locked=frozenset((locked.target.folded,)),
        referenced=MappingProxyType({owned.key: (QualifiedName.parse("BI.S.X"),)}),
        warehouse="WH",
        warehouse_usable=False,
    )
    planned = plan(
        (created, locked, other_db),
        observed=(live(owned, marker=OwnershipMarker("a" * 64, owned.fingerprint)),),
        applied={owned.key: entry(owned, "a" * 64)},
        include_prune=True,
        preflight=preflight,
        manifest=planned_manifest,
    )
    assert sorted(_codes(planned.diagnostics)) == sorted(
        ["SST-PLN011", "SST-PLN008", "SST-PLN008", "SST-PLN009", "SST-PLN019", "SST-PLN017", "SST-PLN021", "SST-PLN012"]
    )


def test_notices_name_each_unchanged_artifact_then_the_counts() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    planned = plan(
        (sales,),
        observed=(marked,),
        applied={sales.key: entry(sales, planned_manifest.manifest_id)},
        manifest=planned_manifest,
    )
    assert _codes(plan_notices(planned)) == ["SST-PLN015", "SST-PLN016"]
    cyclic = plan((view("a", depends_on=("semantic_view:b",)), view("b", depends_on=("semantic_view:a",))))
    assert plan_notices(cyclic) == ()


def test_two_changes_for_one_key_cannot_be_ordered_and_no_previous_manifest_plans_everything() -> None:
    first = change(rendered("SALES"))
    ordered, diagnostic = order.order_changes((first, replace(first, key=first.key.upper())))
    assert ordered == () and diagnostic is not None and diagnostic.code == "SST-PLN022"
    keys, notice = impact_scope(None, manifest_of())
    assert keys is None and notice is not None and notice.code == "SST-PLN006"
