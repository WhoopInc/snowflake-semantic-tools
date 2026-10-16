"""SST-PLN032: under --partial, an artifact with errors, or depending on one, is left out of the plan."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.partial import partial_changeset, partial_split
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.diagnostic_filters import only
from tests.helpers.plan_codes import partial_result as result


def test_sst_pln032_fires() -> None:
    split = partial_split(result(D("SST-REF044", subject="semantic_view:menu", artifact="menu", found="x")))
    assert split is not None
    [menu, *_] = [item for item in split.notices if item.subject == "semantic_view:menu"]
    assert (menu.code, menu.severity) == ("SST-PLN032", Severity.INFO)
    assert menu.message == (
        "semantic_view:menu has errors, or depends on something that does, so this partial run leaves it unpublished"
    )
    assert [item.subject for item in split.notices] == ["agent:analyst", "eval:analyst", "semantic_view:menu"]


def test_sst_pln032_silent() -> None:
    split = partial_split(result(D("SST-VAL804", subject="skill:x", artifact="skill:x", value="SST_1")))
    assert split is not None and split.notices == ()


def test_sst_pln032_fires_for_a_change_the_plan_blocked_against_the_target() -> None:
    out_of_band = D("SST-PLN014", artifact="semantic_view:people")
    people = rendered("PEOPLE")
    blocked = replace(
        change(people, Action.UPDATE),
        action=Action.BLOCKED,
        reason=ChangeReason.VALIDATION_ERRORS,
        diagnostics=DiagnosticBag((out_of_band,)),
    )
    plan = replace(
        changeset(blocked, change(rendered("FINANCE"), Action.UPDATE)), diagnostics=DiagnosticBag((out_of_band,))
    )
    narrowed, left_out = partial_changeset(plan)
    notice = only(DiagnosticBag(left_out), "SST-PLN032")
    assert (notice.severity, notice.subject) == (Severity.INFO, "semantic_view:people")
    assert [item.key for item in narrowed.changes] == ["semantic_view:finance"]
