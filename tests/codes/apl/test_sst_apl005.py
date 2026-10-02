"""SST-APL005: a saved plan was computed for another target than the one apply connects to."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state import SavedPlan
from tests.helpers.artifact_builders import change, changeset, rendered, target


def saved_for(role: str | None) -> SavedPlan:
    return SavedPlan.from_changeset(replace(changeset(change(rendered())), target=replace(target(), role=role)))


def test_sst_apl005_fires() -> None:
    mismatch = saved_for("OTHER").check_applicable(saved_for(None), source="plan.json")
    assert mismatch is not None
    [diagnostic] = mismatch.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL005", Severity.ERROR)
    assert diagnostic.message == (
        "plan.json: plan target 'verify/ACCOUNT/DB/SCHEMA/OTHER' differs from the apply target "
        "'verify/ACCOUNT/DB/SCHEMA'"
    )


def test_sst_apl005_silent() -> None:
    assert saved_for(None).check_applicable(saved_for(None), source="plan.json") is None
