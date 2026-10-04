"""SST-VAL601: one group declares a member name twice."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val601_fires() -> None:
    [diagnostic] = coded(
        checked_tools(group("platform", procedure_member("lookup"), procedure_member("Lookup"))), "SST-VAL601"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool group 'platform': member 'lookup' is declared twice"


def test_sst_val601_silent() -> None:
    assert (
        coded(checked_tools(group("platform", procedure_member("lookup"), procedure_member("tier"))), "SST-VAL601")
        == []
    )
