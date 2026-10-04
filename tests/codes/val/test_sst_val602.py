"""SST-VAL602: one member name is declared in two groups."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val602_fires() -> None:
    found_602 = coded(
        checked_tools(group("platform", procedure_member()), group("partner", procedure_member())), "SST-VAL602"
    )
    [diagnostic] = found_602
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "tool member 'lookup' is declared in platform and partner"


def test_sst_val602_silent() -> None:
    groups = (group("platform", procedure_member()), group("partner", procedure_member("settle")))
    assert coded(checked_tools(*groups), "SST-VAL602") == []
