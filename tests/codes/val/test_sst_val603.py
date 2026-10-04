"""SST-VAL603: a member declares a type SST does not know."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val603_fires() -> None:
    [diagnostic] = coded(checked_tools(group("platform", replace(procedure_member(), type="lambda"))), "SST-VAL603")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup': type 'lambda' is not a known tool type"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val603_silent() -> None:
    assert coded(checked_tools(group("platform", procedure_member())), "SST-VAL603") == []
