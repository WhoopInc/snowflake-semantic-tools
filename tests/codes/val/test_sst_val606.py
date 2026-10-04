"""SST-VAL606: an immutable group declares defined members."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, procedure_member, search_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val606_fires() -> None:
    [diagnostic] = coded(checked_tools(group("vendor", search_member(), immutable=True)), "SST-VAL606")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool group 'vendor' is immutable: true and declares define:"
    assert diagnostic.subject == "tool_group:vendor"


def test_sst_val606_silent() -> None:
    assert coded(checked_tools(group("vendor", procedure_member(), immutable=True)), "SST-VAL606") == []
