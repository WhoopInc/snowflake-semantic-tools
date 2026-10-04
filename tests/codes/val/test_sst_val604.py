"""SST-VAL604: a defined member has nothing to create it from."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val604_fires() -> None:
    member = procedure_member(reference=False, body_file=None, body=None)
    [diagnostic] = coded(checked_tools(group("platform", member)), "SST-VAL604")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup' is under define: and declares neither on: nor body_file:"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val604_silent() -> None:
    assert coded(checked_tools(group("platform", procedure_member(reference=False))), "SST-VAL604") == []
