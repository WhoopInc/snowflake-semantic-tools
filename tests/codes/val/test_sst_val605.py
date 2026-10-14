"""SST-VAL605: a referenced member declares how to create itself."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val605_fires() -> None:
    member = procedure_member(creation_keys=("body",))
    [diagnostic] = coded(checked_tools(member), "SST-VAL605")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup' references an existing object and declares 'body'"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val605_silent() -> None:
    assert coded(checked_tools(procedure_member()), "SST-VAL605") == []
