"""SST-VAL606: a member whose settings make it immutable is one SST would define."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import immutable_member, procedure_member, search_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val606_fires() -> None:
    [diagnostic] = coded(checked_tools(immutable_member(search_member())), "SST-VAL606")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'docs_search' is immutable and declares no relations:"
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val606_silent() -> None:
    members = (immutable_member(procedure_member()), search_member())
    assert coded(checked_tools(*members), "SST-VAL606") == []
