"""SST-VAL612: DDL is rendered for a referenced member."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.tool import reference_ddl
from tests.helpers.agent_builders import procedure_member
from tests.helpers.diagnostic_filters import coded


def test_sst_val612_fires() -> None:
    [diagnostic] = coded(reference_ddl((procedure_member(),)), "SST-VAL612")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup' is under reference: and DDL was rendered for it"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val612_silent() -> None:
    assert coded(reference_ddl((procedure_member(reference=False),)), "SST-VAL612") == []
