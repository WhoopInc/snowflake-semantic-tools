"""SST-VAL612: DDL is rendered for a referenced member."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.tool import reference_ddl
from tests.helpers.agent_builders import found, procedure_member


def test_sst_val612_fires() -> None:
    [diagnostic] = found(reference_ddl((procedure_member(),)), "SST-VAL612")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup' is under reference: and DDL was rendered for it"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val612_silent() -> None:
    assert found(reference_ddl((procedure_member(reference=False),)), "SST-VAL612") == []
