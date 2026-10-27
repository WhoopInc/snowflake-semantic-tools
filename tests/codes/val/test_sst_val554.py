"""SST-VAL554: an allowed orchestration model is legacy."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from tests.helpers.cortex_models import FakeCortexModels, checked, ga


def test_sst_val554_fires() -> None:
    port = FakeCortexModels((CortexBaseModel("claude-sonnet-4-5", "LEGACY", "2026-09-01", "2027-01-15"),))
    [diagnostic] = checked(["claude-sonnet-4-5"], port)
    assert diagnostic.code == "SST-VAL554"
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "orchestration model 'claude-sonnet-4-5' is legacy; end-of-life: 2027-01-15"


def test_sst_val554_names_no_date_when_none_is_announced() -> None:
    [diagnostic] = checked(["m"], FakeCortexModels((CortexBaseModel("m", "LEGACY"),)))
    assert diagnostic.message == "orchestration model 'm' is legacy; end-of-life: no date announced"


def test_sst_val554_silent() -> None:
    assert checked(["claude-sonnet-4-5"], FakeCortexModels((ga("claude-sonnet-4-5"),))) == []
