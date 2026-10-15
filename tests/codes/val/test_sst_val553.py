"""SST-VAL553: an allowed orchestration model is past end-of-life."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from tests.helpers.cortex_models import FakeCortexModels, checked, ga


def test_sst_val553_fires() -> None:
    port = FakeCortexModels((CortexBaseModel("claude-4-sonnet", "EOL", "2026-08-12", "2026-10-14"),))
    [diagnostic] = checked(["claude-4-sonnet"], port)
    assert diagnostic.code == "SST-VAL553"
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "orchestration model 'claude-4-sonnet' is past end-of-life (2026-10-14)"


def test_sst_val553_silent() -> None:
    assert checked(["claude-sonnet-4-5"], FakeCortexModels((ga("claude-sonnet-4-5"),))) == []
