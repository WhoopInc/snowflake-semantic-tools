"""SST-VAL555: an allowed orchestration model is in preview."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from tests.helpers.cortex_models import FakeCortexModels, checked, ga


def test_sst_val555_fires() -> None:
    port = FakeCortexModels((CortexBaseModel("grok-4.6", "PUPR"), CortexBaseModel("next-model", "PRPR")))
    found = checked(["grok-4.6", "next-model"], port)
    assert [item.code for item in found] == ["SST-VAL555", "SST-VAL555"]
    assert found[0].severity is Severity.WARNING
    assert found[0].message == "orchestration model 'grok-4.6' is in public preview"
    assert found[1].message == "orchestration model 'next-model' is in private preview"


def test_sst_val555_silent() -> None:
    assert checked(["claude-opus-5"], FakeCortexModels((ga("claude-opus-5"),))) == []
