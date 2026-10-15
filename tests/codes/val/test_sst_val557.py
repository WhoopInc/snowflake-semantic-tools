"""SST-VAL557: connected validation finds the synonym model listed as a risk."""

from __future__ import annotations

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.app.validate_models import model_check
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cortex_models import FakeCortexModels, agent_compile
from tests.helpers.snowflake_fake import FakeSnowflake


def _validated(config: dict[str, object], port: FakeCortexModels) -> list[Diagnostic]:
    result = ValidateArtifacts(FakeSnowflake(), target="prod", models=model_check(config, Fanout(port))).run(
        agent_compile("sales"), strict=False, connected=True
    )
    return [item for item in result.diagnostics if item.code == "SST-VAL557"]


def test_sst_val557_fires() -> None:
    port = FakeCortexModels((CortexBaseModel("mistral-large2", "LEGACY", "2026-08-12", "2026-10-14"),))
    [diagnostic] = _validated({"enrichment": {"synonym_model": "mistral-large2"}}, port)
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "enrichment.synonym_model 'mistral-large2' is legacy; end-of-life: 2026-10-14"
    assert diagnostic.subject == "config:enrichment.synonym_model"
    # Validation reads the model's lifecycle, and asks it nothing.
    assert port.probed == []


def test_sst_val557_checks_the_default_when_unset() -> None:
    [diagnostic] = _validated({}, FakeCortexModels())
    assert diagnostic.message == "enrichment.synonym_model 'claude-sonnet-4-5' is not in SHOW CORTEX BASE MODELS"


def test_sst_val557_silent() -> None:
    assert _validated({}, FakeCortexModels((CortexBaseModel("claude-sonnet-4-5", "GA"),))) == []
    assert _validated({}, FakeCortexModels(show_error=SnowflakePortError("denied"))) == []
