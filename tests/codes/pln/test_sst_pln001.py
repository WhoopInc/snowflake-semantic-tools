"""SST-PLN001: a query that reads current state failed during observation."""

from __future__ import annotations

from snowflake_semantic_tools.app.observe import observe
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.artifact_builders import rendered
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_pln001_fires() -> None:
    port = FakeSnowflake()
    port.fail("show_objects", SnowflakePortError("connection reset"))
    _, [diagnostic, *_] = observe(port, SEMANTIC_REGISTRY, (rendered().target,), fetched_at="now")
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN001", Severity.ERROR)
    assert diagnostic.message.startswith("observation of ")
    assert diagnostic.message.endswith(" in DB.SCHEMA failed: connection reset")


def test_sst_pln001_silent() -> None:
    _, diagnostics = observe(FakeSnowflake(), SEMANTIC_REGISTRY, (rendered().target,), fetched_at="now")
    assert diagnostics == ()
