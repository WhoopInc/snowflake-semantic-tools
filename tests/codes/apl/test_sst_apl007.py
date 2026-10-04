"""SST-APL007: the certification tag a skill publish applies after creating its version failed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl007_fires() -> None:
    port = FakeSnowflake(existing=())
    port.refused = ("SET TAG",)
    _, result, _ = publish_skill(port, compiled_skill(certified=True))
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL007"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.startswith(
        "skill:month-close: tags could not be applied: ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE VERSION"
    )
    assert " SET TAG SNOWFLAKE.CORE.CERTIFICATION_STATUS = " in diagnostic.message
    assert " failed: recorded refusal: " in diagnostic.message


def test_sst_apl007_silent() -> None:
    _, result, _ = publish_skill(FakeSnowflake(existing=()), compiled_skill(certified=True))
    assert [item.code for item in result.diagnostics] == [] and result.success
