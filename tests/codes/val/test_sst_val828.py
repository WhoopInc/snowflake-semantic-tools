"""SST-VAL828: a publication channel's change has no outcome of its own, or several.

Each channel reports its own outcome and any failure fails the deploy, so the run's result
shows every channel's state. Apply accounts for each skill and plugin change by key
once the waves ran, so a dropped or doubled outcome fails the run instead of reading as success.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.publication import channel_outcome_diagnostics
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_val828_fires() -> None:
    changes = (("skill:month-close", "skill"), ("plugin:finance-kit", "plugin"))
    [missing, doubled] = channel_outcome_diagnostics(changes, ["plugin:finance-kit", "plugin:finance-kit"])
    assert (missing.code, missing.severity, missing.subject) == ("SST-VAL828", Severity.ERROR, "skill:month-close")
    assert missing.message == "skill 'month-close': the catalog channel reported no outcomes"
    assert doubled.message == "skill 'finance-kit': the catalog channel reported 2 outcomes"


def test_sst_val828_silent() -> None:
    assert channel_outcome_diagnostics((("skill:a", "skill"), ("semantic_view:v", "semantic_view")), ["skill:a"]) == ()
    _, result, _ = publish_skill(FakeSnowflake(existing=()), compiled_skill())
    assert result.success, result.outcomes
    assert "SST-VAL828" not in [item.code for item in result.diagnostics]
