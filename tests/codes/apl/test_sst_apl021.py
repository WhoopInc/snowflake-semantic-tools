"""SST-APL021: a skill's readers hold grants while its certification did not succeed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.recorded_snowflake import RecordedSnowflake

READERS = {"DB.S.MONTH_CLOSE": (GrantRow("READ", "ROLE", "ANALYST"),)}


def test_sst_apl021_fires() -> None:
    port = RecordedSnowflake(existing=(), grants=READERS)
    port.refused = ("SET TAG",)
    _, result, _ = publish_skill(port, compiled_skill(certified=True))
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL021"]
    assert (diagnostic.severity, diagnostic.message) == (
        Severity.ERROR,
        "skill:month-close: grants were issued and certification did not succeed",
    )


def test_sst_apl021_silent() -> None:
    # Granted and certified, or refused certification with nobody granted: neither is out of order.
    _, certified, _ = publish_skill(RecordedSnowflake(existing=(), grants=READERS), compiled_skill(certified=True))
    ungranted = RecordedSnowflake(existing=())
    ungranted.refused = ("SET TAG",)
    _, refused, _ = publish_skill(ungranted, compiled_skill(certified=True))
    assert "SST-APL021" not in [item.code for item in (*certified.diagnostics, *refused.diagnostics)]
