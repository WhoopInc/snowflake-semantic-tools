"""SST-SNO013: Snowflake reported 250001: authentication failed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno013_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "Failed to connect to DB: Incorrect username or password was specified for user 'DEPLOYER'.", errno=250001
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO013", Severity.ERROR)
    assert diagnostic.message == "authentication failed for DEPLOYER"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno013_silent() -> None:
    assert [
        item.code for item in refusal(driver_error("Failed to execute request: Read timed out.", sqlstate="08001"))
    ] == ["SST-SNO014"]
