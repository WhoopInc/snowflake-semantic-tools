"""SST-SNO036: Snowflake refused a semantic view whose sample values its members' types cannot hold."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal

# The message Snowflake gives, as a publish of analytics-dbt met it.
REFUSED = (
    "SQL compilation error:\nSample Values validation failed: Invalid sample values for calculations "
    "'SINGLE_CUSTOMER_VIEW.TRIAL_PLAN_START_DATE': ['NaT']\n'USER_JOURNAL_RESPONSES.BEHAVIOR_MAGNITUDE_RESPONSE': "
    "['0,2', '0,5']."
)


def test_sst_sno036_fires() -> None:
    [diagnostic] = refusal(driver_error(REFUSED, errno=10913, sqlstate="42601"))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO036", Severity.ERROR)
    assert diagnostic.message == (
        "Snowflake cannot read these sample values as their members' types: "
        "'SINGLE_CUSTOMER_VIEW.TRIAL_PLAN_START_DATE': ['NaT']\n"
        "'USER_JOURNAL_RESPONSES.BEHAVIOR_MAGNITUDE_RESPONSE': ['0,2', '0,5']"
    )
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno036_fires_on_the_wording_alone() -> None:
    assert [item.code for item in refusal(driver_error(REFUSED))] == ["SST-SNO036"]


def test_sst_sno036_silent() -> None:
    other = "SQL compilation error: Invalid argument types for function 'SUM': (TIMESTAMP_NTZ(9))"
    codes = [item.code for item in refusal(driver_error(other, errno=10913, sqlstate="42601"))]
    assert codes == ["SST-SNO009"]
