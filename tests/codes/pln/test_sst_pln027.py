"""SST-PLN027: a version alias SST would reuse holds files that differ from the bundle."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.lifecycle_codes import lifecycle_state, month_close, plan_skills
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement


def test_sst_pln027_fires() -> None:
    compiled = month_close()
    release = compiled["skill:month-close"].release
    owned = AppliedEntry("f", release.target.sql, "now", "run", "applied", "f", "m")
    port = FakeSnowflake(existing=())
    port.execute_script(
        (
            statement(
                "CREATE CORTEX EXTENSION IF NOT EXISTS DB.S.MONTH_CLOSE TYPE = 'SKILL' COMMENT = 'Close the month.'"
            ),
            statement(f"ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE ADD VERSION {release.alias} FROM @DB.S.EMPTY/"),
        )
    )
    [diagnostic] = plan_skills(port, compiled, lifecycle_state({"skill:month-close": owned})).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN027", Severity.ERROR)
    assert diagnostic.message.startswith(f"skill:month-close: version alias {release.alias} holds files that differ")


def test_sst_pln027_silent() -> None:
    changeset = plan_skills(FakeSnowflake(existing=()), month_close(), lifecycle_state())
    assert "SST-PLN027" not in [item.code for item in changeset.diagnostics]
