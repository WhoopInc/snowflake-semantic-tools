"""SST-PLN026: the stage a skill bundle is published to is not an internal SSE stage."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.lifecycle_codes import BUNDLE_STAGE, lifecycle_state, month_close, plan_skills
from tests.helpers.snowflake_fake import FakeSnowflake


def _planned(stage_type: str) -> list[str]:
    port = FakeSnowflake(existing=())
    port.stage_types[BUNDLE_STAGE.sql] = stage_type
    return [item.code for item in plan_skills(port, month_close(), lifecycle_state()).diagnostics]


def test_sst_pln026_fires() -> None:
    port = FakeSnowflake(existing=())
    port.stage_types[BUNDLE_STAGE.sql] = "INTERNAL"
    [diagnostic] = plan_skills(port, month_close(), lifecycle_state()).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN026", Severity.ERROR)
    assert diagnostic.message == "skill:month-close: stage DB.S.SKILL_BUNDLES is INTERNAL, expected INTERNAL NO CSE"


def test_sst_pln026_silent() -> None:
    assert "SST-PLN026" not in _planned("INTERNAL NO CSE")
