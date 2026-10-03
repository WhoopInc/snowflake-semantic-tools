"""SST-APL017: one channel of a Desktop profile publish failed while the other reported success."""

from __future__ import annotations

import json
from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.publications import compiled_profile, publish_profile
from tests.helpers.snowflake_fake import FakeSnowflake

MISSING = "@DB.S.PROFILES/prompts/analyst/NOPE/AGENTS.md"


class DanglingPointer(FakeSnowflake):
    """The registry row lands, but it points at a tree the stage does not hold."""

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        rows = [dict(row) for row in super().desktop_profile_rows(registry)]
        for row in rows:
            row["SYSTEM_PROMPT_REPO"] = json.dumps({"snowflake_stage": MISSING})
        return tuple(rows)


def test_sst_apl017_fires() -> None:
    _, result, _ = publish_profile(DanglingPointer(existing=()), compiled_profile())
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL017"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"profile:analyst: the registry row 'analyst' was written, but the stage does not hold {MISSING}"
    )


def test_sst_apl017_silent() -> None:
    _, result, _ = publish_profile(FakeSnowflake(existing=()), compiled_profile())
    assert [item.code for item in result.diagnostics] == [] and result.success
