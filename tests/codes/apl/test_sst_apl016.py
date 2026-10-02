"""SST-APL016: a multi-statement publish failed part way and left the artifact unusable."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.recorded_snowflake import RecordedSnowflake


class ShortVersion(RecordedSnowflake):
    """A version that holds less than the bundle it was added from."""

    def list_location(self, location: str) -> tuple[str, ...]:
        listed = super().list_location(location)
        return (
            tuple(path for path in listed if not path.endswith("reference__steps.md"))
            if location.startswith("snow://")
            else listed
        )


def test_sst_apl016_fires() -> None:
    _, result, state = publish_skill(ShortVersion(existing=()), compiled_skill())
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL016"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.startswith("skill:month-close: VERSION$2 of DB.S.MONTH_CLOSE differs from the bundle")
    assert "skill:month-close" in state.applied


def test_sst_apl016_silent() -> None:
    _, result, _ = publish_skill(RecordedSnowflake(existing=()), compiled_skill())
    assert "SST-APL016" not in [item.code for item in result.diagnostics]
