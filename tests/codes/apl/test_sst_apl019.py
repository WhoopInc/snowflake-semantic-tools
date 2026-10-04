"""SST-APL019: a published skill version still holds a file the bundle no longer has."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.snowflake_fake import FakeSnowflake


class LeftoverFile(FakeSnowflake):
    """A version that holds the bundle and one file a removal should have taken out."""

    def list_location(self, location: str) -> tuple[str, ...]:
        listed = super().list_location(location)
        return (*listed, "skills/month-close/old.md") if location.startswith("snow://") else listed


def test_sst_apl019_fires() -> None:
    _, result, _ = publish_skill(LeftoverFile(existing=()), compiled_skill())
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL019"]
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == "skill:month-close: 'skills/month-close/old.md' could not be removed from the published version"
    )


def test_sst_apl019_silent() -> None:
    _, result, _ = publish_skill(FakeSnowflake(existing=()), compiled_skill())
    assert "SST-APL019" not in [item.code for item in result.diagnostics]
