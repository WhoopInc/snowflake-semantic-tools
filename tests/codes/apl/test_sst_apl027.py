"""SST-APL027: the Desktop profile registry, a metadata table SST writes, is absent or the wrong shape."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.publications import compiled_profile, publish_profile
from tests.helpers.snowflake_fake import FakeSnowflake


class NarrowRegistry(FakeSnowflake):
    """Creating the registry leaves a table with only its key column."""

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        super().ensure_profile_registry(qualified_name)
        self.tables[qualified_name.sql] = (("CONFIG_NAME", "VARCHAR"),)


def test_sst_apl027_fires() -> None:
    _, result, _ = publish_profile(NarrowRegistry(existing=()), compiled_profile())
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL027"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.startswith("DB.S.PROFILE_REGISTRY: the registry lacks ACTIVE, ")
    assert diagnostic.message.endswith(" after creation")


def test_sst_apl027_silent() -> None:
    _, result, _ = publish_profile(FakeSnowflake(existing=()), compiled_profile())
    assert "SST-APL027" not in [item.code for item in result.diagnostics]
