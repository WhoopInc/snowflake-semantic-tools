"""SST-VAL841: catalog serves a different version.

Once a version is certified, the catalog serves the latest certified version whatever the
default, so a later version published uncertified is not what catalog users get. Without a
certified version apply makes the published version the default, so a revert warns of nothing.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.skills import CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from tests.helpers.artifact_builders import empty_state
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import CATALOG_CHANNEL, compile_extensions, publish_extensions, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake

FIRST = compile_extensions(skill_catalog(skill()))
SECOND = compile_extensions(skill_catalog(skill(files={"reference/steps.md": "Steps, revised.\n"})))


def _certified(catalog: SkillCatalog) -> dict[str, CompiledExtension]:
    result = CompileSkills(catalog, replace(CATALOG_CHANNEL, certified=True)).run_result()
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def test_sst_val841_fires() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, _certified(skill_catalog(skill())), empty_state())
    changeset, _ = publish_extensions(port, SECOND, published)
    diagnostic = only(changeset.diagnostics, "SST-VAL841")
    alias = SECOND["skill:month-close"].release.alias
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        f"skill:month-close: the catalog will serve VERSION$1 of DB.S.MONTH_CLOSE, not {alias}, "
        "because it is the latest certified version"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val841_silent() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, FIRST, empty_state())
    _, changed = publish_extensions(port, SECOND, published)
    # A revert with nothing certified: apply makes its version the default again.
    changeset, _ = publish_extensions(port, FIRST, changed)
    assert "SST-VAL841" not in codes(changeset.diagnostics)
