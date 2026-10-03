"""SST-VAL825: one extension's version name stands for two different bundles.

A version name is the content digest's leading hex, so two deploys minting it mint the same
files; compile checks every release it mints together, and a later release that reuses an
earlier one's name for other content does not compile.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.validate.publication import MintedVersion, version_collision_diagnostics
from tests.helpers.publications import SKILL_STAGE, skill

TARGET = QualifiedName.parse("DB.S.MONTH_CLOSE")


def _version(key: str, digest: str, target: QualifiedName = TARGET) -> MintedVersion:
    return MintedVersion(key, key.split(":", 1)[1], target, "GIT_0123456789AB", digest)


def test_sst_val825_fires() -> None:
    first = _version("skill:month-close", "0123456789ab" + "0" * 52)
    clash = _version("plugin:month-close", "0123456789ab" + "1" * 52)
    [diagnostic] = version_collision_diagnostics((first, clash))
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL825",
        Severity.ERROR,
        "plugin:month-close",
    )
    assert diagnostic.message == "skill 'month-close': version name 'GIT_0123456789AB' is not collision-free"


def test_sst_val825_silent() -> None:
    # The same name for the same content, or at another extension, is no collision.
    digest = "0123456789ab" + "0" * 52
    elsewhere = QualifiedName.parse("DB.S.OTHER")
    same = (_version("skill:a", digest), _version("skill:b", digest), _version("skill:c", "f" * 64, elsewhere))
    assert version_collision_diagnostics(same) == ()
    catalog = SkillCatalog((skill(), skill("triage", b"other\n")))
    result = CompileSkills(catalog, CatalogChannel("DB", "S", SKILL_STAGE)).run_result()
    assert len([item for item in result.compiled if isinstance(item, CompiledExtension)]) == 2
    assert "SST-VAL825" not in [item.code for item in result.diagnostics]
