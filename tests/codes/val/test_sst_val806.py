"""SST-VAL806: an extension would land outside the catalog channel's one schema.

Every skill and plugin publishes into `skills.catalog`'s database and schema, whatever its
folder, and config refuses a per-folder route; compile checks every release it mints and
warns of one placed anywhere else.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.validate.publication import MintedVersion, schema_diagnostics
from tests.helpers.publications import SKILL_STAGE, skill

DIGEST = "0123456789ab" + "0" * 52


def _version(target: str) -> MintedVersion:
    return MintedVersion("skill:month-close", "month-close", QualifiedName.parse(target), "GIT_0123456789AB", DIGEST)


def test_sst_val806_fires() -> None:
    [diagnostic] = schema_diagnostics(_version("DB.FINANCE.MONTH_CLOSE"), ("DB", "S"))
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL806",
        Severity.WARNING,
        "skill:month-close",
    )
    assert diagnostic.message == "skill 'month-close' would land in schema 'DB.FINANCE'"


def test_sst_val806_silent() -> None:
    assert schema_diagnostics(_version("DB.S.MONTH_CLOSE"), ("DB", "S")) == ()
    catalog = SkillCatalog((skill(), skill("triage")))
    result = CompileSkills(catalog, CatalogChannel("DB", "S", SKILL_STAGE)).run_result()
    assert {item.release.target.folded[:2] for item in result.compiled if isinstance(item, CompiledExtension)} == {
        ("DB", "S")
    }
    assert "SST-VAL806" not in [item.code for item in result.diagnostics]
