"""SST-VAL803: an extension version is not named by its content digest.

Compile names each version `+version_prefix` and 12 hex characters of the bundle digest, so
a pinned reference always reads the same files; it checks every release it mints, and one
named any other way does not compile.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.skill import Plugin, SkillBundle, SkillCatalog
from snowflake_semantic_tools.domain.validate.publication import MintedVersion, version_diagnostics
from tests.helpers.publications import SKILL_STAGE, skill

DIGEST = "0123456789abcdef" * 4
TARGET = QualifiedName.parse("DB.S.MONTH_CLOSE")


def _catalog() -> SkillCatalog:
    plugin = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", "Kit.", "Data", ("month-close",), Origin("p"))
    return SkillCatalog((skill(),), (plugin,))


def _compile(catalog: SkillCatalog) -> tuple[list[str], list[str]]:
    result = CompileSkills(catalog, CatalogChannel("DB", "S", SKILL_STAGE)).run_result()
    compiled = [item.artifact_key for item in result.compiled if isinstance(item, CompiledExtension)]
    return compiled, [item.code for item in result.diagnostics if item.code == "SST-VAL803"]


def test_sst_val803_fires() -> None:
    [diagnostic] = version_diagnostics(
        MintedVersion("skill:month-close", "month-close", TARGET, "GIT_LATEST", DIGEST), "GIT_"
    )
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL803",
        Severity.ERROR,
        "skill:month-close",
    )
    assert diagnostic.message == "skill 'month-close': version 'GIT_LATEST' is not SHA-derived"


def test_sst_val803_keeps_a_release_named_otherwise_from_compiling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SkillBundle, "alias", lambda self, prefix: f"{prefix}LATEST")
    # The plugin is held back by its member skill, as SST-VAL836 reports.
    assert _compile(_catalog()) == ([], ["SST-VAL803"])


def test_sst_val803_keeps_a_plugin_named_otherwise_from_compiling(monkeypatch: pytest.MonkeyPatch) -> None:
    named = SkillBundle.alias
    monkeypatch.setattr(
        SkillBundle, "alias", lambda self, prefix: f"{prefix}LATEST" if self.name == "kit" else named(self, prefix)
    )
    assert _compile(_catalog()) == (["skill:month-close"], ["SST-VAL803"])


def test_sst_val803_silent() -> None:
    assert (
        version_diagnostics(
            MintedVersion("skill:month-close", "month-close", TARGET, "GIT_0123456789AB", DIGEST), "GIT_"
        )
        == ()
    )
    assert _compile(_catalog()) == (["plugin:kit", "skill:month-close"], [])
