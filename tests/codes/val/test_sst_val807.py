"""SST-VAL807: certifying a published skill would move it to another schema.

Certification is a tag on the version. A move changes the extension's name, which every pinned
agent reference uses, so a certifying release in a schema other than the one state records
the extension in is refused rather than re-created there.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.state import State
from tests.helpers.eval_inputs import codes, only
from tests.helpers.recorded_snowflake import RecordedSnowflake
from tests.helpers.skill_inputs import (
    CATALOG_CHANNEL,
    compile_extensions,
    empty_state,
    publish_extensions,
    skill_catalog,
)


def _published() -> tuple[RecordedSnowflake, State]:
    port = RecordedSnowflake(existing=("DB.S.SKILL_BUNDLES",))
    _, state = publish_extensions(port, compile_extensions(skill_catalog()), empty_state())
    return port, state


def _recompiled(channel: CatalogChannel) -> dict[str, CompiledExtension]:
    result = CompileSkills(skill_catalog(), channel).run_result()
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def test_sst_val807_fires() -> None:
    port, state = _published()
    moved = replace(CATALOG_CHANNEL, schema="CERTIFIED", certified=True)
    changeset, _ = publish_extensions(port, _recompiled(moved), state)
    diagnostic = only(changeset.diagnostics, "SST-VAL807")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill 'month-close': certification would change its schema"
    assert diagnostic.subject == "skill:month-close"
    assert [change.action for change in changeset.changes] == [Action.BLOCKED]
    assert not port.object_exists("CORTEX EXTENSION", QualifiedName.parse("DB.CERTIFIED.MONTH_CLOSE"))


def test_sst_val807_silent() -> None:
    # Certifying in place is a tag, and a move that certifies nothing is not certification.
    port, state = _published()
    certified, _ = publish_extensions(port, _recompiled(replace(CATALOG_CHANNEL, certified=True)), state)
    assert "SST-VAL807" not in codes(certified.diagnostics)
    moved, _ = publish_extensions(port, _recompiled(replace(CATALOG_CHANNEL, schema="OTHER")), state)
    assert "SST-VAL807" not in codes(moved.diagnostics)
