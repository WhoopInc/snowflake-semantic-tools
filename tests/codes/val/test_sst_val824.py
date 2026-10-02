"""SST-VAL824: deleted file survives into the next version.

A version is added from its bundle's stage prefix, so a file left there that the bundle no
longer has would be published with it; the plan refuses that, and a clean prefix publishes.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from tests.helpers.eval_inputs import codes, only
from tests.helpers.recorded_snowflake import RecordedSnowflake
from tests.helpers.skill_inputs import compile_extensions, empty_state, publish_extensions, skill_catalog

COMPILED = compile_extensions(skill_catalog())
PREFIX = COMPILED["skill:month-close"].release.prefix


def test_sst_val824_fires() -> None:
    port = RecordedSnowflake(existing=("DB.S.SKILL_BUNDLES",))
    port.stage_files.add(f"{PREFIX}skills/month-close/old_notes.md")
    changeset, _ = publish_extensions(port, COMPILED, empty_state())
    diagnostic = only(changeset.diagnostics, "SST-VAL824")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"skill 'month-close': '{PREFIX}skills/month-close/old_notes.md' was deleted and is still published"
    )
    assert diagnostic.subject == "skill:month-close"
    assert [change.action for change in changeset.changes] == [Action.BLOCKED]


def test_sst_val824_silent() -> None:
    port = RecordedSnowflake(existing=("DB.S.SKILL_BUNDLES",))
    changeset, _ = publish_extensions(port, COMPILED, empty_state())
    assert "SST-VAL824" not in codes(changeset.diagnostics)
    assert [change.action for change in changeset.changes] == [Action.CREATE]
