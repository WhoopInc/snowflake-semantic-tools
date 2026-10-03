"""SST-VAL802: skill is not published as a typed CORTEX EXTENSION.

An extension SST published that now reports no TYPE cannot be a skill, so the plan blocks it;
the same extension reporting TYPE SKILL plans as unchanged.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.ports.snowflake.catalog import ExtensionObservation
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import compile_extensions, empty_state, publish_extensions, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake

COMPILED = compile_extensions(skill_catalog())


class UntypedSnowflake(FakeSnowflake):
    """Reports every extension without a TYPE once `untyped` is set."""

    untyped = False

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        observed = super().observe_extension(qualified_name)
        return replace(observed, extension_type="") if observed is not None and self.untyped else observed


def test_sst_val802_fires() -> None:
    port = UntypedSnowflake(existing=())
    _, published = publish_extensions(port, COMPILED, empty_state())
    port.untyped = True
    changeset, _ = publish_extensions(port, COMPILED, published)
    diagnostic = only(changeset.diagnostics, "SST-VAL802")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill 'month-close': TYPE is absent"
    assert diagnostic.subject == "skill:month-close"
    assert [change.action for change in changeset.changes] == [Action.BLOCKED]


def test_sst_val802_silent() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, COMPILED, empty_state())
    changeset, _ = publish_extensions(port, COMPILED, published)
    assert "SST-VAL802" not in codes(changeset.diagnostics)
    assert [change.action for change in changeset.changes] == [Action.NOOP]
