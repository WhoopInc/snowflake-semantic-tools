"""SST-VAL861: a prune forgets a skill or plugin state records once it is gone from the catalog."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason
from tests.helpers.artifact_builders import empty_state
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import compile_extensions, publish_extensions, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake

BOTH = compile_extensions(skill_catalog(skill(), skill("year-close")))
KEPT = {key: item for key, item in BOTH.items() if key == "skill:month-close"}


def test_sst_val861_fires() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, BOTH, empty_state())
    del port.extensions["DB.S.YEAR_CLOSE"]
    changeset, _ = publish_extensions(port, KEPT, published, include_prune=True)
    diagnostic = only(changeset.diagnostics, "SST-VAL861")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "skill:year-close: DB.S.YEAR_CLOSE is gone from the catalog, so apply --prune forgets its state entry"
    )
    assert diagnostic.subject == "skill:year-close"
    [prune] = [change for change in changeset.changes if change.key == "skill:year-close"]
    assert (prune.action, prune.reason, prune.prune_executable) == (Action.PRUNE, ChangeReason.NOT_PRESENT, True)


def test_sst_val861_silent() -> None:
    # An extension still in the catalog is kept, report-only, and the report names the drop.
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, BOTH, empty_state())
    changeset, _ = publish_extensions(port, KEPT, published, include_prune=True)
    assert "SST-VAL861" not in codes(changeset.diagnostics)
    kept = only(changeset.diagnostics, "SST-PLN034")
    assert "`sst drop DB.S.YEAR_CLOSE --type skill --target <target> --yes`" in kept.message
    [prune] = [change for change in changeset.changes if change.key == "skill:year-close"]
    assert not prune.prune_executable
