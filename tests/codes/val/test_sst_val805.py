"""SST-VAL805: repo and catalog disagree.

A published skill whose folder was deleted is an orphan the prune report names; SST keeps it,
and a skill that still has its source is not reported.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import compile_extensions, empty_state, publish_extensions, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake

BOTH = compile_extensions(skill_catalog(skill(), skill("year-close")))
KEPT = {key: item for key, item in BOTH.items() if key == "skill:month-close"}


def test_sst_val805_fires() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, BOTH, empty_state())
    changeset, _ = publish_extensions(port, KEPT, published, include_prune=True)
    diagnostic = only(changeset.diagnostics, "SST-VAL805")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "skill reconciliation: DB.S.YEAR_CLOSE is published and skill:year-close has no source"
    assert diagnostic.subject == "skill:year-close"


def test_sst_val805_silent() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, BOTH, empty_state())
    changeset, _ = publish_extensions(port, BOTH, published, include_prune=True)
    assert "SST-VAL805" not in codes(changeset.diagnostics)
