"""SST-VAL841: catalog serves a different version.

Reverting a skill to a version that already exists publishes nothing new, so the catalog keeps
serving the later default; a skill whose version is the default warns of nothing.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.artifact_builders import empty_state
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import compile_extensions, publish_extensions, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake

FIRST = compile_extensions(skill_catalog(skill()))
SECOND = compile_extensions(skill_catalog(skill(files={"reference/steps.md": "Steps, revised.\n"})))


def test_sst_val841_fires() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, FIRST, empty_state())
    _, changed = publish_extensions(port, SECOND, published)
    changeset, _ = publish_extensions(port, FIRST, changed)
    diagnostic = only(changeset.diagnostics, "SST-VAL841")
    alias = FIRST["skill:month-close"].release.alias
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        f"skill:month-close: the catalog will serve VERSION$3 of DB.S.MONTH_CLOSE, not {alias}, "
        "because it is the default version"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val841_silent() -> None:
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, FIRST, empty_state())
    changeset, _ = publish_extensions(port, FIRST, published)
    assert "SST-VAL841" not in codes(changeset.diagnostics)
