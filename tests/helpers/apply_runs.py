"""One apply run of a hand-built plan against the in-memory doubles, for the apply code tests."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ApplyResult, ChangeSet
from snowflake_semantic_tools.domain.state import State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import rendered, state
from tests.helpers.clocks import FixedClock
from tests.helpers.snowflake_fake import FakeSnowflake

STATE_TABLE = rendered("SST_STATE").target
_DEFAULT_OPTIONS = ApplyOptions()


def apply_plan(
    plan: ChangeSet,
    port: FakeSnowflake | None = None,
    *,
    options: ApplyOptions = _DEFAULT_OPTIONS,
    previous: State | None = None,
    store: InMemoryStateStore | None = None,
) -> ApplyResult:
    """Apply `plan` on `port`, a fresh double unless given, from `previous`, empty unless given."""
    return ApplyArtifacts(
        port or FakeSnowflake(), store or InMemoryStateStore(), FixedClock(), state_table=STATE_TABLE
    ).run(plan, previous or state(), options)
