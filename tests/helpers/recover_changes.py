"""Plan changes and a recover run for `sst state recover`'s tests: proved, unmanaged, refused."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.recover import RecoverResult, RecoverState
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason, OwnershipMarker
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, empty_state, marker, observed, rendered
from tests.helpers.clocks import FixedClock
from tests.helpers.snowflake_fake import FakeSnowflake

TABLE = QualifiedName.parse("DB.S.SST_STATE")


def unmanaged(name: str, ownership: OwnershipMarker | None) -> Change:
    artifact = rendered(name)
    return replace(
        change(artifact, Action.BLOCKED, live=observed(artifact, ownership=ownership)),
        reason=ChangeReason.UNMANAGED_OBJECT,
    )


def proved(name: str) -> Change:
    artifact = rendered(name)
    return unmanaged(name, marker(artifact))


def recover(port: FakeSnowflake, *changes: Change, store: InMemoryStateStore | None = None) -> RecoverResult:
    return RecoverState(port, store or InMemoryStateStore(), FixedClock(), state_table=TABLE, actor="R", host="h").run(
        changeset(*changes), empty_state(), "verify"
    )
