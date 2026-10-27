"""Plan changes and a recover run for `sst state recover`'s tests: proved, unmanaged, refused."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.recover import RecoverResult, RecoverState
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    CompositeObservation,
    OwnershipMarker,
    RecoveryProof,
)
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
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


def composite(name: str, reason: ChangeReason = ChangeReason.DEPENDENCY_BLOCKED) -> Change:
    """A skill's planned change, blocked for `reason`, carrying what its handler observed."""
    planned = change(rendered(name), Action.BLOCKED)
    observation = CompositeObservation(planned.key, details=(("version", "VERSION$1"),))
    return replace(planned, artifact_type="skill", reason=reason, composite_observation=observation)


class Proving(ExtensionLifecycleHandler):
    """A skill handler whose `recover` answers each key with the proof, or raises the error, given."""

    def __init__(self, port: FakeSnowflake, answers: Mapping[str, RecoveryProof | SnowflakePortError]) -> None:
        super().__init__(port, {}, "skill")
        self._answers = dict(answers)

    def recover(self, change: Change) -> RecoveryProof:
        answer = self._answers[change.key]
        if isinstance(answer, SnowflakePortError):
            raise answer
        return answer


def recover(
    port: FakeSnowflake,
    *changes: Change,
    store: InMemoryStateStore | None = None,
    handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
) -> RecoverResult:
    return RecoverState(port, store or InMemoryStateStore(), FixedClock(), state_table=TABLE, actor="R", host="h").run(
        changeset(*changes), empty_state(), "verify", lifecycle_handlers=handlers or {}
    )
