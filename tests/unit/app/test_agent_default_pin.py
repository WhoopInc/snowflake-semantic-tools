"""After an agent update commits a version, apply pins the agent's default to it by name.

The program ends with the default following `'LAST'`; apply then reads the version it committed,
the newest release SHOW lists, and runs `SET DEFAULT_VERSION = '<that version>'`, so a version
committed later, such as an eval-model version, never becomes the default. A read or a pin that
fails leaves the write applied and recorded, and reports SST-APL107.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from snowflake_semantic_tools.app.apply.one import ChangeApplier
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOutcome,
    ExecResult,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.agent_updates import PIN, agent_update, committed
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import changeset
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


class PinRefused(FakeSnowflake):
    """A fake whose pin, the script that sets a version's name as the default, raises or fails bare."""

    def __init__(self, *, raises: bool) -> None:
        super().__init__()
        self.raises = raises

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        if any("DEFAULT_VERSION = 'VERSION$" in str(item) for item in statements):
            if self.raises:
                raise SnowflakePortError("the run lock was lost")
            return ExecResult(False)
        return super().execute_script(statements)


def _applied(port: FakeSnowflake, *, adds_version: bool = True) -> tuple[list[str], list[str], InMemoryStateStore]:
    """Apply the agent's update; return the statements sent after the update, APL107's messages, the store."""
    store = InMemoryStateStore()
    result = apply_plan(changeset(agent_update(adds_version=adds_version)), port, store=store)
    assert result.success, result.diagnostics
    assert store.state is not None and "agent:sales" in store.state.applied
    sent = [text for script in port.scripts for text in script]
    after = sent[sent.index("add version") + 1 :] if "add version" in sent else sent
    return after, [item.message for item in coded(result.diagnostics, "SST-APL107")], store


def test_a_version_adding_update_pins_the_default_to_the_release_it_committed() -> None:
    port = FakeSnowflake()
    committed(port)
    after, warnings, store = _applied(port)
    # The program leaves the default following LAST; the pin, to the newest release and not the
    # eval-model version beside it, runs once the write is verified.
    assert after[:2] == ["ALTER AGENT DB.S.SALES SET DEFAULT_VERSION = 'LAST'", PIN]
    assert warnings == []
    assert dict(store.state.applied["agent:sales"].component_fingerprints)["version"] == "VERSION$3"  # type: ignore[union-attr]


def test_an_update_that_adds_no_version_pins_nothing_after_its_program() -> None:
    port = FakeSnowflake()
    committed(port)
    after, warnings, _ = _applied(port, adds_version=False)
    assert PIN not in after and warnings == []


def test_a_pin_whose_read_fails_leaves_the_write_recorded_and_warns() -> None:
    port = FakeSnowflake()
    port.refuse("observe_agent_versions")
    after, warnings, _ = _applied(port)
    assert PIN not in after
    assert warnings == [
        "agent:sales: the default version could not be pinned by name (reading its versions failed: "
        "observe_agent_versions refused); it is left following LAST, and the next `sst plan` pins it"
    ]


def test_a_pin_with_no_release_listed_to_pin_to_warns() -> None:
    _, warnings, _ = _applied(FakeSnowflake())
    assert warnings == [
        "agent:sales: the default version could not be pinned by name (SHOW VERSIONS IN AGENT listed no "
        "version it committed); it is left following LAST, and the next `sst plan` pins it"
    ]


def test_a_pin_that_raises_or_fails_without_an_error_warns_with_why() -> None:
    for raises, why in ((True, "the run lock was lost"), (False, "unknown Snowflake failure")):
        port = PinRefused(raises=raises)
        committed(port)
        _, warnings, _ = _applied(port)
        assert warnings == [
            f"agent:sales: the default version could not be pinned by name (pinning VERSION$3 failed: {why}); "
            "it is left following LAST, and the next `sst plan` pins it"
        ]


def test_only_an_applied_version_adding_update_is_pinned() -> None:
    port = FakeSnowflake()
    committed(port)
    port.refuse("observe_agent_versions", "execute_script")
    applier = ChangeApplier(port, FixedClock(), {})
    update = agent_update()
    assert update.rendered is not None
    applied = ApplyOutcome(update.key, Action.UPDATE, OutcomeStatus.APPLIED, 1, 5, "")
    failed = replace(applied, status=OutcomeStatus.FAILED)
    assert applier._pin_default(failed, update.rendered) == failed
    for adds_version in (False, None):
        assert applier._pin_default(applied, replace(update.rendered, adds_version=adds_version)) == applied
