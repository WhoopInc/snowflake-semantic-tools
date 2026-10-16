"""The drift digest plan observes and apply reads back: what each reads, and what state records."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app import drift
from snowflake_semantic_tools.app.drift import observed_digest, written_digest
from snowflake_semantic_tools.app.observe import DEFAULT_OBSERVE_OPTIONS, ObserveOptions, observe
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    DRIFT_CHECK_BASIC,
    Action,
    Change,
    ChangeReason,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    ShowRow,
    drift_properties,
    live_digest,
    spec_fingerprint,
    version_comment,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, GrantPreservation
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import changeset, empty_state
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

PROCEDURE = QualifiedName.parse("DB.SCHEMA.P")
VIEW = QualifiedName.parse("DB.SCHEMA.V")
MARKER = OwnershipMarker("a" * 64, "b" * 64)
DESCRIBED = {"execute as": "CALLER", "body": "BEGIN RETURN 1; END;"}


def _artifact(object_type: str, target: QualifiedName, *, temporary: bool = False) -> RenderedArtifact:
    artifact = RenderedArtifact.create(
        key=f"tool:{target.name.folded.casefold()}",
        artifact_type="tool",
        target=target,
        ddl=statement(f"create or replace {object_type.lower()} x"),
        shape=PublishShape(object_type, grant_preservation=GrantPreservation.NONE, temporary=temporary),
    )
    return replace(artifact, expected_marker=MARKER)


def _port() -> FakeSnowflake:
    port = FakeSnowflake()
    port.show(
        ShowRow("P", "DB", "SCHEMA", "OWNER", "now", f"tool {MARKER.text}", object_type="PROCEDURE", is_secure="N"),
        ShowRow("V", "DB", "SCHEMA", "OWNER", "now", f"view {MARKER.text}", object_type="SEMANTIC VIEW"),
    )
    port.routine_descriptions["PROCEDURE DB.SCHEMA.P()"] = DESCRIBED
    port.definitions[VIEW.sql] = "create or replace semantic view V"
    port.show_rows["PROCEDURE DB.SCHEMA.P"] = {"is_secure": "N"}
    port.markers[PROCEDURE.sql] = MARKER
    port.markers[VIEW.sql] = MARKER
    return port


def _observed(
    port: FakeSnowflake, options: ObserveOptions = DEFAULT_OBSERVE_OPTIONS
) -> tuple[dict[str, str | None], list[str]]:
    desired = {item.key: item for item in (_artifact("PROCEDURE", PROCEDURE), _artifact("SEMANTIC VIEW", VIEW))}
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (PROCEDURE,),
        fetched_at="now",
        observed_object_types={"tool": frozenset(("PROCEDURE",)), "semantic_view": frozenset(("SEMANTIC VIEW",))},
        desired_artifacts=desired,
        options=options,
    )
    digests = {key: item.live_digest for key, item in observation.artifacts.items()}
    return digests, [item.code for item in diagnostics]


def test_plan_digests_a_marked_procedure_and_view_from_one_describe_and_one_get_ddl() -> None:
    port = _port()
    digests, codes = _observed(port)
    assert codes == []
    assert digests["tool:p"] == live_digest(drift_properties("PROCEDURE", {"is_secure": "N"}, described=DESCRIBED))
    assert digests["semantic_view:v"] == live_digest({"definition": "create or replace semantic view V"})
    assert port.routine_reads == ["PROCEDURE DB.SCHEMA.P()"]


def test_a_basic_drift_check_reads_no_describe_and_no_get_ddl() -> None:
    port = _port()
    port.refuse("get_ddl", "describe_routine")
    digests, codes = _observed(port, ObserveOptions(drift_check=DRIFT_CHECK_BASIC))
    assert codes == [] and port.routine_reads == []
    assert digests["tool:p"] == live_digest({"secure": "N"}) and digests["semantic_view:v"] is None


def test_a_failed_drift_read_warns_and_digests_what_shows() -> None:
    port = _port()
    port.refuse("describe_routine")
    digests, codes = _observed(port)
    assert codes == ["SST-PLN103"] and digests["tool:p"] == live_digest({"secure": "N"})


def test_an_unmarked_or_undesired_object_is_not_digested() -> None:
    port = _port()
    port.objects.clear()
    port.show(ShowRow("P", "DB", "SCHEMA", "OWNER", "now", "no marker", object_type="PROCEDURE"))
    digests, _ = _observed(port)
    assert digests["tool:p"] is None and port.routine_reads == []
    assert observed_digest(port, "STAGE", PROCEDURE, _artifact("STAGE", PROCEDURE), shown={}, newest_spec=None) == (
        None,
        (),
    )


def test_apply_reads_back_the_digest_of_what_it_wrote_and_state_records_it() -> None:
    port = _port()
    artifact = _artifact("PROCEDURE", PROCEDURE)
    store = InMemoryStateStore()
    create = Change(artifact.key, "tool", Action.CREATE, ChangeReason.NOT_PRESENT, artifact, None, (), 50)
    result = apply_plan(changeset(create), port, store=store)
    assert result.success, result.diagnostics
    expected = live_digest(drift_properties("PROCEDURE", {"is_secure": "N"}, described=DESCRIBED))
    assert result.outcomes[0].live_digest == expected
    assert store.state is not None and store.state.applied[artifact.key].live_digest == expected


def test_a_read_back_that_raises_leaves_the_written_digest_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.app.apply import one

    def broken(port: object, rendered: object) -> str | None:
        raise RuntimeError("driver bug")

    monkeypatch.setattr(one, "written_digest", broken)
    artifact = _artifact("PROCEDURE", PROCEDURE)
    create = Change(artifact.key, "tool", Action.CREATE, ChangeReason.NOT_PRESENT, artifact, None, (), 50)
    result = apply_plan(changeset(create), _port())
    assert result.success and result.outcomes[0].live_digest is None


def test_an_unchanged_object_whose_entry_has_no_digest_records_the_one_plan_read() -> None:
    artifact = _artifact("PROCEDURE", PROCEDURE)
    observed = observe(
        _port(),
        SEMANTIC_REGISTRY,
        (PROCEDURE,),
        fetched_at="now",
        observed_object_types={"tool": frozenset(("PROCEDURE",))},
        desired_artifacts={artifact.key: artifact},
    )[0].artifacts[artifact.key]
    older = AppliedEntry("b" * 64, PROCEDURE.sql, "then", "run", APPLIED, "b" * 64, "a" * 64)
    other = replace(older, qualified_name="DB.SCHEMA.Q")
    noop = Change(artifact.key, "tool", Action.NOOP, ChangeReason.UNCHANGED, artifact, observed, (), 50)
    undigested = Change(
        "tool:q", "tool", Action.NOOP, ChangeReason.UNCHANGED, artifact, replace(observed, live_digest=None), (), 50
    )
    # A run that changes nothing writes no state, so the plan also creates one object.
    port = _port()
    created = _artifact("PROCEDURE", QualifiedName.parse("DB.SCHEMA.R"))
    port.markers[created.target.sql] = MARKER
    create = Change(created.key, "tool", Action.CREATE, ChangeReason.NOT_PRESENT, created, None, (), 50)
    store = InMemoryStateStore()
    previous = empty_state({artifact.key: older, "tool:q": other})
    # Apply reads state again under the lock; it must be what the plan read.
    port.remote_state = previous.applied
    result = apply_plan(changeset(noop, undigested, create), port, previous=previous, store=store)
    assert result.success, result.diagnostics
    assert store.state is not None
    assert store.state.applied[artifact.key].live_digest == observed.live_digest is not None
    assert store.state.applied["tool:q"].live_digest is None


def test_written_digest_is_unknown_for_what_it_cannot_or_need_not_read() -> None:
    port = _port()
    assert written_digest(port, _artifact("STAGE", PROCEDURE)) is None
    assert written_digest(port, _artifact("PROCEDURE", PROCEDURE, temporary=True)) is None
    # The object is not listed, as when it was dropped right after the write.
    assert written_digest(port, _artifact("PROCEDURE", QualifiedName.parse("DB.SCHEMA.GONE"))) is None
    port.refuse("describe_routine")
    assert written_digest(port, _artifact("PROCEDURE", PROCEDURE)) is None
    port.refuse("show_row")
    assert written_digest(port, _artifact("PROCEDURE", PROCEDURE)) is None
    assert written_digest(port, _artifact("SEMANTIC VIEW", VIEW)) == live_digest(
        {"definition": "create or replace semantic view V"}
    )


def test_an_agent_digests_its_newest_committed_specification() -> None:
    port = FakeSnowflake()
    agent = QualifiedName.parse("DB.SCHEMA.SALES")
    spec = '{"models": {"orchestration": "auto"}}'
    port.agent_version_rows[agent.sql] = (
        AgentVersion("VERSION$1", None, False, version_comment("abc1234", "{}"), "{}"),
        AgentVersion("VERSION$2", "PROMOTED", True, version_comment("abc1234", spec), spec),
    )
    assert written_digest(port, _artifact("AGENT", agent)) == live_digest({"specification": spec_fingerprint(spec)})
    port.refuse("observe_agent_versions")
    assert written_digest(port, _artifact("AGENT", agent)) is None


def test_the_read_names_report_what_failed() -> None:
    assert drift._READS == {"PROCEDURE": "DESCRIBE PROCEDURE", "SEMANTIC VIEW": "GET_DDL"}
