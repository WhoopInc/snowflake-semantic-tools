"""The agent spec stage: apply creates it when absent, once per run; plan catches one it cannot use.

SST never replaces, alters, or drops the stage, so every test also checks the statement log
holds no such statement.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ExecResult, OutcomeStatus
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.agent_stage import (
    STAGE,
    apply_plan,
    plan_agents,
    stage_changes,
    stage_creations,
    staged_agent,
)
from tests.helpers.snowflake_fake import FakeSnowflake

CREATE_STAGE = f"CREATE STAGE IF NOT EXISTS {STAGE.sql} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"


def test_apply_creates_an_absent_stage_once_before_the_first_upload_for_every_agent_on_it() -> None:
    port = FakeSnowflake()
    agents = tuple(staged_agent(name) for name in ("a", "b", "c"))
    result, _ = apply_plan(port, plan_agents(port, *agents))
    assert result.success, result.diagnostics
    assert stage_creations(port) == [CREATE_STAGE]
    assert stage_changes(port) == []
    first_upload = next(index for index, sent in enumerate(port.log) if sent.kind == "upload")
    created = next(index for index, sent in enumerate(port.log) if sent.statements == (CREATE_STAGE,))
    assert created < first_upload
    assert sorted(path for path, _ in port.uploads) == sorted(str(agent.upload_path) for agent in agents)
    assert port.stage_types[STAGE.sql] == "INTERNAL NO CSE"


def test_apply_leaves_an_existing_internal_sse_stage_as_it_is() -> None:
    port = FakeSnowflake()
    port.stage_types[STAGE.sql] = "INTERNAL NO CSE"
    read: list[QualifiedName] = []
    stage_type = port.stage_type

    def reading(qualified_name: QualifiedName) -> str | None:
        read.append(qualified_name)
        return stage_type(qualified_name)

    port.stage_type = reading  # type: ignore[method-assign]
    result, _ = apply_plan(port, plan_agents(port, staged_agent("a"), staged_agent("b")))
    assert result.success, result.diagnostics
    # Plan checks the stage once, and apply once for both agents; neither changes it.
    assert read == [STAGE, STAGE]
    assert stage_creations(port) == [] and stage_changes(port) == []
    assert len(port.uploads) == 2


def test_apply_fails_an_agent_whose_stage_is_another_kind_and_never_alters_it() -> None:
    port = FakeSnowflake()
    # A plan made without the preflight reads, so apply is the first to see the stage.
    planned = plan_agents(port, staged_agent("a"), preflight=False)
    port.stage_types[STAGE.sql] = "INTERNAL"
    result, _ = apply_plan(port, planned)
    [outcome] = result.outcomes
    assert outcome.status is OutcomeStatus.FAILED and not outcome.write_succeeded
    assert outcome.error is not None and outcome.error.code == "SST-APL001"
    assert outcome.error.message == (
        f"stage {STAGE.sql} is INTERNAL, expected INTERNAL NO CSE; SST never alters a stage"
    )
    assert [item.code for item in result.diagnostics] == ["SST-APL001"]
    assert port.uploads == [] and port.scripts == []
    assert port.stage_types[STAGE.sql] == "INTERNAL"


def test_apply_fails_an_agent_whose_stage_cannot_be_created_and_reports_why() -> None:
    port = FakeSnowflake()
    planned = plan_agents(port, staged_agent("a"))
    port.refused = ("CREATE STAGE",)
    result, _ = apply_plan(port, planned)
    [outcome] = result.outcomes
    assert outcome.status is OutcomeStatus.FAILED and not outcome.write_succeeded
    assert outcome.error is not None
    assert outcome.error.message.startswith(f"stage {STAGE.sql} could not be created: ")
    assert port.uploads == [] and stage_changes(port) == []


def test_apply_fails_an_agent_whose_stage_is_still_absent_after_creation() -> None:
    port = FakeSnowflake()
    planned = plan_agents(port, staged_agent("a"))

    def creates_nothing(statements: Sequence[Sql]) -> ExecResult:
        del statements
        return ExecResult(True)

    port.execute_script = creates_nothing  # type: ignore[method-assign]
    result, _ = apply_plan(port, planned)
    [outcome] = result.outcomes
    assert outcome.error is not None and outcome.error.message == f"stage {STAGE.sql} is absent after creation"
    assert port.uploads == []


def test_plan_says_apply_creates_an_absent_stage_once_and_blocks_nothing() -> None:
    port = FakeSnowflake()
    planned = plan_agents(port, staged_agent("a"), staged_agent("b"))
    notes = [item for item in planned.diagnostics if item.code == "SST-PLN035"]
    assert [item.message for item in notes] == [
        f"stage {STAGE.sql} does not exist; apply creates it, server-side encrypted, before it stages an agent there"
    ]
    assert [change.action for change in planned.changes] == [Action.CREATE, Action.CREATE]
    assert not planned.blocked and not planned.diagnostics.has_errors
    assert port.scripts == []


def test_plan_blocks_every_agent_on_an_absent_stage_the_role_cannot_create() -> None:
    port = FakeSnowflake()
    port.preflight.lacking["DB.S"] = ("CREATE STAGE",)
    planned = plan_agents(port, staged_agent("a"), staged_agent("b"))
    assert [change.action for change in planned.changes] == [Action.BLOCKED, Action.BLOCKED]
    blocking = [item for item in planned.diagnostics if item.code == "SST-PLN008"]
    assert [item.message for item in blocking] == ["TEST_ROLE lacks CREATE STAGE on DB.S"] * 2
    assert "SST-PLN035" not in [item.code for item in planned.diagnostics]
    result, _ = apply_plan(port, planned)
    assert not result.success and result.outcomes == ()
    assert port.uploads == [] and stage_creations(port) == []


def test_plan_blocks_an_agent_on_a_stage_of_another_kind() -> None:
    port = FakeSnowflake()
    port.stage_types[STAGE.sql] = "INTERNAL"
    planned = plan_agents(port, staged_agent("a"))
    assert [change.action for change in planned.changes] == [Action.BLOCKED]
    [blocking] = [item for item in planned.diagnostics if item.code == "SST-PLN026"]
    assert blocking.message == f"agent:a: stage {STAGE.sql} is INTERNAL, expected INTERNAL NO CSE"
    assert "SST-PLN035" not in [item.code for item in planned.diagnostics]


def test_plan_blocks_an_agent_on_a_stage_the_role_cannot_write() -> None:
    port = FakeSnowflake()
    port.stage_types[STAGE.sql] = "INTERNAL NO CSE"
    port.preflight.stage_lacking[STAGE.sql] = ("WRITE",)
    planned = plan_agents(port, staged_agent("a"))
    assert [change.action for change in planned.changes] == [Action.BLOCKED]
    [blocking] = [item for item in planned.diagnostics if item.code == "SST-PLN008"]
    assert blocking.message == f"TEST_ROLE lacks WRITE on {STAGE.sql}"


def test_plan_blocks_an_agent_whose_stage_schema_does_not_exist() -> None:
    port = FakeSnowflake()
    port.preflight.missing_schemas.add("DB.SPECS")
    elsewhere = QualifiedName.parse("DB.SPECS.AGENT_SPECS")
    planned = plan_agents(port, staged_agent("a", stage=elsewhere))
    assert [change.action for change in planned.changes] == [Action.BLOCKED]
    assert [item.code for item in planned.diagnostics if item.code.startswith("SST-PLN0")] == ["SST-PLN010"]


def test_a_partial_first_apply_leaves_the_stage_for_the_next_plan_and_apply() -> None:
    port = FakeSnowflake()
    first, second = staged_agent("a"), staged_agent("b")
    port.refused = ("CREATE AGENT DB.S.B",)
    result, store = apply_plan(port, plan_agents(port, first, second))
    assert {outcome.key: outcome.status for outcome in result.outcomes} == {
        "agent:a": OutcomeStatus.APPLIED,
        "agent:b": OutcomeStatus.FAILED,
    }
    assert store.state is not None
    port.refused = ()
    replanned = plan_agents(port, second, state=store.state)
    assert "SST-PLN035" not in [item.code for item in replanned.diagnostics]
    assert [change.action for change in replanned.changes if change.key == "agent:b"] == [Action.CREATE]
    port.remote_state = store.state.applied
    retried, _ = apply_plan(port, replanned, store.state)
    assert retried.success, retried.diagnostics
    assert stage_creations(port) == [CREATE_STAGE]
    assert stage_changes(port) == []
