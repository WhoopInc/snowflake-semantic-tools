"""`plan --capture-prior`: each published kind's prior read the way it can be restored."""

from __future__ import annotations

import dataclasses

from snowflake_semantic_tools.app.observe import ObserveOptions, observe
from snowflake_semantic_tools.app.prior import agent_prior, prior_definition
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact, ShowRow
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.artifact_builders import rendered
from tests.helpers.snowflake_fake import FakeSnowflake

AGENT = QualifiedName.parse("DB.S.SALES_AGENT")
SPEC = '{"models":{"orchestration":"auto"},"instructions":{"response":"Be brief, café"}}'
PRETTY = (
    '{\n  "models": {\n    "orchestration": "auto"\n  },\n  "instructions": {\n    "response": "Be brief, café"\n  }\n}'
)


def _row(object_type: str, name: str = "V") -> ShowRow:
    return ShowRow(name, "DB", "SCHEMA", "OWNER", "now", object_type=object_type)


def _agent_row() -> ShowRow:
    return ShowRow("SALES_AGENT", "DB", "S", "OWNER", "now", object_type="AGENT")


def _desired(*, alias: str | None = None, signature: tuple[str, ...] = ()) -> RenderedArtifact:
    return dataclasses.replace(rendered(), desired_alias=alias, routine_signature=signature)


def test_a_replaced_object_s_prior_is_what_get_ddl_prints() -> None:
    port = FakeSnowflake(definitions={"DB.SCHEMA.V": "create or replace semantic view V"})
    for object_type in ("SEMANTIC VIEW", "CORTEX SEARCH SERVICE"):
        assert prior_definition(port, object_type, _row(object_type), _desired()) == (
            "create or replace semantic view V",
            (),
        )


def test_a_routine_is_read_by_its_name_with_its_argument_types() -> None:
    port = FakeSnowflake(definitions={"DB.SCHEMA.P(VARCHAR, NUMBER)": "create or replace procedure P"})
    procedure = _desired(signature=("VARCHAR", "NUMBER"))
    found, quiet = prior_definition(port, "PROCEDURE", _row("PROCEDURE", "P"), procedure)
    assert (found, quiet) == ("create or replace procedure P", ())
    # Addressed by another signature, Snowflake finds no such routine; the plan only warns.
    missing, [warning] = prior_definition(port, "FUNCTION", _row("FUNCTION", "P"), _desired())
    assert missing is None and (warning.code, warning.severity) == ("SST-PLN037", Severity.WARNING)


def test_a_stage_or_an_object_no_artifact_renders_is_never_read() -> None:
    port = FakeSnowflake()
    port.refuse("get_ddl", "show_agent_versions")
    # Apply only creates a stage tool when it is absent, and GET_DDL reads no stage.
    assert prior_definition(port, "STAGE", _row("STAGE"), _desired()) == (None, ())
    assert prior_definition(port, "SEMANTIC VIEW", _row("SEMANTIC VIEW"), None) == (None, ())
    assert prior_definition(port, "AGENT", _row("AGENT"), None) == (None, ())


def test_an_agent_s_prior_is_the_version_it_serves_and_how_to_point_back_at_it() -> None:
    port = FakeSnowflake()
    port.refuse("get_ddl")
    port.agent_version_rows[AGENT.sql] = (
        AgentVersion(None, None, specification="{}"),
        AgentVersion("VERSION$3", "PROD", specification=SPEC),
        AgentVersion("VERSION$2", None, is_default=True, specification="{}"),
        AgentVersion("VERSION$1", None, specification="{}"),
    )
    found, quiet = prior_definition(port, "AGENT", _agent_row(), _desired(alias="prod"))
    assert quiet == ()
    assert found == (
        "-- agent DB.S.SALES_AGENT serves VERSION$3 (alias PROD, the last committed).\n"
        "-- An update adds a version and keeps this one; to roll back after apply, run:\n"
        "ALTER AGENT DB.S.SALES_AGENT MODIFY VERSION VERSION$3 SET ALIAS = PROD;\n"
        "ALTER AGENT DB.S.SALES_AGENT SET DEFAULT_VERSION = 'VERSION$2';\n"
        "-- The specification of VERSION$3:\n"
        f"{PRETTY}\n"
    )


def test_an_unaliased_agent_serves_its_default_else_its_last_committed_version() -> None:
    default = (AgentVersion("VERSION$1", "OLD", is_default=True, specification=SPEC),)
    served = agent_prior(AGENT, default, None)
    assert served is not None
    assert served.startswith("-- agent DB.S.SALES_AGENT serves VERSION$1 (alias OLD, the default).\n")
    assert "SET ALIAS" not in served
    # No version marked default: the last committed one is served, and pinned back to.
    last = (AgentVersion("VERSION$9", None), AgentVersion("VERSION$10", None, specification="not json"))
    pinned = agent_prior(AGENT, last, "prod")
    assert pinned == (
        "-- agent DB.S.SALES_AGENT serves VERSION$10 (the last committed).\n"
        "-- An update adds a version and keeps this one; to roll back after apply, run:\n"
        "ALTER AGENT DB.S.SALES_AGENT SET DEFAULT_VERSION = 'VERSION$10';\n"
        "-- The specification of VERSION$10:\n"
        "not json\n"
    )
    # A version not named VERSION$<n> sorts first, so a numbered one is still preferred.
    odd = agent_prior(AGENT, (AgentVersion("VERSION$1", None), AgentVersion("CUSTOM", None)), None)
    assert odd is not None and "serves VERSION$1 (the last committed)" in odd


def test_an_agent_with_only_a_live_version_or_a_hidden_specification_is_still_described() -> None:
    live = agent_prior(AGENT, (AgentVersion(None, None, specification=SPEC),), None)
    assert live == (
        "-- agent DB.S.SALES_AGENT serves the live version.\n"
        "-- An update commits the live version before it adds one, so this one is kept.\n"
        "-- The specification of the live version:\n"
        f"{PRETTY}\n"
    )
    # A secure agent shows its specification only to its owner role, as Snowflake documents.
    secure = agent_prior(AGENT, (AgentVersion("VERSION$1", None, is_default=True),), None)
    assert secure is not None and secure.endswith("-- The role may not read the specification of VERSION$1.\n")
    assert agent_prior(AGENT, (), None) is None


def test_an_agent_whose_versions_cannot_be_read_only_warns() -> None:
    port = FakeSnowflake()
    found, [warning] = prior_definition(port, "AGENT", _agent_row(), _desired())
    assert found is None
    assert warning.message == "the prior definition of DB.S.SALES_AGENT was not captured: the agent lists no version"
    port.fail("show_agent_versions", SnowflakePortError("insufficient privileges"))
    refused, [warning] = prior_definition(port, "AGENT", _agent_row(), _desired())
    assert refused is None and warning.message.endswith("was not captured: insufficient privileges")


def test_one_object_s_prior_failing_does_not_stop_the_others_being_observed() -> None:
    first, second = rendered("A"), rendered("B")
    port = FakeSnowflake(definitions={second.target.sql: "create or replace semantic view B"})
    port.show(_row("SEMANTIC VIEW", "A"), _row("SEMANTIC VIEW", "B"))
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (first.target, second.target),
        fetched_at="now",
        desired_artifacts={first.key: first, second.key: second},
        options=ObserveOptions(grants=False, capture_prior=True),
    )
    assert observation.artifacts[first.key].definition is None
    assert observation.artifacts[second.key].definition == "create or replace semantic view B"
    assert [(item.code, item.severity) for item in diagnostics] == [("SST-PLN037", Severity.WARNING)]
