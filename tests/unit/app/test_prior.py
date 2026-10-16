"""`plan --capture-prior`: each published kind's prior read the way it can be restored."""

from __future__ import annotations

import dataclasses

import pytest

from snowflake_semantic_tools.app.observe import ObserveOptions, observe
from snowflake_semantic_tools.app.prior import (
    agent_prior,
    prior_definition,
    restorable_routine,
    restorable_semantic_view,
)
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, RenderedArtifact, ShowRow
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions
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
    port = FakeSnowflake(definitions={"DB.SCHEMA.V": "create or replace cortex search service V"})
    found = prior_definition(port, "CORTEX SEARCH SERVICE", _row("CORTEX SEARCH SERVICE"), _desired())
    assert found == ("create or replace cortex search service V", ())


# GET_DDL's form of a semantic view: lowercased, without COPY GRANTS, MAX_STALENESS first.
GET_DDL_VIEW = (
    "create or replace semantic view DB.SCHEMA.V\n"
    "\ttables (T as DB.SCHEMA.T primary key (ID) comment='max_staleness copy grants')\n"
    "\tmetrics (T.N as COUNT(T.ID))\n"
    "\tcomment='a view; with tag'\n"
    "\tmax_staleness='300 seconds'\n"
    "\tai_sql_generation 'use max_staleness = ''1 day'' (never)'\n"
    "\twith tag (DB.SCHEMA.OWNER='team');\n"
)


def test_a_semantic_view_s_prior_is_put_in_the_form_the_server_re_executes() -> None:
    port = FakeSnowflake(definitions={"DB.SCHEMA.V": GET_DDL_VIEW})
    found, quiet = prior_definition(port, "SEMANTIC VIEW", _row("SEMANTIC VIEW"), _desired())
    assert quiet == ()
    # The AI clause moves before MAX_STALENESS and COPY GRANTS is appended, as SST renders;
    # the keywords inside the strings and the TABLES list are left where they are.
    assert found == (
        "create or replace semantic view DB.SCHEMA.V\n"
        "\ttables (T as DB.SCHEMA.T primary key (ID) comment='max_staleness copy grants')\n"
        "\tmetrics (T.N as COUNT(T.ID))\n"
        "  comment='a view; with tag'\n"
        "  ai_sql_generation 'use max_staleness = ''1 day'' (never)'\n"
        "  max_staleness='300 seconds'\n"
        "  with tag (DB.SCHEMA.OWNER='team')\n"
        "  COPY GRANTS;\n"
    )
    assert found is not None and found.index("ai_sql_generation") < found.index("max_staleness='")


def test_a_semantic_view_already_in_the_rendered_form_is_kept_byte_for_byte() -> None:
    rendered_form = 'CREATE OR REPLACE SEMANTIC VIEW "my view"\n  AI_SQL_GENERATION $$x$$\n  copy grants'
    assert restorable_semantic_view(rendered_form) == rendered_form
    # A view with no trailing clause gains only COPY GRANTS, and a view named like a clause is a name.
    assert restorable_semantic_view("create or replace semantic view") == (
        "create or replace semantic view\n  COPY GRANTS"
    )
    assert restorable_semantic_view("create or replace semantic view DB.S.COMMENT") == (
        "create or replace semantic view DB.S.COMMENT\n  COPY GRANTS"
    )
    assert restorable_semantic_view("create or replace semantic view V metrics (M as 1)\n") == (
        "create or replace semantic view V metrics (M as 1)\n  COPY GRANTS\n"
    )


def test_a_tag_clause_without_with_and_a_bare_number_are_clauses_of_the_view() -> None:
    # The grammar makes WITH optional before TAG, and MAX_STALENESS may be a bare number.
    text = (
        "create or replace semantic view V\n"
        "\tmax_staleness = 300\n"
        "\ttag (DB.S.OWNER='team')\n"
        "\tai_sql_generation 'x';\n"
    )
    restored = restorable_semantic_view(text)
    assert restored == (
        "create or replace semantic view V\n"
        "  ai_sql_generation 'x'\n"
        "  max_staleness = 300\n"
        "  tag (DB.S.OWNER='team')\n"
        "  COPY GRANTS;\n"
    )
    assert restorable_semantic_view(restored) == restored
    # A bare TAG is the WITH TAG clause, so giving both is giving it twice.
    assert "it gives WITH TAG twice" in restorable_semantic_view(
        "create or replace semantic view V tag (A='x') with tag (B='y')"
    )


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("create or replace semantic view V comment='open", "it does not lex"),
        ("create semantic view V", "it is not a CREATE OR REPLACE SEMANTIC VIEW"),
        ("create or replace semantic view V comment='x' secure", "it has 'secure' where SST expects a clause"),
        ("create or replace semantic view V with grants", "its WITH is not WITH TAG"),
        ("create or replace semantic view V copy", "its COPY is not COPY GRANTS"),
        ("create or replace semantic view V comment='x' tables (T)", "its TABLES comes after a trailing clause"),
        ("create or replace semantic view V comment='x' comment='y'", "it gives COMMENT twice"),
        ("create or replace semantic view V comment='x' -- note\n", "it has a comment between its clauses"),
        ("create or replace semantic view V tables (T))", "its parentheses do not balance"),
        ("create or replace semantic view V tables ((T)", "its parentheses do not balance"),
        ("create or replace semantic view V; select 1", "it has ';' where SST expects a clause"),
    ],
)
def test_a_semantic_view_of_another_shape_is_kept_as_returned_under_a_warning(text: str, reason: str) -> None:
    kept = restorable_semantic_view(text)
    first, second, third, rest = kept.split("\n", 3)
    assert first.startswith(f"-- SST did not put this definition in the form the server re-executes: {reason}")
    assert second.startswith("-- Before running it, check that it ends with COPY GRANTS")
    assert third == "-- and that MAX_STALENESS comes after the AI clauses."
    assert rest == text


def test_a_routine_is_read_by_its_name_with_its_argument_types() -> None:
    port = FakeSnowflake(
        definitions={"DB.SCHEMA.P(VARCHAR, NUMBER)": "create or replace procedure P(A VARCHAR, B NUMBER)"}
    )
    procedure = _desired(signature=("VARCHAR", "NUMBER"))
    found, quiet = prior_definition(port, "PROCEDURE", _row("PROCEDURE", "P"), procedure)
    assert (found, quiet) == ("create or replace procedure P(A VARCHAR, B NUMBER) COPY GRANTS", ())
    # Addressed by another signature, Snowflake finds no such routine; the plan only warns.
    missing, [warning] = prior_definition(port, "FUNCTION", _row("FUNCTION", "P"), _desired())
    assert missing is None and (warning.code, warning.severity) == ("SST-PLN037", Severity.WARNING)


def test_a_stage_or_an_object_no_artifact_renders_is_never_read() -> None:
    port = FakeSnowflake()
    port.refuse("get_ddl", "observe_agent_versions")
    # Apply only creates a stage tool when it is absent, and GET_DDL reads no stage.
    assert prior_definition(port, "STAGE", _row("STAGE"), _desired()) == (None, ())
    assert prior_definition(port, "SEMANTIC VIEW", _row("SEMANTIC VIEW"), None) == (None, ())
    assert prior_definition(port, "AGENT", _row("AGENT"), None) == (None, ())


def test_an_agent_s_prior_is_the_version_it_serves_and_how_to_point_back_at_it() -> None:
    port = FakeSnowflake()
    # The observation has already listed the agent's versions for its update; the prior reuses them.
    port.refuse("get_ddl", "observe_agent_versions")
    versions = AgentVersions(
        (
            AgentVersion(None, None, specification="{}"),
            AgentVersion("VERSION$3", "PROD", specification=SPEC),
            AgentVersion("VERSION$2", None, is_default=True, specification="{}"),
            AgentVersion("VERSION$1", None, specification="{}"),
        )
    )
    found, quiet = prior_definition(port, "AGENT", _agent_row(), _desired(alias="prod"), versions)
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


def test_an_agent_that_lists_no_version_only_warns() -> None:
    port = FakeSnowflake()
    found, [warning] = prior_definition(port, "AGENT", _agent_row(), _desired())
    assert found is None
    assert warning.message == "the prior definition of DB.S.SALES_AGENT was not captured: the agent lists no version"


def test_observing_an_agent_with_its_prior_lists_its_versions_once() -> None:
    artifact = dataclasses.replace(rendered(), key="agent:v", artifact_type="agent", object_type="AGENT")
    port = FakeSnowflake()
    port.show(_row("AGENT"))
    port.agent_version_rows[artifact.target.sql] = (AgentVersion(None, "PROD", specification=SPEC),)
    listed: list[QualifiedName] = []
    read = port.observe_agent_versions

    def counted(qualified_name: QualifiedName) -> AgentVersions:
        listed.append(qualified_name)
        return read(qualified_name)

    port.observe_agent_versions = counted  # type: ignore[method-assign]
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
        artifact_types=frozenset(("agent",)),
        observed_object_types={"agent": frozenset(("AGENT",))},
        desired_artifacts={artifact.key: artifact},
        options=ObserveOptions(grants=False, capture_prior=True),
    )
    assert diagnostics == ()
    observed = observation.artifacts[artifact.key]
    assert observed.has_live_version and observed.aliases == ("PROD",)
    assert observed.definition is not None
    assert observed.definition.startswith('-- agent DB."SCHEMA".V serves the live version (alias PROD).\n')
    assert listed == [artifact.target]


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
    assert observation.artifacts[second.key].definition == "create or replace semantic view B\n  COPY GRANTS"
    assert [(item.code, item.severity) for item in diagnostics] == [("SST-PLN037", Severity.WARNING)]


# GET_DDL's form of a procedure: no COPY GRANTS, a parenthesis inside a quoted name and the body.
GET_DDL_PROCEDURE = (
    'CREATE OR REPLACE PROCEDURE DB.SCHEMA."V(x)"("ID" VARCHAR, "N" NUMBER(38,0))\n'
    "RETURNS VARCHAR\nLANGUAGE SQL\nEXECUTE AS CALLER\nAS 'BEGIN RETURN '')''; END';\n"
)


def test_a_routine_s_prior_gains_copy_grants_after_its_argument_list() -> None:
    port = FakeSnowflake(definitions={'DB.SCHEMA."V(x)"(VARCHAR, NUMBER)': GET_DDL_PROCEDURE})
    found, quiet = prior_definition(
        port, "PROCEDURE", _row("PROCEDURE", "V(x)"), _desired(signature=("VARCHAR", "NUMBER"))
    )
    assert quiet == ()
    assert found == GET_DDL_PROCEDURE.replace("NUMBER(38,0))\n", "NUMBER(38,0)) COPY GRANTS\n", 1)


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("CREATE OR REPLACE PROCEDURE P() COPY GRANTS RETURNS INT AS '1'", None),
        ("CREATE OR REPLACE SECURE FUNCTION DB.S.F() RETURNS INT AS '1'", None),
        ("CREATE FUNCTION F() RETURNS INT AS '1'", "it is not a CREATE OR REPLACE"),
        ("CREATE OR REPLACE TABLE T (A INT)", "it does not create a procedure or function"),
        ("CREATE OR REPLACE 'x' FUNCTION F() AS '1'", "it does not create a procedure or function"),
        ("CREATE OR REPLACE FUNCTION F RETURNS INT", "its name is not followed by its argument list"),
        ("CREATE OR REPLACE FUNCTION F(A INT", "its argument list does not close"),
        ("CREATE OR REPLACE FUNCTION F() AS 'unterminated", "it does not lex"),
    ],
    ids=["has it", "secure", "no replace", "not a routine", "odd word", "no list", "unclosed", "unlexed"],
)
def test_a_routine_prior_of_another_shape_is_kept_under_a_comment(text: str, reason: str | None) -> None:
    found = restorable_routine(text)
    if reason is None:
        assert "COPY GRANTS" in found and not found.startswith("--")
        assert found.count("COPY GRANTS") == 1
    else:
        assert found.startswith(f"-- SST did not add COPY GRANTS to this definition: {reason}") and found.endswith(text)


def test_a_search_service_s_prior_is_followed_by_the_grants_it_held() -> None:
    port = FakeSnowflake(definitions={"DB.SCHEMA.V": "create or replace cortex search service DB.SCHEMA.V;\n"})
    grants = (
        GrantRow("USAGE", "ROLE", "READER"),
        GrantRow("OWNERSHIP", "ROLE", "DEPLOYER"),
        GrantRow("USAGE", "DATABASE_ROLE", "DB.analyst", grant_option=True),
        GrantRow("USAGE", "ROLE", ""),
    )
    found, _ = prior_definition(port, "CORTEX SEARCH SERVICE", _row("CORTEX SEARCH SERVICE"), _desired(), grants=grants)
    assert found == (
        "create or replace cortex search service DB.SCHEMA.V;\n"
        "-- Running the replace above revokes these grants; they give them back:\n"
        'GRANT USAGE ON CORTEX SEARCH SERVICE DB."SCHEMA".V TO DATABASE ROLE DB."analyst" WITH GRANT OPTION;\n'
        "-- grant USAGE to ROLE  again by hand.\n"
        'GRANT USAGE ON CORTEX SEARCH SERVICE DB."SCHEMA".V TO ROLE READER;\n'
    )
    unread, _ = prior_definition(port, "CORTEX SEARCH SERVICE", _row("CORTEX SEARCH SERVICE"), _desired(), grants=None)
    assert unread is not None and unread.endswith("so grant them again.\n")
