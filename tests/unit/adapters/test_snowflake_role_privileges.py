"""One role's schema privileges, walked up from the schema's grants: what is read, and what is held."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector.grant_walk import Grantee, grantees_of, schema_holders
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.preflight import ROLE_HIERARCHY_DEPTH
from snowflake_semantic_tools.domain.sql import sql
from tests.helpers.snowflake_fake.driver import FakeDriverConnector
from tests.helpers.snowflake_fake.grants import GrantGraph

SCOPE = SchemaScope(Identifier.parse("DB"), Identifier.parse("SCH"))
RUN = ("CREATE TASK", "CREATE STAGE", "CREATE FILE FORMAT")
ON_SCHEMA = "SHOW GRANTS ON SCHEMA DB.SCH"


def _missing(graph: GrantGraph, role: str, privileges: tuple[str, ...] = RUN) -> tuple[tuple[str, ...], list[str]]:
    """What `role` lacks per the connector over `graph`, and every statement it sent."""
    session = graph.session()
    missing = FakeDriverConnector(session).missing_role_privileges(role, SCOPE, privileges)
    assert not [statement for statement in session.statements if statement.startswith("SHOW GRANTS TO")]
    return missing, session.statements


def test_a_direct_holder_holds_without_reading_any_role() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role="RUNNER").on_schema("CREATE STAGE", role="RUNNER")
    graph.on_schema("CREATE FILE FORMAT", role="OTHER").role_to("OTHER", user="SOMEONE")
    missing, sent = _missing(graph, "RUNNER", ("CREATE TASK", "CREATE STAGE"))
    assert missing == ()
    assert sent == [ON_SCHEMA]


def test_a_privilege_no_role_holds_is_missing_after_one_read() -> None:
    graph = GrantGraph(SCOPE).on_schema("USAGE", role="RUNNER").role_to("RUNNER", user="ME")
    missing, sent = _missing(graph, "RUNNER")
    assert missing == RUN
    assert sent == [ON_SCHEMA]


def test_a_privilege_is_inherited_through_one_role() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role="BUILDER").role_to("BUILDER", role_to="RUNNER")
    graph.role_to("BUILDER", user="SOMEONE")
    missing, sent = _missing(graph, "RUNNER", ("CREATE TASK",))
    assert missing == ()
    assert sent == [ON_SCHEMA, "SHOW GRANTS OF ROLE BUILDER"]


def test_a_privilege_is_inherited_through_three_roles() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role="R3")
    graph.role_to("R3", role_to="R2").role_to("R2", role_to="R1").role_to("R1", role_to="RUNNER")
    missing, sent = _missing(graph, "RUNNER")
    assert missing == ("CREATE STAGE", "CREATE FILE FORMAT")
    assert sent == [ON_SCHEMA, "SHOW GRANTS OF ROLE R3", "SHOW GRANTS OF ROLE R2", "SHOW GRANTS OF ROLE R1"]


def test_a_privilege_is_inherited_through_a_database_role_and_a_nested_one() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE STAGE", database_role="WRITER")
    graph.database_role_to("WRITER", role_to="RUNNER")
    graph.on_schema("CREATE TASK", database_role="TASKS").database_role_to("TASKS", database_role_to="WRITER")
    missing, sent = _missing(graph, "RUNNER")
    assert missing == ("CREATE FILE FORMAT",)
    assert sent == [ON_SCHEMA, "SHOW GRANTS OF DATABASE ROLE DB.WRITER", "SHOW GRANTS OF DATABASE ROLE DB.TASKS"]


def test_ownership_of_the_schema_holds_every_privilege() -> None:
    graph = (
        GrantGraph(SCOPE).on_schema("OWNERSHIP", database_role="OWNERS").database_role_to("OWNERS", role_to="RUNNER")
    )
    missing, sent = _missing(graph, "RUNNER")
    assert missing == ()
    assert sent == [ON_SCHEMA, "SHOW GRANTS OF DATABASE ROLE DB.OWNERS"]


def test_a_privilege_only_a_role_outside_the_primary_hierarchy_holds_is_missing() -> None:
    # The session may hold SECONDARY as a secondary role; a task running as RUNNER never does.
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role="SECONDARY").role_to("SECONDARY", user="ME")
    graph.role_to("RUNNER", user="ME")
    missing, sent = _missing(graph, "RUNNER", ("CREATE TASK",))
    assert missing == ("CREATE TASK",)
    assert sent == [ON_SCHEMA, "SHOW GRANTS OF ROLE SECONDARY"]


def _chain(holder: int) -> GrantGraph:
    """R<n> granted to R<n-1> down to R00, the primary; R<holder> holds CREATE TASK."""
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role=f"R{holder:02d}")
    for level in range(1, 40):
        graph.role_to(f"R{level:02d}", role_to=f"R{level - 1:02d}")
    return graph


def test_the_walk_climbs_no_further_than_the_hierarchy_depth() -> None:
    within, sent = _missing(_chain(ROLE_HIERARCHY_DEPTH - 1), "R00", ("CREATE TASK",))
    assert within == ()
    assert len(sent) == ROLE_HIERARCHY_DEPTH
    beyond, sent = _missing(_chain(ROLE_HIERARCHY_DEPTH), "R00", ("CREATE TASK",))
    assert beyond == ("CREATE TASK",)
    assert len(sent) == ROLE_HIERARCHY_DEPTH
    assert f"SHOW GRANTS OF ROLE R{1:02d}" not in sent


def test_a_cycle_ends_and_each_role_is_read_once_for_every_privilege() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role="A").on_schema("CREATE STAGE", role="B")
    graph.on_schema("CREATE FILE FORMAT", role="A")
    graph.role_to("A", role_to="B").role_to("B", role_to="A").role_to("B", role_to="C")
    missing, sent = _missing(graph, "RUNNER")
    assert missing == RUN
    assert sorted(sent) == sorted(
        [ON_SCHEMA, "SHOW GRANTS OF ROLE A", "SHOW GRANTS OF ROLE B", "SHOW GRANTS OF ROLE C"]
    )


def test_holders_share_reads_and_the_walk_stops_once_everything_is_held() -> None:
    graph = GrantGraph(SCOPE)
    for privilege in RUN:
        graph.on_schema(privilege, role="SHARED")
    graph.role_to("SHARED", role_to="MIDDLE").role_to("MIDDLE", role_to="RUNNER").role_to("MIDDLE", role_to="FAR")
    graph.role_to("FAR", role_to="FARTHER")
    missing, sent = _missing(graph, "RUNNER")
    assert missing == ()
    assert sent == [ON_SCHEMA, "SHOW GRANTS OF ROLE SHARED", "SHOW GRANTS OF ROLE MIDDLE"]


def test_names_with_special_characters_are_quoted_as_identifiers() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", database_role='w"r.iter')
    graph.database_role_to('w"r.iter', role_to="Mixed Case").role_to("Mixed Case", role_to="runner")
    missing, sent = _missing(graph, "runner", ("CREATE TASK",))
    assert missing == ()
    assert sent[1:] == ['SHOW GRANTS OF DATABASE ROLE DB."w""r.iter"', 'SHOW GRANTS OF ROLE "Mixed Case"']
    quoted, _ = _missing(graph, '"runner"', ("CREATE TASK",))
    assert quoted == ()


def test_a_role_whose_grants_cannot_be_read_fails_the_check() -> None:
    graph = GrantGraph(SCOPE).on_schema("CREATE TASK", role="GONE").forget("GONE")
    with pytest.raises(SnowflakePortError, match="does not exist"):
        FakeDriverConnector(graph.session()).missing_role_privileges("RUNNER", SCOPE, ("CREATE TASK",))


def test_the_fake_refuses_the_downward_walk() -> None:
    connector = FakeDriverConnector(GrantGraph(SCOPE).session())
    with pytest.raises(SnowflakePortError, match="does not answer"):
        connector._dict_rows(sql("SHOW GRANTS TO ROLE R"))


def test_grants_to_anything_but_a_role_confer_nothing() -> None:
    rows = (
        {"privilege": "CREATE TASK", "granted_to": "SHARE", "grantee_name": "OUT"},
        {"privilege": "CREATE TASK", "granted_to": "ROLE", "grantee_name": ""},
        {"privilege": "SELECT", "granted_to": "ROLE", "grantee_name": "READER"},
        {"privilege": "create task", "granted_to": "role", "grantee_name": "TASKER"},
    )
    assert schema_holders(rows, SCOPE, ("CREATE TASK",)) == {
        Grantee.role(Identifier.parse("TASKER")): frozenset({"CREATE TASK"})
    }
    writer = Grantee.database_role(Identifier.parse("DB"), Identifier.parse("WRITER"))
    edges = (
        {"granted_to": "USER", "grantee_name": "ME"},
        {"granted_to": "ROLE", "grantee_name": ""},
        {"granted_to": "DATABASE_ROLE", "grantee_name": "OTHER.READER"},
        {"granted_to": "APPLICATION", "grantee_name": "APP"},
    )
    assert grantees_of(writer, edges) == (Grantee.database_role(Identifier.parse("OTHER"), Identifier.parse("READER")),)
    assert grantees_of(Grantee.role(Identifier.parse("R")), edges[2:3]) == ()
