"""Apply against a real account: what only Snowflake can say about the write path.

Golden files prove the DDL is the intended text; this layer proves Snowflake accepts it and that
the object it leaves behaves -- the view carries SST's ownership marker, answers a metric query,
leaves nothing for the next plan to do, and keeps its grants across a replace. Assertions are on
outcomes read back through the connector, never on DDL text.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.sql import ident, qname, sql
from tests.helpers.e2e_cli import SstRun, run_sst
from tests.helpers.live_project import live_view, project_args, published_project
from tests.helpers.live_snowflake import LiveAccount

pytestmark = pytest.mark.live

# A role the test role may grant SELECT to; the grant test is skipped without one.
GRANTEE_ROLE = "SST_TEST_SNOWFLAKE_GRANTEE_ROLE"


@dataclass(frozen=True)
class Applied:
    schema: SchemaScope
    project: Path
    args: list[str]
    first: SstRun


@pytest.fixture(scope="module")
def applied(
    tmp_path_factory: pytest.TempPathFactory,
    live_account: LiveAccount,
    live_connector: SnowflakeConnector,
    scratch_schema: Callable[[str], SchemaScope],
) -> Applied:
    schema = scratch_schema("apply")
    project, manifest = published_project(
        tmp_path_factory.mktemp("apply"), live_connector, schema, key_pair=live_account.key_pair
    )
    args = project_args(project, manifest)
    compiled = run_sst("compile", *args)
    assert compiled.exit_code == 0, compiled.stdout + compiled.stderr
    return Applied(schema, project, args, run_sst("--output", "json", "apply", *args, "--yes"))


def test_apply_publishes_a_view_that_carries_its_marker_and_answers_a_metric(
    applied: Applied, live_connector: SnowflakeConnector
) -> None:
    assert applied.first.exit_code == 0, applied.first.stdout
    view = live_view(applied.schema)
    assert live_connector.describe_marker(view, "SEMANTIC VIEW") is not None
    answered = live_connector.query(
        sql("SELECT SV.ORDER_COUNT FROM SEMANTIC_VIEW({view} METRICS ORDERS.ORDER_COUNT) AS SV", view=qname(view))
    )
    assert answered.rows == ((2,),)


def test_a_plan_after_apply_has_nothing_left_to_do(applied: Applied) -> None:
    assert applied.first.exit_code == 0
    replanned = run_sst("--output", "json", "plan", *applied.args)
    assert replanned.exit_code == 0, replanned.stdout


def test_replacing_the_view_keeps_the_grants_on_it(
    applied: Applied, live_account: LiveAccount, live_connector: SnowflakeConnector
) -> None:
    grantee = live_account.grantee_role
    if not grantee:
        pytest.skip(f"{GRANTEE_ROLE} names no role to grant SELECT to")
    view = live_view(applied.schema)
    granted = live_connector.execute_script(
        (
            sql(
                "GRANT SELECT ON SEMANTIC VIEW {view} TO ROLE {role}",
                view=qname(view),
                role=ident(Identifier.parse(grantee)),
            ),
        )
    )
    assert granted.ok, granted.error
    views = applied.project / "semantic_models" / "semantic_views" / "live.yml"
    views.write_text(views.read_text(encoding="utf-8").replace("worth.", "worth, in cents."), encoding="utf-8")
    assert run_sst("compile", *applied.args).exit_code == 0
    replaced = run_sst("--output", "json", "apply", *applied.args, "--yes")
    assert replaced.exit_code == 0, replaced.stdout
    grantees = {(row.privilege, row.grantee_name.upper()) for row in live_connector.show_grants("SEMANTIC VIEW", view)}
    assert ("SELECT", grantee.upper()) in grantees, "replacing the view dropped a grant on it"
