"""SST-PLN041: the live definition a planned update replaces could not be read to compare."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.plan_diff import diff_updates
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView, Table
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.plan_codes import view
from tests.helpers.semantic_view_rows import described_view
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

_MODEL = SemanticView(fqn="DB.SCH.SALES", tables=(Table("ORDERS", "DB.MARTS.ORDERS"),))
_CHANGE = Change(
    "semantic_view:sales", "semantic_view", Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, view("sales"), None, (), 0
)


def _diff(port: FakeSnowflake) -> tuple[tuple[Change, ...], tuple[object, ...]]:
    compiled = {"semantic_view:sales": CompiledView(_MODEL, statement("CREATE SEMANTIC VIEW DB.SCH.SALES"))}
    return diff_updates((_CHANGE,), compiled, Fanout(port))


def test_sst_pln041_fires() -> None:
    port = FakeSnowflake()
    port.fail("describe_semantic_view", SnowflakePortError("denied"))
    [change], [diagnostic] = _diff(port)
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN041", Severity.INFO)  # type: ignore[attr-defined]
    assert diagnostic.message == (  # type: ignore[attr-defined]
        "what the update of DB.SCH.SALES changes was not compared member by member: denied"
    )
    # The update is planned all the same; only its members are unknown.
    assert (change.action, change.member_diff) == (Action.UPDATE, None)


def test_sst_pln041_silent() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_MODEL)
    [change], diagnostics = _diff(port)
    assert diagnostics == ()
    assert change.member_diff == ()
