"""SST-VAL415: a verified query runs and returns no rows.

Connected validation runs each verified query under a row count once it compiles; the fake
session answers the count from `table_row_counts`, keyed by the counted query.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table, VerifiedQuery
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored, authored_query, statement

COUNTED = "(SELECT COUNT(*) FROM DB.S.ORDERS) AS SST_VQ"


def _validated(port: FakeSnowflake) -> list[Diagnostic]:
    query = VerifiedQuery("order_total", "How many orders?", authored_query("SELECT COUNT(*) FROM DB.S.ORDERS"))
    view = SemanticView(
        "DB.S.SALES",
        (Table("ORDERS", "DB.S.ORDERS"),),
        metrics=(Metric("ORDER_COUNT", authored("COUNT(*)"), "ORDERS"),),
        verified_queries=(query,),
    )
    compiled = CompileResult((CompiledView(view, statement("CREATE SEMANTIC VIEW DB.S.SALES")),))
    result = ValidateArtifacts(port, clock=FixedClock()).run(compiled, strict=False, connected=True)
    return coded(result.diagnostics, "SST-VAL415")


def test_sst_val415_fires() -> None:
    [diagnostic] = _validated(FakeSnowflake())
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "verified_query 'order_total' executed and returned 0 rows in 1ms"
    assert diagnostic.subject == "semantic_view:sales"


def test_sst_val415_silent() -> None:
    port = FakeSnowflake()
    port.table_row_counts[COUNTED] = 12
    assert _validated(port) == []
