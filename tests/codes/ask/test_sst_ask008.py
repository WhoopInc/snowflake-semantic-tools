"""SST-ASK008: a scratch copy `sst ask` makes of an edited dependency was refused."""

from __future__ import annotations

from snowflake_semantic_tools.app.ask_scratch import Edited, ScratchSession
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.compile_builders import compiled
from tests.helpers.snowflake_fake import FakeSnowflake, failed

_WHERE = SchemaScope.parse("SCR.X")


def test_sst_ask008_fires() -> None:
    port = FakeSnowflake()
    port.execute_results = [failed("Insufficient privileges to operate on schema 'X'")]
    session = ScratchSession(port, _WHERE, "ab12cd34")
    diagnostic = session.build(Edited(compiled("SALES").compiled))
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK008", Severity.ERROR)
    assert diagnostic.message == (
        "semantic_view:sales: the scratch copy SCR.X.SALES__SST_ASK_AB12CD34 could not be created: "
        "Insufficient privileges to operate on schema 'X'"
    )
    assert session.copies == []


def test_sst_ask008_silent() -> None:
    session = ScratchSession(FakeSnowflake(), _WHERE, "ab12cd34")
    assert session.build(Edited(compiled("SALES").compiled)) is None
    assert [copy.name.sql for copy in session.copies] == ["SCR.X.SALES__SST_ASK_AB12CD34"]
