"""SST-ASK009: a scratch copy or a thread a session made could not be removed."""

from __future__ import annotations

from snowflake_semantic_tools.app.ask_scratch import Edited, ScratchSession
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.compile_builders import compiled
from tests.helpers.snowflake_fake import FakeSnowflake, failed

_WHERE = SchemaScope.parse("SCR.X")


def _built(port: FakeSnowflake) -> ScratchSession:
    session = ScratchSession(port, _WHERE, "ab12cd34")
    assert session.build(Edited(compiled("SALES").compiled)) is None
    return session


def test_sst_ask009_fires() -> None:
    port = FakeSnowflake()
    session = _built(port)
    port.execute_results = [failed("Object does not exist, or operation cannot be performed")]
    [diagnostic] = session.drop()
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK009", Severity.WARNING)
    assert diagnostic.message == (
        "SCR.X.SALES__SST_ASK_AB12CD34 could not be removed: Object does not exist, or operation cannot be performed"
    )


def test_sst_ask009_silent() -> None:
    assert _built(FakeSnowflake()).drop() == ()
