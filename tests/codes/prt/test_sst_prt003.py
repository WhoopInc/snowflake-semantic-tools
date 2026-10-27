"""SST-PRT003: a statement exceeded its deadline, or the connection dropped mid-statement."""

from __future__ import annotations

from snowflake.connector.errors import Error as DriverError

from snowflake_semantic_tools.adapters.snowflake.connector.session import _port_error
from snowflake_semantic_tools.domain.diagnostics import Severity


def _failure(message: str, sqlstate: str | None) -> DriverError:
    failure = DriverError(message)
    failure.sqlstate = sqlstate  # type: ignore[assignment]
    return failure


def test_sst_prt003_fires() -> None:
    diagnostic = _port_error(_failure("Statement reached its statement or warehouse timeout", None)).diagnostic
    assert diagnostic is not None and (diagnostic.code, diagnostic.severity) == ("SST-PRT003", Severity.ERROR)
    assert diagnostic.message == (
        "query timed out after its deadline (Statement reached its statement or warehouse timeout)"
    )


def test_sst_prt003_silent() -> None:
    assert _port_error(_failure("SQL compilation error", "42000")).diagnostic is None
