"""SST-PRT004: Snowflake refused a statement because the session's role lacks a privilege."""

from __future__ import annotations

from snowflake.connector.errors import Error as DriverError

from snowflake_semantic_tools.adapters.snowflake.connector.session import _port_error
from snowflake_semantic_tools.domain.diagnostics import Severity


def _failure(message: str, sqlstate: str) -> DriverError:
    failure = DriverError(message)
    failure.sqlstate = sqlstate
    return failure


def test_sst_prt004_fires() -> None:
    diagnostic = _port_error(_failure("Insufficient privileges to operate on schema 'S'", "42501")).diagnostic
    assert diagnostic is not None and (diagnostic.code, diagnostic.severity) == ("SST-PRT004", Severity.ERROR)
    assert diagnostic.message == (
        "the session's role lacks a privilege the statement needs (Insufficient privileges to operate on schema 'S')"
    )


def test_sst_prt004_silent() -> None:
    assert _port_error(_failure("Object does not exist", "02000")).diagnostic is None
