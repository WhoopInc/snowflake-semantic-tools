"""SST-PRT002: Snowflake rejected the session's credential at login."""

from __future__ import annotations

from snowflake.connector.errors import Error as DriverError

from snowflake_semantic_tools.adapters.snowflake.connector.session import _port_error
from snowflake_semantic_tools.domain.diagnostics import Severity


def _failure(message: str, errno: int) -> DriverError:
    failure = DriverError(message)
    failure.sqlstate = "08001"
    failure.errno = errno
    return failure


def test_sst_prt002_fires() -> None:
    error = _port_error(_failure("Incorrect username or password was specified.", 390100), connecting_to="acme")
    diagnostic = error.diagnostic
    assert diagnostic is not None and (diagnostic.code, diagnostic.severity) == ("SST-PRT002", Severity.ERROR)
    assert diagnostic.message == "authentication failed for acme"


def test_sst_prt002_silent() -> None:
    error = _port_error(_failure("Could not connect to Snowflake backend", 250001), connecting_to="acme")
    assert error.diagnostic is not None and error.diagnostic.code == "SST-PRT001"
