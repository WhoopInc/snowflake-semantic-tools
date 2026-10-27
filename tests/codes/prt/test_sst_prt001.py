"""SST-PRT001: the connector could not open a session for a reason other than the credential."""

from __future__ import annotations

from snowflake.connector.errors import Error as DriverError

from snowflake_semantic_tools.adapters.snowflake.connector.session import _port_error
from snowflake_semantic_tools.domain.diagnostics import Severity


def _failure(message: str, sqlstate: str | None, errno: int) -> DriverError:
    failure = DriverError(message)
    failure.sqlstate = sqlstate  # type: ignore[assignment]
    failure.errno = errno
    return failure


def test_sst_prt001_fires() -> None:
    error = _port_error(_failure("Could not connect to Snowflake backend", "08001", 250001), connecting_to="acme")
    diagnostic = error.diagnostic
    assert diagnostic is not None and (diagnostic.code, diagnostic.severity) == ("SST-PRT001", Severity.ERROR)
    assert diagnostic.message == "could not connect to acme: Could not connect to Snowflake backend"


def test_sst_prt001_silent() -> None:
    error = _port_error(_failure("Could not connect to Snowflake backend", "08001", 250001))
    assert error.diagnostic is not None and error.diagnostic.code != "SST-PRT001"
