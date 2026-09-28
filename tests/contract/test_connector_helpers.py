from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import (
    _json_resources,
    _object_type,
    _physical_resources,
    _port_error,
)
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
from snowflake_semantic_tools.domain.state.model import AppliedEntry, AppliedResource, ResourceStatus


def test_connector_validates_object_type_and_preserves_error_metadata() -> None:
    assert _object_type(" semantic   view ") == "SEMANTIC VIEW"
    with pytest.raises(SnowflakePortError, match="unsupported"):
        _object_type("SEMANTIC VIEW; DROP DATABASE X")

    class Error(Exception):
        sqlstate = "42000"
        errno = 1

    result = _port_error(Error("bad"))
    assert (str(result), result.sqlstate, result.errno) == ("bad", "42000", 1)


def test_state_resource_json_preserves_retention_status() -> None:
    entry = AppliedEntry(
        "a" * 64,
        "DB.S.V",
        "now",
        "run",
        "applied",
        "a" * 64,
        "m",
        physical_resources=(AppliedResource("DATASET", "DB.S.V", ResourceStatus.RETAINED),),
    )

    encoded = _json_resources(entry)

    assert _physical_resources(encoded) == entry.physical_resources
