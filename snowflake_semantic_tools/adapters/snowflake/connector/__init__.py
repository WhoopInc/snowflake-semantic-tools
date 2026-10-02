"""The production Snowflake connector: one driver session serving every role of `SnowflakePort`.

`session` owns the connection, its lock, SQL execution, and the translation of driver
failures into `SnowflakePortError`. `catalog`, `preflight` (built on `catalog`), `stage`,
`profile_registry`, and `state_table` each implement one role protocol on that session, as
`profiler` and `cortex` implement the ones `sst enrich` reads through, and
`SnowflakeConnector` assembles them into the one class importers construct.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector.cortex import CortexMethods
from snowflake_semantic_tools.adapters.snowflake.connector.preflight import PreflightMethods
from snowflake_semantic_tools.adapters.snowflake.connector.profile_registry import ProfileRegistryMethods
from snowflake_semantic_tools.adapters.snowflake.connector.profiler import ProfilerMethods
from snowflake_semantic_tools.adapters.snowflake.connector.stage import StageMethods
from snowflake_semantic_tools.adapters.snowflake.connector.state_table import StateTableMethods


class SnowflakeConnector(
    PreflightMethods, StageMethods, ProfileRegistryMethods, StateTableMethods, ProfilerMethods, CortexMethods
):
    """Reach Snowflake through one driver connection, implementing every port role.

    Constructing one connects; `close` ends the session. Every role shares the session's
    lock, so statements from concurrent callers never interleave on the connection.

    Raises:
        SnowflakePortError: connecting failed.
    """


__all__ = ["SnowflakeConnector"]
