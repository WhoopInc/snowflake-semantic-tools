"""The production Snowflake connector: one driver session serving every role of `SnowflakePort`.

`session` owns the connection, its lock, SQL execution, and the translation of driver
failures into `SnowflakePortError`. `catalog`, `stage`, `profile_registry`, and
`state_table` each implement one role protocol on that session, and `SnowflakeConnector`
assembles them into the one class importers construct.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector.catalog import CatalogMethods
from snowflake_semantic_tools.adapters.snowflake.connector.profile_registry import ProfileRegistryMethods
from snowflake_semantic_tools.adapters.snowflake.connector.stage import StageMethods
from snowflake_semantic_tools.adapters.snowflake.connector.state_table import StateTableMethods


class SnowflakeConnector(CatalogMethods, StageMethods, ProfileRegistryMethods, StateTableMethods):
    """Reach Snowflake through one driver connection, implementing every `SnowflakePort` role.

    Constructing one connects; `close` ends the session. Every role shares the session's
    lock, so statements from concurrent callers never interleave on the connection.

    Raises:
        SnowflakePortError: connecting failed.
    """


__all__ = ["SnowflakeConnector"]
