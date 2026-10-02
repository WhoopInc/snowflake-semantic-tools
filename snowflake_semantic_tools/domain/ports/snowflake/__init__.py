"""The interfaces application use cases reach Snowflake through, one protocol per role.

`catalog.CatalogPort` reads what exists and how it is defined, `execution.ExecutionPort`
runs SQL, `stage.StagePort` moves files on stages and in extension versions,
`profile_registry.ProfileRegistryPort` keeps CoCo Desktop's profile registry, and
`state.StatePort` keeps the state table; each raises `errors.SnowflakePortError`.
`SnowflakePort`, defined here, is their union: the composition root builds one, and each
use case asks for only the roles it uses.
"""

from __future__ import annotations

from typing import Protocol

from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.ports.snowflake.profile_registry import ProfileRegistryPort
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagePort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort


class SnowflakePort(CatalogPort, ExecutionPort, StagePort, ProfileRegistryPort, StatePort, Protocol):
    """Everything application use cases reach Snowflake for: the union of the role protocols.

    It declares nothing of its own, so each method's contract lives on its role. An adapter
    satisfies it structurally, without subclassing it.
    """
