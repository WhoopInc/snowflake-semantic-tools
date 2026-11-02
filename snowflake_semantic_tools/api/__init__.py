"""The library interface to SST: load a project once, then ask it what it declares and what is wrong.

This package and the CLI are SST's two supported interfaces; every other module is internal and
may change in any release. See `docs/guides/python-api.md`.
"""

from snowflake_semantic_tools.api.deployed import ConnectionParams, Deployed, deployed
from snowflake_semantic_tools.api.lineage import Lineage, LineageEdge, LineageNode
from snowflake_semantic_tools.api.locations import (
    BlockLocations,
    LocationKey,
    Locations,
    Placement,
    RouteFolder,
    TargetLocation,
)
from snowflake_semantic_tools.api.schemas import schemas
from snowflake_semantic_tools.api.session import ObjectSummary, ProjectSession, Rendered, Validation
from snowflake_semantic_tools.domain.model.setting import Setting

__all__ = [
    "BlockLocations",
    "ConnectionParams",
    "Deployed",
    "Lineage",
    "LineageEdge",
    "LineageNode",
    "LocationKey",
    "Locations",
    "ObjectSummary",
    "Placement",
    "ProjectSession",
    "Rendered",
    "RouteFolder",
    "Setting",
    "TargetLocation",
    "Validation",
    "deployed",
    "schemas",
]
