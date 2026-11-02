"""The library interface to SST: load a project once, then ask it what it declares and what is wrong.

This package and the CLI are SST's two supported interfaces; every other module is internal and
may change in any release. See `docs/guides/python-api.md`.
"""

from snowflake_semantic_tools.api.authoring_context import (
    AuthoringContext,
    ColumnName,
    ModelName,
    SourceName,
    Variable,
)
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
from snowflake_semantic_tools.domain.render.field_vocabulary import FieldVocabulary, TemplateFunction

__all__ = [
    "AuthoringContext",
    "BlockLocations",
    "ColumnName",
    "ConnectionParams",
    "Deployed",
    "FieldVocabulary",
    "Lineage",
    "LineageEdge",
    "LineageNode",
    "LocationKey",
    "Locations",
    "ModelName",
    "ObjectSummary",
    "Placement",
    "ProjectSession",
    "Rendered",
    "RouteFolder",
    "Setting",
    "SourceName",
    "TargetLocation",
    "TemplateFunction",
    "Validation",
    "Variable",
    "deployed",
    "schemas",
]
