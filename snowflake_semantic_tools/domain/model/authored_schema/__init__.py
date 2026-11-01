"""The declared shape of every authored file kind: its keys, their nesting, kinds and statuses.

`field_types` holds the vocabulary; one module per family of kinds holds its tables, and the loaders
and validators read the name sets derived from them. `sst_config.yml` itself is not here: its
table is `config_schema`'s, which a kind's `config:` field points at by block name. This
module re-exports the public surface, so importers never reach into the submodules.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.authored_schema.agents import AGENT, AGENT_TOOL, COWORK_PROFILE
from snowflake_semantic_tools.domain.model.authored_schema.dbt_meta import DBT_COLUMN_META, DBT_MODEL_META
from snowflake_semantic_tools.domain.model.authored_schema.evals import EVAL_CONFIG, EVAL_DATASET, EVAL_METRIC
from snowflake_semantic_tools.domain.model.authored_schema.extensions import (
    COCO_PROFILE,
    COCO_SHARED_PROFILE,
    PLUGIN,
    SKILL_FRONT_MATTER,
)
from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    READ_STATUSES,
    Field,
    FieldStatus,
    FileKind,
    Node,
    NodePattern,
    ValueKind,
    node_patterns,
    unread_fields,
    walk,
)
from snowflake_semantic_tools.domain.model.authored_schema.semantic import SEMANTIC_KINDS
from snowflake_semantic_tools.domain.model.authored_schema.tools import (
    CREATION_KEYS,
    MOVED_TOOL_KEYS,
    TOOL_COLUMN_KEYS,
    TOOL_MEMBER,
    TOOL_MEMBER_KEYS,
)

# Every file kind, by name, in the order the reference lists them.
FILE_KINDS: Mapping[str, FileKind] = MappingProxyType(
    {
        kind.name: kind
        for kind in (
            *SEMANTIC_KINDS,
            TOOL_MEMBER,
            AGENT,
            COWORK_PROFILE,
            SKILL_FRONT_MATTER,
            PLUGIN,
            COCO_PROFILE,
            COCO_SHARED_PROFILE,
            EVAL_METRIC,
            EVAL_DATASET,
            EVAL_CONFIG,
            DBT_MODEL_META,
            DBT_COLUMN_META,
        )
    }
)

__all__ = [
    "AGENT",
    "AGENT_TOOL",
    "COCO_PROFILE",
    "COCO_SHARED_PROFILE",
    "COWORK_PROFILE",
    "CREATION_KEYS",
    "DBT_COLUMN_META",
    "DBT_MODEL_META",
    "EVAL_CONFIG",
    "EVAL_DATASET",
    "EVAL_METRIC",
    "FILE_KINDS",
    "MOVED_TOOL_KEYS",
    "PLUGIN",
    "READ_STATUSES",
    "SEMANTIC_KINDS",
    "SKILL_FRONT_MATTER",
    "TOOL_COLUMN_KEYS",
    "TOOL_MEMBER",
    "TOOL_MEMBER_KEYS",
    "Field",
    "FieldStatus",
    "FileKind",
    "Node",
    "NodePattern",
    "ValueKind",
    "node_patterns",
    "unread_fields",
    "walk",
]
