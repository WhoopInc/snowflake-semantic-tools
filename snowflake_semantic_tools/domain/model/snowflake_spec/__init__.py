"""What SST writes to Snowflake, as tables: each key, what fills it, and where Snowflake documents it.

`AGENT_SPEC` and `TOOL_RESOURCES` are an agent's specification; `COWORK_SPEC` is a CoWork
profile's. Each cites its pages from `domain.snowflake_docs`.
"""

from snowflake_semantic_tools.domain.model.snowflake_spec.agent import (
    AGENT_NOT_IN_SPEC,
    AGENT_PASSTHROUGH,
    AGENT_SPEC,
    EXECUTION_ENVIRONMENT,
    TOOL_RESOURCES,
    TOOL_SPEC,
)
from snowflake_semantic_tools.domain.model.snowflake_spec.cowork import (
    COWORK_NOT_IN_SPEC,
    COWORK_PASSTHROUGH,
    COWORK_SPEC,
    ROUTER_NAME,
    ROUTER_RESOURCES,
    ROUTER_TYPE,
    SUBAGENT_SOURCE,
)
from snowflake_semantic_tools.domain.model.snowflake_spec.model import (
    ANY,
    Authored,
    Derived,
    Documented,
    Evidence,
    Fixed,
    Measured,
    Setting,
    Source,
    SpecKey,
    SpecNode,
    SpecPath,
    merges_into,
    passthrough_split,
    settings,
    sources,
    unmodelled,
    walk,
)

__all__ = [
    "AGENT_NOT_IN_SPEC",
    "AGENT_PASSTHROUGH",
    "AGENT_SPEC",
    "ANY",
    "COWORK_NOT_IN_SPEC",
    "COWORK_PASSTHROUGH",
    "COWORK_SPEC",
    "EXECUTION_ENVIRONMENT",
    "ROUTER_NAME",
    "ROUTER_RESOURCES",
    "ROUTER_TYPE",
    "SUBAGENT_SOURCE",
    "TOOL_RESOURCES",
    "TOOL_SPEC",
    "Authored",
    "Derived",
    "Documented",
    "Evidence",
    "Fixed",
    "Measured",
    "Setting",
    "Source",
    "SpecKey",
    "SpecNode",
    "SpecPath",
    "merges_into",
    "passthrough_split",
    "settings",
    "sources",
    "unmodelled",
    "walk",
]
