"""What SST writes to Snowflake, as tables: each key, what fills it, and where Snowflake documents it.

`AGENT_SPEC` and `TOOL_RESOURCES` are an agent's specification; `COWORK_SPEC` is a CoWork
profile's. `docs` holds every Snowflake documentation page the package cites.
"""

from snowflake_semantic_tools.domain.model.snowflake_spec import docs
from snowflake_semantic_tools.domain.model.snowflake_spec.agent import (
    AGENT_NOT_IN_SPEC,
    AGENT_PASSTHROUGH,
    AGENT_SPEC,
    EXECUTION_ENVIRONMENT,
    TOOL_RESOURCES,
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
    sources,
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
    "docs",
    "sources",
    "walk",
]
