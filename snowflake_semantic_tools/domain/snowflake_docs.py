"""The Snowflake documentation pages that define what SST writes and reads, one constant per page.

Every link to Snowflake's documentation in the package comes from here, so a page that moves
is one edit. It imports nothing of the package, so any module may cite a page: a docstring by
its constant's name, a message by its `url`. Each page below was found with the Snowflake
documentation search except the two CoWork pages: they are preview pages the public search does
not index, and SST has cited them since the feature shipped. Snowflake documents these contracts
in prose, with no machine-readable schema, so the pages are cited for readers; nothing is
validated against them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DocPage:
    """One Snowflake documentation page.

    Attributes:
        title: The page's title, as the page gives it.
        url: Its address, always under `https://docs.snowflake.com/en/`.
        defines: What SST relies on it for.
        preview: The feature it describes is in preview (a LIMITEDACCESS page), so it may change.
        searchable: The Snowflake documentation search finds it, so its address was checked there.
    """

    title: str
    url: str
    defines: str
    preview: bool = False
    searchable: bool = True


CREATE_AGENT = DocPage(
    "CREATE AGENT",
    "https://docs.snowflake.com/en/sql-reference/sql/create-agent",
    "Creating an agent from a specification.",
)
ALTER_AGENT = DocPage(
    "ALTER AGENT",
    "https://docs.snowflake.com/en/sql-reference/sql/alter-agent",
    "The specification's top-level layout, and the comment, profile and secure flag set beside it.",
)
AGENTS_REST_API = DocPage(
    "Cortex Agents REST API",
    "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api",
    "The specification's schemas: models, orchestration, instructions, tools, tool resources, "
    "and the execution environment.",
)
MANAGE_AGENTS = DocPage(
    "Create and manage agents",
    "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage",
    "Each tool type's resources, a search tool's columns among them.",
)
SEARCH_IN_AGENTS = DocPage(
    "Use Cortex Search with Cortex Agents",
    "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-agents",
    "A search tool's resources: its columns, and the stage its documents open from.",
)
INACCESSIBLE_TOOLS = DocPage(
    "Inaccessible tool handling",
    "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-inaccessible-tool-handling",
    "`orchestration.tool_not_accessible`.",
)
AGENT_TOOLSETS = DocPage(
    "Agent toolsets",
    "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-toolsets",
    "An `agent_toolset` tool and its `agent_name` resource.",
)
AGENT_SKILLS = DocPage(
    "Agent skills",
    "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-skills",
    "The `skills` section: each skill's name and source.",
)
COWORK_PROFILES = DocPage(
    "About CoWork profiles",
    "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles",
    "CoWork profiles: a router agent over subagents.",
    preview=True,
    searchable=False,
)
COWORK_ROUTING = DocPage(
    "Set up CoWork profile routing",
    "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing",
    "The router tool and its `subagents` resource.",
    preview=True,
    searchable=False,
)
READ_YAML_FROM_SEMANTIC_VIEW = DocPage(
    "SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW",
    "https://docs.snowflake.com/en/sql-reference/functions/system_read_yaml_from_semantic_view",
    "Reading a semantic view back as YAML.",
)
SEMANTIC_VIEW_YAML = DocPage(
    "YAML specification for semantic views",
    "https://docs.snowflake.com/en/user-guide/views-semantic/semantic-view-yaml-spec",
    "The keys of a semantic view's YAML.",
)
PAGES: tuple[DocPage, ...] = (
    CREATE_AGENT,
    ALTER_AGENT,
    AGENTS_REST_API,
    MANAGE_AGENTS,
    SEARCH_IN_AGENTS,
    INACCESSIBLE_TOOLS,
    AGENT_TOOLSETS,
    AGENT_SKILLS,
    COWORK_PROFILES,
    COWORK_ROUTING,
    READ_YAML_FROM_SEMANTIC_VIEW,
    SEMANTIC_VIEW_YAML,
)
