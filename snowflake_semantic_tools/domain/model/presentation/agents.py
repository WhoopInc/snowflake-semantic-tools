"""Presentation of agents, CoWork profiles and tool members."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.presentation.common import (
    display,
    evals,
    instructions,
    retired,
    skills,
    tags,
)
from snowflake_semantic_tools.domain.model.presentation.types import Group, KindUi, ListLayout, ListUi, Ui, Widget

_IDENTITY = Group("identity", "Identity", "What it is called and how people see it.")
_INSTRUCTIONS = Group("instructions", "Instructions", "How it plans, answers, and what it is asked.")
_TOOLS = Group("tools", "Tools", "What it can call to answer.")
_SKILLS = Group("skills", "Skills", "The skills it loads.")
_EVALUATION = Group("evaluation", "Evaluation", "The dataset and config its evals run.")
_SETTINGS = Group("settings", "Settings", "How it publishes, overriding its folder's settings.")
_ROUTING = Group("routing", "Routing", "The agents it hands questions to.")
_ADVANCED = Group("advanced", "Advanced", "Keys added to the specification as written, and metadata.")
_OBJECT = Group("object", "Object", "What Snowflake object it is, or which existing one it names.")
_SEARCH = Group("search", "Search service", "What a search service indexes and how it refreshes.")
_ROUTINE = Group("routine", "Procedure or function", "The routine's language, body, signature and access.")

KINDS = {
    "agent": KindUi(
        "Agent",
        "Agents",
        (_IDENTITY, _INSTRUCTIONS, _TOOLS, _SKILLS, _EVALUATION, _SETTINGS, _ADVANCED),
        description="comment",
        owner="meta.owner",
        key_settings=("spec.tools", "spec.skills", "evals.dataset"),
        flatten=("spec",),
    ),
    "cowork_profile": KindUi(
        "CoWork profile",
        "CoWork profiles",
        (_IDENTITY, _INSTRUCTIONS, _ROUTING, _TOOLS, _SKILLS, _EVALUATION, _SETTINGS, _ADVANCED),
        description="comment",
        owner="meta.owner",
        key_settings=("spec.subagents",),
        flatten=("spec",),
    ),
    "tool": KindUi(
        "Tool",
        "Tools",
        (_OBJECT, _SEARCH, _ROUTINE, _SETTINGS, _ADVANCED),
        description="description",
        owner="meta.owner",
        key_settings=("type", "on"),
    ),
}

_agent_tags, _agent_tag_lists = tags("agent.config.tags")
_profile_tags, _profile_tag_lists = tags("cowork_profile.config.tags")
_agent_skills, _agent_skill_lists = skills("agent.spec.skills")
_profile_skills, _profile_skill_lists = skills("cowork_profile.spec.skills")
_agent_instructions, _agent_instruction_lists = instructions("agent.spec.instructions")
_profile_instructions, _profile_instruction_lists = instructions("cowork_profile.spec.instructions")

_TOOL = "agent.spec.tools"
_SUBAGENTS = "cowork_profile.spec.subagents"

FIELDS: dict[str, Ui] = {
    "agent.name": Ui("Name", group="identity"),
    "agent.comment": Ui("Description", group="identity", widget=Widget.PROSE),
    **display("agent.profile"),
    "agent.spec": Ui("Specification"),
    **_agent_instructions,
    _TOOL: Ui("Tools", group="tools"),
    f"{_TOOL}.type": Ui("Type"),
    f"{_TOOL}.name": Ui("Name"),
    f"{_TOOL}.description": Ui("When to use it", widget=Widget.PROSE),
    f"{_TOOL}.tool_spec_passthrough": Ui("Extra tool spec keys", advanced=True, widget=Widget.CODE),
    f"{_TOOL}.columns_and_descriptions": retired("Columns and descriptions"),
    f"{_TOOL}.warehouse": Ui("Warehouse", advanced=True),
    f"{_TOOL}.query_timeout": Ui("Query timeout (seconds)", advanced=True),
    f"{_TOOL}.semantic_view": Ui("Semantic view", widget=Widget.REFERENCE),
    f"{_TOOL}.search_service": Ui("Search service", widget=Widget.REFERENCE),
    f"{_TOOL}.max_results": Ui("Most results"),
    f"{_TOOL}.title_column": Ui("Title column", widget=Widget.REFERENCE),
    f"{_TOOL}.id_column": Ui("ID column", widget=Widget.REFERENCE),
    f"{_TOOL}.stage_path": Ui("Preview stage", widget=Widget.CODE),
    f"{_TOOL}.relative_path_column": Ui("Path column", widget=Widget.REFERENCE),
    f"{_TOOL}.filter": Ui("Fixed filter", advanced=True, widget=Widget.CODE),
    f"{_TOOL}.columns": Ui("Column descriptions"),
    f"{_TOOL}.columns.column": Ui("Column", widget=Widget.REFERENCE),
    f"{_TOOL}.columns.description": Ui("Description", widget=Widget.PROSE),
    f"{_TOOL}.columns.type": Ui("Search type"),
    f"{_TOOL}.columns.searchable": Ui("Searchable"),
    f"{_TOOL}.columns.filterable": Ui("Filterable"),
    f"{_TOOL}.passthrough": Ui("Extra resource keys", advanced=True, widget=Widget.CODE),
    f"{_TOOL}.identifier": Ui("Procedure or function", widget=Widget.REFERENCE),
    f"{_TOOL}.input_schema": Ui("Input schema", widget=Widget.CODE),
    f"{_TOOL}.agent": Ui("Agent", widget=Widget.REFERENCE),
    **_agent_skills,
    "agent.spec.passthrough": Ui("Extra specification keys", group="advanced", widget=Widget.CODE),
    "agent.spec.models": retired("Models"),
    "agent.spec.orchestration": retired("Orchestration"),
    "agent.config": Ui("Settings", group="settings"),
    **_agent_tags,
    "agent.deprecated": Ui("Retired", group="settings"),
    "agent.meta": Ui("Metadata", group="advanced", widget=Widget.CODE),
    **evals("agent.evals"),
    "agent.alias": retired("Alias"),
    "agent.secure": retired("Secure"),
    "agent.enabled": retired("Enabled"),
    "agent.tags": retired("Tags"),
    "cowork_profile.name": Ui("Name", group="identity"),
    "cowork_profile.comment": Ui("Description", group="identity", widget=Widget.PROSE),
    **display("cowork_profile.display"),
    "cowork_profile.spec": Ui("Specification"),
    **_profile_instructions,
    _SUBAGENTS: Ui("Subagents", group="routing"),
    f"{_SUBAGENTS}.agent": Ui("Agent", widget=Widget.REFERENCE),
    f"{_SUBAGENTS}.display_name": Ui("Display name"),
    f"{_SUBAGENTS}.description": Ui("When to route to it", widget=Widget.PROSE),
    f"{_SUBAGENTS}.sample_questions": Ui("Sample questions"),
    f"{_SUBAGENTS}.sample_questions.question": Ui("Question", widget=Widget.PROSE),
    "cowork_profile.spec.tools": Ui("Built-in tools", group="tools"),
    "cowork_profile.spec.tools.type": Ui("Tool"),
    "cowork_profile.spec.tools.name": Ui("Name"),
    **_profile_skills,
    "cowork_profile.spec.passthrough": Ui("Extra specification keys", group="advanced", widget=Widget.CODE),
    "cowork_profile.config": Ui("Settings", group="settings"),
    **_profile_tags,
    "cowork_profile.deprecated": Ui("Retired", group="settings"),
    "cowork_profile.meta": Ui("Metadata", group="advanced", widget=Widget.CODE),
    **evals("cowork_profile.evals"),
    "tool.name": Ui("Name", group="object"),
    "tool.type": Ui("Type", group="object"),
    "tool.description": Ui("Description", group="object", widget=Widget.PROSE),
    "tool.on": Ui("Reads from", group="search"),
    "tool.on.table": Ui("Model", widget=Widget.REFERENCE),
    "tool.on.search_column": Ui("Search column", widget=Widget.REFERENCE),
    "tool.on.attributes": Ui("Attribute columns", widget=Widget.REFERENCE),
    "tool.where": Ui("Only rows where", group="search", widget=Widget.SQL),
    "tool.attribute_columns": Ui("Attribute columns", group="search", widget=Widget.REFERENCE),
    "tool.refresh_mode": Ui("Refresh mode", group="search", advanced=True),
    "tool.language": Ui("Language", group="routine"),
    "tool.runtime_version": Ui("Runtime version", group="routine"),
    "tool.handler": Ui("Handler", group="routine"),
    "tool.body": Ui("Body", group="routine", widget=Widget.REFERENCE),
    "tool.returns": Ui("Returns", group="routine", widget=Widget.CODE),
    "tool.packages": Ui("Packages", group="routine", advanced=True),
    "tool.imports": Ui("Imports", group="routine", advanced=True, widget=Widget.REFERENCE),
    "tool.external_access_integrations": Ui("External access integrations", group="routine", advanced=True),
    "tool.secrets": Ui("Secrets", group="routine", advanced=True),
    "tool.search_column": Ui("Search column", group="search", widget=Widget.REFERENCE),
    "tool.title_column": Ui("Title column", group="search", widget=Widget.REFERENCE),
    "tool.id_column": Ui("ID column", group="search", widget=Widget.REFERENCE),
    "tool.columns": Ui("Columns", group="search"),
    "tool.columns.column": Ui("Column", widget=Widget.REFERENCE),
    "tool.columns.description": Ui("Description", widget=Widget.PROSE),
    "tool.columns.type": Ui("Search type"),
    "tool.columns.searchable": Ui("Searchable"),
    "tool.columns.filterable": Ui("Filterable"),
    "tool.relative_path_column": Ui("Path column", group="search", widget=Widget.REFERENCE),
    "tool.signature": Ui("Parameters", group="routine"),
    "tool.signature.name": Ui("Name"),
    "tool.signature.type": Ui("Type", widget=Widget.CODE),
    "tool.signature.required": retired("Required"),
    "tool.relations": Ui("Existing object per target", group="object"),
    "tool.meta": Ui("Metadata", group="advanced", widget=Widget.CODE),
    "tool.config": Ui("Settings", group="settings"),
    "tool.warehouse": retired("Warehouse"),
    "tool.target_lag": retired("Target lag"),
    "tool.embedding_model": retired("Embedding model"),
    "tool.execute_as": retired("Execute as"),
    "tool.immutable": retired("Immutable"),
    "tool.owner": retired("Owner"),
    "tool.body_file": retired("Body file"),
    "tool.columns_and_descriptions": retired("Columns and descriptions"),
    "tool.group": retired("Group"),
    "tool.define": retired("Define"),
    "tool.reference": retired("Reference"),
}

_COLUMN_LIST = ListUi(
    "column", "column", ListLayout.TABLE, subtitle="description", columns=("type", "searchable", "filterable")
)

LISTS: dict[str, ListUi] = {
    **_agent_instruction_lists,
    **_profile_instruction_lists,
    _TOOL: ListUi("tool", "name", ListLayout.CARDS, subtitle="description", columns=("type",)),
    f"{_TOOL}.columns": _COLUMN_LIST,
    **_agent_skill_lists,
    **_profile_skill_lists,
    **_agent_tag_lists,
    **_profile_tag_lists,
    _SUBAGENTS: ListUi("subagent", "agent", ListLayout.CARDS, subtitle="description"),
    f"{_SUBAGENTS}.sample_questions": ListUi("sample question", "question", ListLayout.CARDS),
    "cowork_profile.spec.tools": ListUi("built-in tool", "type", ListLayout.TABLE, columns=("name",)),
    "tool.columns": _COLUMN_LIST,
    "tool.signature": ListUi("parameter", "name", ListLayout.TABLE, columns=("type",)),
}
