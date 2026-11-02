"""Presentation of the semantic kinds: views and the members they attach."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.presentation.common import ordering, retired, tags
from snowflake_semantic_tools.domain.model.presentation.types import Group, KindUi, ListLayout, ListUi, Ui, Widget

_ABOUT = Group("about", "About", "What it is called and what it is for.")
_DEFINITION = Group("definition", "Definition", "What it computes or joins, and from which models.")
_SHAPE = Group("shape", "Columns and members", "Which of its tables' columns and members it keeps.")
_SETTINGS = Group("settings", "Settings", "How it publishes, overriding its folder's settings.")
_BEHAVIOUR = Group("behaviour", "Behaviour", "How it aggregates, windows and is exposed.")
_GUIDANCE = Group("guidance", "Guidance", "What it tells Cortex Analyst.")

KINDS = {
    "semantic_view": KindUi(
        "Semantic view",
        "Semantic views",
        (_ABOUT, _DEFINITION, _SHAPE, _SETTINGS),
        description="description",
        key_settings=("tables", "custom_instructions"),
    ),
    "metric": KindUi(
        "Metric",
        "Metrics",
        (_ABOUT, _DEFINITION, _BEHAVIOUR),
        description="description",
        key_settings=("expr", "access_modifier"),
    ),
    "filter": KindUi("Filter", "Filters", (_ABOUT, _DEFINITION), description="description", key_settings=("expr",)),
    "custom_instruction": KindUi(
        "Custom instruction", "Custom instructions", (_ABOUT, _GUIDANCE), description="description"
    ),
    "verified_query": KindUi(
        "Verified query",
        "Verified queries",
        (_ABOUT, _DEFINITION),
        description="description",
        key_settings=("question", "verified_by"),
    ),
    "relationship": KindUi(
        "Relationship",
        "Relationships",
        (_ABOUT, _DEFINITION),
        description="description",
        key_settings=("left_table", "right_table"),
    ),
}

_view_tags, _view_tag_lists = tags("semantic_view.config.tags")

FIELDS: dict[str, Ui] = {
    "semantic_view.name": Ui("Name", group="about"),
    "semantic_view.description": Ui("Description", group="about", widget=Widget.PROSE),
    "semantic_view.tables": Ui("Tables", group="definition", widget=Widget.REFERENCE),
    "semantic_view.custom_instructions": Ui("Custom instructions", group="definition", widget=Widget.REFERENCE),
    "semantic_view.variables": Ui("Variables", group="definition", advanced=True),
    "semantic_view.variables.name": Ui("Name"),
    "semantic_view.variables.data_type": Ui("Data type"),
    "semantic_view.variables.default_value": Ui("Default value"),
    "semantic_view.variables.description": Ui("Description", widget=Widget.PROSE),
    "semantic_view.config": Ui("Settings", group="settings"),
    **_view_tags,
    "semantic_view.columns": Ui("Keep only these columns", group="shape", widget=Widget.REFERENCE),
    "semantic_view.exclude_columns": Ui("Drop these columns", group="shape", widget=Widget.REFERENCE),
    "semantic_view.metrics": Ui("Keep only these metrics", group="shape", widget=Widget.REFERENCE),
    "semantic_view.exclude_metrics": Ui("Drop these metrics", group="shape", widget=Widget.REFERENCE),
    "semantic_view.relationships": Ui("Keep only these relationships", group="shape", widget=Widget.REFERENCE),
    "semantic_view.exclude_relationships": Ui("Drop these relationships", group="shape", widget=Widget.REFERENCE),
    "semantic_view.table_config": retired("Table settings"),
    "semantic_view.enabled": retired("Enabled"),
    "semantic_view.tags": retired("Tags"),
    "semantic_view.max_staleness": retired("Maximum staleness"),
    "metric.name": Ui("Name", group="about"),
    "metric.expr": Ui("Expression", group="definition", widget=Widget.SQL),
    "metric.description": Ui("Description", group="about", widget=Widget.PROSE),
    "metric.synonyms": Ui("Synonyms", group="about"),
    "metric.using_relationships": Ui("Join through", group="definition", widget=Widget.REFERENCE, advanced=True),
    "metric.non_additive_dimensions": Ui("Semi-additive over", group="behaviour", advanced=True),
    "metric.non_additive_dimensions.ref": Ui("Dimension", widget=Widget.REFERENCE),
    **ordering("metric.non_additive_dimensions"),
    "metric.non_additive_dimensions.order": retired("Order"),
    "metric.non_additive_dimensions.nulls": retired("Nulls"),
    "metric.non_additive_dimensions.dimension": retired("Dimension"),
    "metric.non_additive_dimensions.table": retired("Table"),
    "metric.access_modifier": Ui("Access", group="behaviour"),
    "metric.window": Ui("Window", group="behaviour", advanced=True),
    "metric.window.partition_by": Ui("Partition by", widget=Widget.REFERENCE),
    "metric.window.partition_by_excluding": Ui("Partition by all but", widget=Widget.REFERENCE),
    "metric.window.order_by": Ui("Order by"),
    "metric.window.order_by.ref": Ui("Column or metric", widget=Widget.REFERENCE),
    **ordering("metric.window.order_by"),
    "metric.window.order_by.column": retired("Column"),
    "metric.window.order_by.direction": retired("Direction"),
    "metric.window.frame": Ui("Frame", widget=Widget.CODE),
    "metric.visibility": retired("Visibility"),
    "metric.non_additive_by": retired("Non-additive by"),
    "metric.tables": retired("Tables"),
    "metric.derived": retired("Derived"),
    "filter.name": Ui("Name", group="about"),
    "filter.expr": Ui("Condition", group="definition", widget=Widget.SQL),
    "filter.description": Ui("Description", group="about", widget=Widget.PROSE),
    "filter.labels": Ui("Labels", group="definition"),
    "filter.tables": retired("Tables"),
    "custom_instruction.name": Ui("Name", group="about"),
    "custom_instruction.description": Ui("Notes", group="about", widget=Widget.PROSE),
    "custom_instruction.ai_sql_generation": Ui("SQL guidance", group="guidance", widget=Widget.MARKDOWN),
    "custom_instruction.ai_question_categorization": Ui("Question guidance", group="guidance", widget=Widget.MARKDOWN),
    "custom_instruction.sql_generation": retired("SQL guidance (0.3)"),
    "custom_instruction.question_categorization": retired("Question guidance (0.3)"),
    "verified_query.name": Ui("Name", group="about"),
    "verified_query.description": Ui("Notes", group="about", widget=Widget.PROSE),
    "verified_query.question": Ui("Question", group="definition", widget=Widget.PROSE),
    "verified_query.sql": Ui("SQL", group="definition", widget=Widget.SQL),
    "verified_query.verified_at": Ui("Verified on", group="about"),
    "verified_query.verified_by": Ui("Verified by", group="about"),
    "verified_query.use_as_onboarding_question": Ui("Onboarding question", group="about"),
    "verified_query.tables": retired("Tables"),
    "verified_query.sql_file": retired("SQL file"),
    "relationship.name": Ui("Name", group="about"),
    "relationship.description": Ui("Notes", group="about", widget=Widget.PROSE),
    "relationship.left_table": Ui("Many side", group="definition", widget=Widget.REFERENCE),
    "relationship.right_table": Ui("One side", group="definition", widget=Widget.REFERENCE),
    "relationship.relationship_conditions": Ui("Join on", group="definition", widget=Widget.SQL),
    "relationship.relationship_type": retired("Relationship type"),
    "relationship.join_type": retired("Join type"),
    "relationship.relationship_columns": retired("Columns"),
    "relationship.left_column": retired("Left column"),
    "relationship.right_column": retired("Right column"),
}

LISTS: dict[str, ListUi] = {
    "semantic_view.variables": ListUi(
        "variable", "name", ListLayout.TABLE, subtitle="description", columns=("data_type", "default_value")
    ),
    **_view_tag_lists,
    "metric.non_additive_dimensions": ListUi(
        "dimension", "ref", ListLayout.TABLE, columns=("sort_direction", "null_order")
    ),
    "metric.window.order_by": ListUi("sort column", "ref", ListLayout.TABLE, columns=("sort_direction", "null_order")),
}
