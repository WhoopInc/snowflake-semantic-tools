"""Presentation of skills, plugins, Cortex Code profiles, evals, and dbt model and column meta."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.presentation.common import retired, threshold
from snowflake_semantic_tools.domain.model.presentation.types import Group, KindUi, ListLayout, ListUi, Ui, Widget

_ABOUT = Group("about", "About", "What it is called and what it is for.")
_CONTENTS = Group("contents", "Contents", "What it bundles or pins.")
_SETTINGS = Group("settings", "Settings", "How it publishes, overriding its folder's settings.")
_ADVANCED = Group("advanced", "Advanced", "Metadata and keys other tools read.")
_JUDGE = Group("judge", "Judge", "The model that scores, and the rubric it follows.")
_SCORING = Group("scoring", "Scoring", "Score bands and when a score passes.")
_QUESTIONS = Group("questions", "Questions", "What is asked, and what each answer should do.")
_DATASET = Group("dataset", "Dataset", "The Snowflake dataset the eval runs on.")
_METRICS = Group("metrics", "Metrics", "What the eval scores, and what gates it.")
_RUN = Group("run", "Run", "How a run is named and when it is finished.")
_TABLE = Group("table", "Table", "Keys, synonyms and ranges of the model's table.")
_COLUMN = Group("column", "Column", "How the column renders in the views that hold it.")

KINDS = {
    "skill": KindUi(
        "Skill", "Skills", (_ABOUT, _SETTINGS, _ADVANCED), description="description", key_settings=("category",)
    ),
    "plugin": KindUi(
        "Plugin",
        "Plugins",
        (_ABOUT, _CONTENTS, _SETTINGS),
        description="description",
        owner="owner_team",
        key_settings=("skills",),
    ),
    "coco_profile": KindUi(
        "CoCo profile",
        "CoCo profiles",
        (_ABOUT, _CONTENTS, _SETTINGS),
        description="description",
        key_settings=("model", "skills", "plugins"),
    ),
    "coco_shared_profile": KindUi("Shared CoCo layer", "Shared CoCo layers", (_CONTENTS,), key_settings=("skills",)),
    "eval_metric": KindUi(
        "Eval metric",
        "Eval metrics",
        (_ABOUT, _JUDGE, _SCORING, _ADVANCED),
        description="description",
        key_settings=("model", "gate_default"),
    ),
    "eval_dataset": KindUi(
        "Eval dataset",
        "Eval datasets",
        (_ABOUT, _QUESTIONS),
        description="description",
        key_settings=("questions",),
    ),
    "eval_config": KindUi(
        "Eval config",
        "Eval configs",
        (_ABOUT, _DATASET, _METRICS, _RUN, _SETTINGS),
        key_settings=("dataset.name_template", "metrics.system"),
    ),
    "dbt_model_meta": KindUi("Model table", "Model tables", (_TABLE,), key_settings=("primary_key",)),
    "dbt_column_meta": KindUi("Model column", "Model columns", (_COLUMN,), key_settings=("column_type",)),
}

_GROUND = "eval_dataset.questions.ground_truth"
_CALLS = f"{_GROUND}.ground_truth_invocations"

FIELDS: dict[str, Ui] = {
    "skill.name": Ui("Name", group="about"),
    "skill.description": Ui("When to use it", group="about", widget=Widget.PROSE),
    "skill.config": Ui("Settings", group="settings"),
    "skill.tools": Ui("Expected tools", group="advanced"),
    "skill.parent_skill": Ui("Parent skill", group="advanced"),
    "skill.category": Ui("Category", group="about"),
    "skill.categories": Ui("Categories", group="advanced"),
    "skill.triggers": Ui("Trigger phrases", group="advanced"),
    "skill.license": Ui("License", group="advanced"),
    "skill.allowed-tools": Ui("Allowed tools", group="advanced"),
    "skill.compatibility": Ui("Compatibility", group="advanced"),
    "skill.metadata": Ui("Metadata", group="advanced", widget=Widget.CODE),
    "plugin.name": Ui("Name", group="about"),
    "plugin.description": Ui("When to use it", group="about", widget=Widget.PROSE),
    "plugin.owner_team": Ui("Owning team", group="about"),
    "plugin.skills": Ui("Skills", group="contents", widget=Widget.REFERENCE),
    "plugin.config": Ui("Settings", group="settings"),
    "coco_profile.name": Ui("Name", group="about"),
    "coco_profile.description": Ui("Description", group="about", widget=Widget.PROSE),
    "coco_profile.model": Ui("Model", group="contents"),
    "coco_profile.skills": Ui("Skills", group="contents", widget=Widget.REFERENCE),
    "coco_profile.plugins": Ui("Plugins", group="contents", widget=Widget.REFERENCE),
    "coco_profile.config": Ui("Settings", group="settings"),
    "coco_profile.owner_team": retired("Owning team"),
    "coco_profile.hooks": retired("Hooks"),
    "coco_profile.mcp_servers": retired("MCP servers"),
    "coco_profile.commands": retired("Commands"),
    "coco_profile.allowed_roles": retired("Allowed roles"),
    "coco_shared_profile.skills": Ui("Skills every profile pins", group="contents", widget=Widget.REFERENCE),
    "eval_metric.name": Ui("Name", group="about"),
    "eval_metric.description": Ui("What it measures", group="about", widget=Widget.PROSE),
    "eval_metric.model": Ui("Judge model", group="judge"),
    "eval_metric.score_ranges": Ui("Score bands", group="scoring"),
    "eval_metric.score_ranges.min_score": Ui("Low band"),
    "eval_metric.score_ranges.median_score": Ui("Middle band"),
    "eval_metric.score_ranges.max_score": Ui("High band"),
    "eval_metric.prompt": Ui("Rubric", group="judge", widget=Widget.MARKDOWN),
    "eval_metric.gate_default": Ui("Gates by default", group="scoring"),
    **{
        path: Ui(ui.label, group="scoring") if path == "eval_metric.threshold_default" else ui
        for path, ui in threshold("eval_metric.threshold_default", "Default pass band").items()
    },
    "eval_metric.enabled": Ui("Enabled", group="about"),
    "eval_metric.meta": Ui("Metadata", group="advanced", widget=Widget.CODE),
    "eval_dataset.name": Ui("Name", group="about"),
    "eval_dataset.description": Ui("What it covers", group="about", widget=Widget.PROSE),
    "eval_dataset.questions": Ui("Questions", group="questions"),
    "eval_dataset.questions.question": Ui("Question", widget=Widget.PROSE),
    _GROUND: Ui("Expected answer"),
    _CALLS: Ui("Expected tool calls"),
    f"{_CALLS}.tool_name": Ui("Tool"),
    f"{_CALLS}.tool_input": Ui("Input", widget=Widget.CODE),
    f"{_CALLS}.tool_output": Ui("Output", widget=Widget.CODE),
    f"{_GROUND}.ground_truth_output": Ui("Expected answer", widget=Widget.PROSE),
    f"{_GROUND}.required_filters": Ui("Required filters"),
    f"{_GROUND}.immutable": Ui("Never changes"),
    f"{_GROUND}.immutable_reason": Ui("Why it never changes", widget=Widget.PROSE),
    f"{_GROUND}.ground_truth_subagents": Ui("Expected subagents", widget=Widget.REFERENCE),
    "eval_dataset.database": retired("Database"),
    "eval_dataset.schema": retired("Schema"),
    "eval_dataset.enabled": retired("Enabled"),
    "eval_dataset.agent": retired("Agent"),
    "eval_config.name": Ui("Name", group="about"),
    "eval_config.config": Ui("Settings", group="settings"),
    "eval_config.dataset": Ui("Dataset", group="dataset"),
    "eval_config.dataset.mint": Ui("Create the dataset"),
    "eval_config.dataset.name_template": Ui("Dataset name"),
    "eval_config.dataset.source_table_template": Ui("Source table"),
    "eval_config.dataset.column_mapping": Ui("Columns"),
    "eval_config.dataset.column_mapping.query_text": Ui("Question column"),
    "eval_config.dataset.column_mapping.ground_truth": Ui("Expected answer column"),
    "eval_config.metrics": Ui("Metrics", group="metrics"),
    "eval_config.metrics.system": Ui("Snowflake metrics"),
    "eval_config.metrics.system.name": Ui("Metric"),
    "eval_config.metrics.system.version": Ui("Version"),
    "eval_config.metrics.system.gate": Ui("Gates the eval"),
    **threshold("eval_config.metrics.system.threshold"),
    "eval_config.metrics.system.judge_model": retired("Judge model"),
    "eval_config.metrics.custom": Ui("Custom metrics", widget=Widget.REFERENCE),
    "eval_config.metrics.routing": Ui("Routing metrics"),
    "eval_config.metrics.routing.name": Ui("Metric"),
    "eval_config.metrics.routing.gate": Ui("Gates the eval"),
    **threshold("eval_config.metrics.routing.threshold"),
    "eval_config.run": Ui("Run", group="run"),
    "eval_config.run.name_template": Ui("Run name"),
    "eval_config.run.label": Ui("Label"),
    "eval_config.run.description": Ui("What it checks", widget=Widget.PROSE),
    "eval_config.run.variant": Ui("Agent variant"),
    "eval_config.run.accept_statuses": Ui("Finished statuses"),
    "eval_config.run.tier": retired("Tier"),
    "eval_config.run.retry": retired("Retry"),
    "eval_config.run.concurrency": retired("Concurrency"),
    "eval_config.run.baseline_runs": retired("Baseline runs"),
    "eval_config.run.retention": retired("Retention"),
    "eval_config.sweep": retired("Sweep"),
    "eval_config.agent_version": retired("Agent version"),
    "eval_config.database": retired("Database"),
    "eval_config.schema": retired("Schema"),
    "eval_config.enabled": retired("Enabled"),
    "eval_config.agent": retired("Agent"),
    "dbt_model_meta.primary_key": Ui("Primary key", group="table", widget=Widget.REFERENCE),
    "dbt_model_meta.unique_keys": Ui("Unique keys", group="table"),
    "dbt_model_meta.synonyms": Ui("Synonyms", group="table"),
    "dbt_model_meta.distinct_range": Ui("Distinct range", group="table", advanced=True),
    "dbt_model_meta.database": retired("Database"),
    "dbt_model_meta.schema": retired("Schema"),
    "dbt_column_meta.column_type": Ui("Fact or dimension", group="column"),
    "dbt_column_meta.data_type": Ui("Data type", group="column", advanced=True),
    "dbt_column_meta.synonyms": Ui("Synonyms", group="column"),
    "dbt_column_meta.sample_values": Ui("Sample values", group="column"),
    "dbt_column_meta.is_enum": Ui("Sample values are complete", group="column"),
    "dbt_column_meta.exclude": Ui("Left out of every view", group="column"),
    "dbt_column_meta.access_modifier": Ui("Access", group="column", advanced=True),
    "dbt_column_meta.governance_tags": Ui("Sensitive tags", group="column", advanced=True),
}

_GATED = ListUi("metric", "name", ListLayout.TABLE, columns=("gate", "threshold.min", "threshold.max"))

LISTS: dict[str, ListUi] = {
    "eval_dataset.questions": ListUi(
        "question", "question", ListLayout.CARDS, subtitle="ground_truth.ground_truth_output"
    ),
    _CALLS: ListUi("expected call", "tool_name", ListLayout.TABLE),
    "eval_config.metrics.system": _GATED,
    "eval_config.metrics.routing": _GATED,
}
