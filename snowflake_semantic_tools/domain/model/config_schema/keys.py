"""The key table of `sst_config.yml`: every key, block, and wildcard slot SST reads.

One table serves two consumers. Validation (`validate.py`) turns an unknown, removed,
mistyped, or unsupported key into a diagnostic instead of a silent no-op, and the
generated configuration reference renders the same rows, so the documentation cannot
drift from what the engine accepts. Row order is the reference page's order.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.skill import DEFAULT_VERSION_PREFIX

CONFIG_FILE = "sst_config.yml"
# The two spellings discovery accepts in the project root; both present is an error, not a precedence.
CONFIG_NAMES: tuple[str, ...] = (CONFIG_FILE, "sst_config.yaml")


class KeyKind(Enum):
    """The type of value a key takes.

    `MAP` is a block of free-form names, `ENUM` a string from the key's `choices`, and
    `ANY` accepts every value unchecked. An `INTEGER` key never accepts a boolean.
    """

    BLOCK = "block"
    MAP = "map"
    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    LIST = "list"
    ENUM = "enum"
    ANY = "any"


class KeyStatus(Enum):
    """Whether SST reads a key, reserves it, reads it under its new name, or no longer reads it."""

    CURRENT = "current"
    # Read as its replacement, with a warning naming it.
    DEPRECATED = "deprecated"
    # Reserved for a later release: setting it is an error until SST reads it.
    UNSUPPORTED = "unsupported"
    REMOVED = "removed"


class ChildPolicy(Enum):
    """Which undeclared children a block accepts.

    `DECLARED` accepts none, `NAMES` matches any other child to the block's `<name>`
    slot, and `ROUTES` matches any other child not starting with `+` to its `<route>`
    slot.
    """

    DECLARED = "declared"
    NAMES = "names"
    ROUTES = "routes"


@dataclass(frozen=True, slots=True)
class ConfigKey:
    """One key, block, or wildcard slot of the configuration file.

    `<name>` stands for any free-form entry of a name map and `<route>` for any
    unprefixed folder route or group key inside a routed block.

    Attributes:
        summary: The reference-page description; for a removed key, why it was removed.
        default: The default as the reference prints it, or None when the key has none.
        required: A block that is present must set this key; an empty value is unset.
        minimum: Inclusive lower bound of an integer value, or None for no bound.
        maximum: Inclusive upper bound of an integer value, or None for no bound.
        replacement: For a removed key, the reason its diagnostic gives; for a deprecated key,
            the key it is read as.
        code: The code reported instead of the generic one: a removed key's dedicated
            code, or the code for a value outside `choices` or not `fixed`.
        fixed: The only boolean the key accepts, or None when either is accepted.
        one_of: Children of which a present block must declare at least one.
    """

    path: str
    kind: KeyKind
    summary: str
    status: KeyStatus = KeyStatus.CURRENT
    default: str | None = None
    choices: tuple[str, ...] = ()
    required: bool = False
    minimum: int | None = None
    maximum: int | None = None
    children: ChildPolicy = ChildPolicy.DECLARED
    replacement: str | None = None
    code: str | None = None
    fixed: bool | None = None
    one_of: tuple[str, ...] = ()

    @property
    def segments(self) -> tuple[str, ...]:
        """Return the dotted path split into its segments."""
        return tuple(self.path.split("."))

    @property
    def parent(self) -> str:
        """Return the path of the enclosing block; `""` for a top-level key."""
        return ".".join(self.segments[:-1])

    @property
    def name(self) -> str:
        """Return the last path segment: the key as it is written in its block."""
        return self.segments[-1]


_key = ConfigKey


def _removed(path: str, reason: str, *, code: str | None = None) -> ConfigKey:
    return ConfigKey(path, KeyKind.ANY, reason, status=KeyStatus.REMOVED, replacement=reason, code=code)


def _deprecated(path: str, replacement: str) -> ConfigKey:
    return ConfigKey(
        path,
        KeyKind.BLOCK,
        f"Deprecated spelling of `{replacement}:`, read as it.",
        status=KeyStatus.DEPRECATED,
        replacement=replacement,
    )


def _unsupported(path: str, kind: KeyKind, summary: str) -> ConfigKey:
    return ConfigKey(path, kind, summary, status=KeyStatus.UNSUPPORTED)


_S = KeyKind.STRING
_B = KeyKind.BOOLEAN
_I = KeyKind.INTEGER
_L = KeyKind.LIST
_BLOCK = KeyKind.BLOCK

CONFIG_SCHEMA: tuple[ConfigKey, ...] = (
    _key("project", _BLOCK, "Where each artifact type lives on disk."),
    _key("project.semantic_models_dir", _S, "Semantic view YAML. dbt projects only.", default="semantic_models"),
    _key("project.agents_dir", _S, "Agent specs; per-agent evals nest under each agent.", default="agents"),
    _key("project.eval_metrics_dir", _S, "Shared LLM-judge metrics for evals.", default="eval_metrics"),
    _key("project.tools_dir", _S, "Backing objects for agent tools.", default="tools"),
    _key(
        "project.skills_dir",
        _S,
        "`SKILL.md` folders, published as Cortex Extensions and inside Desktop profiles.",
        default="skills",
    ),
    _key("project.plugins_dir", _S, "Plugin manifests, each grouping skills into one extension.", default="plugins"),
    _key("project.profiles_dir", _S, "CoCo Desktop profiles plus the shared prompt and rules.", default="profiles"),
    _key("project.hooks_dir", _S, "Hook definitions and their scripts, referenced by profiles.", default="hooks"),
    _key("project.mcp_servers_dir", _S, "MCP server configs, referenced by profiles.", default="mcp-servers"),
    _key(
        "project.commands_dir",
        _S,
        "Desktop slash commands (`*.md`, nested folders allowed), referenced by profiles.",
        default="commands",
    ),
    _key(
        "project.target_profile",
        _S,
        "The `profiles.yml` profile of a project with no `dbt_project.yml`. In a dbt project it must match "
        "the dbt `profile:`.",
    ),
    _key("validation", _BLOCK, "What blocks a build."),
    _key("validation.strict", _B, "Promote every warning to an error.", default="false"),
    _key(
        "validation.snowflake_syntax_check",
        _B,
        "Compile expressions against Snowflake during validate and plan.",
        default="true",
    ),
    _key(
        "validation.description_floor",
        _I,
        "Shortest description, in characters, a semantic view or metric may carry; unset checks none.",
        minimum=1,
    ),
    _key(
        "validation.instruction_budget",
        _I,
        "Longest composed comment and instructions, in characters, a semantic view may publish; unset checks none.",
        minimum=1,
    ),
    _removed(
        "validation.exclude_dirs",
        "every file under the configured directories is read; set enabled: false on a view to skip it",
    ),
    _removed("validation.expression_rules", "the expression rules it disabled are no longer optional"),
    _removed("validation.multipath_check", "multi-path relationship analysis is always on"),
    _removed("validation.smoke_query", "smoke probes run only under sst test --suite smoke"),
    _key("diagnostics", _BLOCK, "How severe a diagnostic is, beyond what the error registry declares."),
    _key(
        "diagnostics.severity_overrides",
        KeyKind.MAP,
        "Per-code severity: promote freely; demote an error no lower than warning, and never a "
        "non-demotable code (SST-CFG033).",
        children=ChildPolicy.NAMES,
    ),
    _key(
        "diagnostics.severity_overrides.<name>",
        KeyKind.ENUM,
        "The severity this code reports at.",
        choices=("error", "warning", "info"),
    ),
    _key("enrichment", _BLOCK, "What `sst enrich` collects from the warehouse, and how much."),
    _key(
        "enrichment.distinct_limit",
        _I,
        "Distinct values sampled per column. A column with no more than this many is an enum.",
        default="25",
        minimum=1,
        maximum=1000,
    ),
    _key(
        "enrichment.sample_values_display_limit",
        _I,
        "Sample values written for a column that is not an enum. At most `distinct_limit`.",
        default="10",
        minimum=1,
        maximum=1000,
    ),
    _key("enrichment.synonym_model", _S, "Cortex model that writes synonyms.", default="mistral-large2"),
    _key(
        "enrichment.synonym_max_count",
        _I,
        "Synonyms written per column and per table.",
        default="4",
        minimum=1,
        maximum=20,
    ),
    _key(
        "enrichment.allow_sample_value_collection",
        _B,
        "False refuses every run that reads row data: `--include sample-values` and `enums` (SST-CFG038).",
        default="true",
    ),
    _key("generation", _BLOCK, "How the commands that reach Snowflake pace their work."),
    _key(
        "generation.threads",
        _I,
        "Sessions `plan`, `apply` and `test` work on at once, unless `--threads` or `$SST_THREADS` says.",
        default="1",
        minimum=1,
        maximum=16,
    ),
    _unsupported("generation.view_timeout", _I, "Per-view statement timeout, in seconds."),
    _removed("generation.publish_via", "SST 1.0 renders DDL directly"),
    _removed("generation.filters_to_instructions", "standalone filter prose always renders into the view"),
    _removed("generation.use_create_or_alter", "SST 1.0 renders DDL directly"),
    _removed("generation.emit_relationship_type", "relationship and join types are never rendered"),
    _unsupported("dbt", _BLOCK, "How SST invokes dbt."),
    _removed("defer", "SST reads the manifest dbt resolves; configure deferral in dbt"),
    _key(
        "vars",
        KeyKind.MAP,
        "Values read by `{{ var('<name>') }}`.",
        children=ChildPolicy.NAMES,
    ),
    _key("vars.<name>", KeyKind.ANY, "One variable value."),
    _removed("vars.sha_version", "SST supplies sha_version from the commit being published", code="SST-CFG040"),
    _key("state", _BLOCK, "Where apply records what it published, per target."),
    _key("state.+database", _S, "State table database.", default="the target database"),
    _key("state.+schema", _S, "State table schema.", default="the target schema"),
    _key("state.+table", _S, "State table name.", default="SST_STATE"),
    _key(
        "tags",
        _BLOCK,
        "Tag object names read by `{{ tag('<name>') }}`.",
        children=ChildPolicy.NAMES,
    ),
    _key("tags.default_prefix", _S, "`<database>.<schema>` for entries without `fqn:`."),
    _key("tags.<name>", _BLOCK, "One tag reference; empty takes `default_prefix`."),
    _key("tags.<name>.fqn", _S, "The full three-part tag name."),
    _key("tools", _BLOCK, "Defaults for tool backing objects.", children=ChildPolicy.ROUTES),
    _key("tools.+database", _S, "Database for defined tool objects.", default="the target database"),
    _key("tools.+schema", _S, "Schema for defined tool objects.", default="the target schema"),
    _key("tools.+warehouse", _S, "Warehouse for defined search services.", default="the target warehouse"),
    _key("tools.+target_lag", _S, "Target lag for defined search services."),
    _key("tools.+embedding_model", _S, "Embedding model for defined search services."),
    _key(
        "tools.+execute_as",
        KeyKind.ENUM,
        "Rights a generic tool runs with.",
        default="caller",
        choices=("caller", "owner"),
    ),
    _removed("tools.+enabled", "omit the tools instead"),
    _key("tools.<route>", _BLOCK, "Per-group override of the `+` keys above, for a group with `define:` members."),
    _key(
        "semantic_views",
        _BLOCK,
        "Defaults for semantic views, overridable per folder.",
        children=ChildPolicy.ROUTES,
    ),
    _key("semantic_views.+database", _S, "Database for semantic views.", default="the target database"),
    _key("semantic_views.+schema", _S, "Schema for semantic views.", default="the target schema"),
    _key(
        "semantic_views.+enabled",
        _B,
        "Default for views that do not set `enabled` themselves.",
        default="true",
    ),
    _unsupported("semantic_views.+tags", _L, "Default view tags."),
    _key(
        "semantic_views.+max_staleness",
        _I,
        "Default `max_staleness`, in seconds, for views that set none; at least 120 (SST-CFG023).",
        minimum=120,
        code="SST-CFG023",
    ),
    _removed("semantic_views.+meta", "put metadata on the view itself"),
    _key("semantic_views.<route>", _BLOCK, "Folder route: overrides for views under that directory."),
    _key("agents", _BLOCK, "Defaults for Cortex Agents.", children=ChildPolicy.ROUTES),
    _key("agents.+database", _S, "Database for agents.", default="the target database"),
    _key("agents.+schema", _S, "Schema for agents.", default="the target schema"),
    _key("agents.+warehouse", _S, "Warehouse for agent tool execution.", default="the target warehouse"),
    _key("agents.+orchestration_model", _S, "Orchestration model.", default="auto"),
    _key("agents.+query_timeout", _I, "Tool query timeout in seconds."),
    _key("agents.+budget_seconds", _I, "Orchestration time budget."),
    _key("agents.+budget_tokens", _I, "Orchestration token budget."),
    _key(
        "agents.+tool_not_accessible",
        KeyKind.ENUM,
        "Behavior when a tool is not accessible.",
        choices=("accept", "reject", "legacy"),
    ),
    _key("agents.+analytical_search", _B, "Enable analytical search."),
    _key("agents.+alias", _S, "Version alias assigned after publication."),
    _key("agents.+enabled", _B, "Default for agents that do not set `enabled` themselves.", default="true"),
    _unsupported("agents.+secure", _B, "Default agent security flag."),
    _unsupported("agents.+tags", _L, "Default agent tags."),
    _removed("agents.+copy_grants", "agents are never replaced, so there are no grants to copy"),
    _removed("agents.+meta", "put metadata on the agent itself"),
    _removed("agents.+create_mode", "agents are created once and versioned"),
    _unsupported("agents.<route>", _BLOCK, "Folder route."),
    _key("evals", _BLOCK, "Defaults for agent evaluations.", children=ChildPolicy.ROUTES),
    _key("evals.+eval_tier", KeyKind.ENUM, "Whether a regression blocks.", choices=("blocking", "report")),
    _key("evals.+metrics", _L, "Default system metrics."),
    _key("evals.+metric_version", _S, "System metric version."),
    _key("evals.+judge_model", _S, "Custom judge model."),
    _key("evals.+agent_version", _S, "Agent version an eval runs against."),
    _key("evals.+retry", _I, "Retries per run; every attempt is reported."),
    _key("evals.+concurrency", _I, "Concurrent eval runs."),
    _key("evals.+baseline_runs", _I, "Completed attempts a baseline needs."),
    _key("evals.+min_dataset_rows", _I, "Smallest accepted question set."),
    _key("evals.+retention", KeyKind.ENUM, "Run retention class.", choices=("audit", "decision")),
    _removed("evals.+enabled", "omit the evals instead"),
    _removed("evals.+database", "eval objects resolve to the agent's schema", code="SST-CFG015"),
    _removed("evals.+schema", "eval objects resolve to the agent's schema", code="SST-CFG015"),
    _removed("evals.<route>", "eval location is structural", code="SST-CFG042"),
    _key(
        "skills",
        _BLOCK,
        "Skill and plugin publishing. Omitting a channel block disables that channel.",
        children=ChildPolicy.ROUTES,
        one_of=("catalog", "stage"),
    ),
    _key(
        "skills.+certified",
        _B,
        "Tag each new skill and plugin version `SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'`.",
        default="false",
    ),
    _key(
        "skills.+version_prefix",
        _S,
        "Prefix of every version alias; the rest is 12 hex characters of the bundle digest.",
        default=DEFAULT_VERSION_PREFIX,
    ),
    _key(
        "skills.+threads",
        _I,
        "Concurrent catalog publishes. Profiles always publish one at a time.",
        default="4",
        minimum=1,
        maximum=16,
    ),
    _removed("skills.+grant_read_to", "access is managed outside SST; SST never issues grants"),
    _removed("skills.+enabled", "omit a channel block to disable it"),
    _removed("skills.<route>", "the unprefixed keys of skills: are its channel blocks", code="SST-CFG042"),
    _key(
        "skills.extensions",
        _BLOCK,
        "Name map for extensions this project consumes but does not publish.",
        children=ChildPolicy.NAMES,
    ),
    _key("skills.extensions.default_prefix", _S, "`<database>.<schema>` for entries without `fqn:`."),
    _key("skills.extensions.<name>", _BLOCK, "One consumed extension; empty takes `default_prefix`."),
    _key("skills.extensions.<name>.fqn", _S, "The full three-part extension name."),
    _key("skills.catalog", _BLOCK, "Publish each skill and plugin as a Cortex Extension."),
    _key("skills.catalog.+database", _S, "Database for extensions.", default="the target database"),
    _key("skills.catalog.+schema", _S, "Schema for extensions.", default="the target schema"),
    _key(
        "skills.catalog.+bundle_stage",
        _S,
        "Internal stage that `ADD VERSION` reads from. Created when absent.",
        required=True,
    ),
    _key(
        "skills.catalog.+flatten",
        _B,
        "Must be true: agents read supporting files beside `SKILL.md`.",
        default="true",
        fixed=True,
        code="SST-VAL818",
    ),
    _removed("skills.catalog.+registry_table", "SST state records every published version"),
    _removed("skills.catalog.+prune_deleted", "every version is built from a complete bundle"),
    _key("skills.stage", _BLOCK, "Publish CoCo Desktop profiles through a stage and the profile registry."),
    _key("skills.stage.+database", _S, "Database of the profile stage and registry.", default="the target database"),
    _key("skills.stage.+schema", _S, "Schema of the profile stage and registry.", default="the target schema"),
    _key("skills.stage.+stage", _S, "Internal stage that holds profile trees. Created when absent.", required=True),
    _key(
        "skills.stage.+registry_table",
        _S,
        "Profile registry table. Desktop reads `CORTEX_CODE.CONFIG.PROFILE_REGISTRY`.",
        default="PROFILE_REGISTRY",
    ),
    _key(
        "skills.stage.+flatten",
        _B,
        "Must be false: Desktop expects the authored nested layout.",
        default="false",
        fixed=False,
        code="SST-VAL818",
    ),
    _key(
        "skills.stage.+auto_compress",
        _B,
        "Must be false: compressed uploads are invisible to Desktop.",
        default="false",
        fixed=False,
        code="SST-VAL819",
    ),
    _key(
        "skills.stage.+layout",
        KeyKind.ENUM,
        "Stage layout: content-addressed trees under `skills/`, `prompts/`, `mcp/`, and `hooks/`.",
        default="by_type",
        choices=("by_type",),
        code="SST-VAL821",
    ),
    _key("apply", _BLOCK, "Stages apply uploads through."),
    _key("apply.agent_spec_stage", _BLOCK, "Stage for staged agent specs."),
    _key("apply.agent_spec_stage.database", _S, "Stage database.", default="the target database"),
    _key("apply.agent_spec_stage.schema", _S, "Stage schema.", default="the target schema"),
    _key("apply.agent_spec_stage.stage", _S, "Stage name.", default="AGENT_SPECS"),
    _key("apply.eval_config_stage", _BLOCK, "Stage for eval run configs, in each agent's schema."),
    _key("apply.eval_config_stage.stage", _S, "Stage name.", default=DEFAULT_EVAL_CONFIG_STAGE),
    _key(
        "apply.fail_fast",
        _B,
        "Stop at the first failure instead of continuing; `--fail-fast` and `--no-fail-fast` override it.",
        default="false",
    ),
    _deprecated("deploy", "apply"),
    _key("snowflake", _BLOCK, "Allowlists for Snowflake surfaces the renderer accepts."),
    _key("snowflake.orchestration_models", _L, "Orchestration models agents may name.", default="[auto]"),
    _unsupported("snowflake.tool_types", _L, "Extra agent tool types."),
    _key(
        "snowflake.allow_unknown_keys",
        _B,
        "Render agent spec keys SST does not model with a warning; false makes each an error.",
        default="true",
    ),
    _key("snowflake.profile", _BLOCK, "Agent profile allowlists."),
    _key("snowflake.profile.avatar_allowlist", _L, "Avatars an agent profile may name; unset allows any."),
)

CONFIG_KEYS: Mapping[str, ConfigKey] = MappingProxyType({key.path: key for key in CONFIG_SCHEMA})

# Keyed by parent path, with the top-level keys under "", in table order.
_CHILDREN: dict[str, dict[str, ConfigKey]] = {}
for _entry in CONFIG_SCHEMA:
    _CHILDREN.setdefault(_entry.parent, {})[_entry.name] = _entry
CHILDREN: Mapping[str, Mapping[str, ConfigKey]] = MappingProxyType(
    {parent: MappingProxyType(children) for parent, children in _CHILDREN.items()}
)
TOP_LEVEL_KEYS: tuple[str, ...] = tuple(CHILDREN[""])
