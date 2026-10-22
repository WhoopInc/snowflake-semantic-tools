"""The key table of `sst_config.yml`: every key, block, and wildcard slot SST reads.

One table serves two consumers. Validation (`validate.py`) turns an unknown, removed, or
mistyped key into a diagnostic instead of a silent no-op, and the
generated configuration reference renders the same rows, so the documentation cannot
drift from what the engine accepts. Row order is the reference page's order.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.lifecycle.drift import DRIFT_CHECK_FULL, DRIFT_CHECKS
from snowflake_semantic_tools.domain.model.skill import DEFAULT_VERSION_PREFIX
from snowflake_semantic_tools.domain.model.tool import EXECUTE_AS

CONFIG_FILE = "sst_config.yml"
# How many Snowflake sessions `plan`, `validate` and `apply` work on at once when nothing says.
DEFAULT_THREADS = 4
# The two spellings discovery accepts in the project root; both present is an error, not a precedence.
CONFIG_NAMES: tuple[str, ...] = (CONFIG_FILE, "sst_config.yaml")


class KeyKind(Enum):
    """The type of value a key takes.

    `MAP` is a block of free-form names, `ENUM` a string from the key's `choices`, and
    `ANY` accepts every value unchecked. An `INTEGER` key never accepts a boolean. An
    `IDENTIFIER` is a string naming one Snowflake identifier, an `OBJECT_NAME` one
    identifier or a three-part name, and a `SCHEMA_NAME` a two-part `DATABASE.SCHEMA`; each may
    be empty, which reads as unset.
    """

    BLOCK = "block"
    MAP = "map"
    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    LIST = "list"
    ENUM = "enum"
    IDENTIFIER = "identifier"
    OBJECT_NAME = "object name"
    SCHEMA_NAME = "schema name"
    ANY = "any"


class KeyStatus(Enum):
    """Whether SST reads a key, reads it under its new name, or no longer reads it."""

    CURRENT = "current"
    # An error naming its replacement; the rest of the block is checked as that key.
    DEPRECATED = "deprecated"
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
        minimum: Inclusive lower bound of an integer value, or None for no bound.
        maximum: Inclusive upper bound of an integer value, or None for no bound.
        replacement: For a removed key, the reason its diagnostic gives; for a deprecated key,
            the key it is read as.
        code: The code reported instead of the generic one: a removed key's dedicated
            code, or the code for a value outside `choices` or bounds.
        folded: An `ENUM` value is matched against `choices` by `vocabulary_key`, ignoring case
            and runs of spaces, as the member key it is the default for is matched.
        root_only: A `+` key that only the block's root may set: a folder route or an artifact's
            `config:` that sets it is an error (SST-CFG055).
    """

    path: str
    kind: KeyKind
    summary: str
    status: KeyStatus = KeyStatus.CURRENT
    default: str | None = None
    choices: tuple[str, ...] = ()
    minimum: int | None = None
    maximum: int | None = None
    children: ChildPolicy = ChildPolicy.DECLARED
    replacement: str | None = None
    code: str | None = None
    folded: bool = False
    root_only: bool = False

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


_S = KeyKind.STRING
_B = KeyKind.BOOLEAN
_I = KeyKind.INTEGER
_L = KeyKind.LIST
_BLOCK = KeyKind.BLOCK
# A location part a block's objects are created in; a stage or table may be written qualified.
_ID = KeyKind.IDENTIFIER
_OBJ = KeyKind.OBJECT_NAME
_SCH = KeyKind.SCHEMA_NAME

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
    _key(
        "project.cowork_profiles_dir",
        _S,
        "Snowflake CoWork profiles (Snowflake private preview), each routing questions to agents.",
        default="cowork_profiles",
    ),
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
        "Compile expressions against Snowflake during validate and plan, and run each allowed orchestration "
        "model in a throwaway agent (SST-VAL551 to SST-VAL556). Must be written out, true or false (SST-CFG031).",
        default="true",
    ),
    _key(
        "validation.description_floor",
        _I,
        "Shortest description, in characters, a semantic view or metric may carry (SST-VAL004); unset checks none.",
        minimum=1,
    ),
    _key(
        "validation.instruction_budget",
        _I,
        "Longest composed comment and instructions, in characters, a semantic view may publish (SST-VAL018); "
        "unset checks none.",
        minimum=1,
    ),
    _key(
        "validation.exclude_dirs",
        _L,
        "Globs of project-relative directories discovery skips: no semantic model file under one is read.",
        default="[]",
    ),
    _removed("validation.expression_rules", "the expression rules it disabled are no longer optional"),
    _removed("validation.multipath_check", "multi-path relationship analysis is always on"),
    _removed("validation.smoke_query", "smoke probes run only under sst test --suite smoke"),
    _key("diagnostics", _BLOCK, "How severe a diagnostic is, beyond what the error registry declares."),
    _key(
        "diagnostics.severity_overrides",
        KeyKind.MAP,
        "Per-code severity. A warning may become an error or info, an error a warning, and an info a warning; "
        "a non-demotable code is never lowered (SST-CFG033). Strict mode promotes after overrides apply.",
        children=ChildPolicy.NAMES,
    ),
    _key(
        "diagnostics.severity_overrides.<name>",
        KeyKind.ENUM,
        "The severity this registered code reports at; a code the registry lacks is SST-CFG003.",
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
    _key("enrichment.synonym_model", _S, "Cortex model that writes synonyms.", default="claude-sonnet-4-5"),
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
        "False refuses every run that reads row data, one that includes `sample-values` or `enums` (SST-CFG038).",
        default="true",
    ),
    _key(
        "governance",
        _BLOCK,
        "Which Snowflake column tags make a column sensitive, read only when `read_column_tags` is true.",
    ),
    _key(
        "governance.sensitive_tags",
        _L,
        "Tags that make a column sensitive, each `<database>.<schema>.<tag>`, or `<database>.<schema>.*` for "
        "every tag in that schema (SST-CFG057); an unquoted part matches without case. A column carrying one, "
        "set on it or inherited, by any apply method, is never sampled and its values reach no prompt.",
        default="[]",
    ),
    _key(
        "governance.read_column_tags",
        _B,
        "Read each relation's column tags (`TAG_REFERENCES_ALL_COLUMNS`) in `sst enrich` and the connected "
        "`sst validate`; false reads none, so only `pii_tags` and `meta.sst.governance_tags` mark a column.",
        default="false",
    ),
    _key("generation", _BLOCK, "How the commands that reach Snowflake pace their work."),
    _key(
        "generation.threads",
        _I,
        "Sessions `plan`, `validate` and `test` work on at once, and changes `apply` applies at once, unless "
        "`--threads` or `$SST_THREADS` says. Unset, applying takes `skills.+threads` first, and `test` takes 1.",
        default=str(DEFAULT_THREADS),
        minimum=1,
        maximum=16,
    ),
    _key(
        "generation.view_timeout",
        _I,
        "Seconds one statement of a plan or apply session may run (`STATEMENT_TIMEOUT_IN_SECONDS`).",
        default="300",
        minimum=1,
    ),
    _removed("generation.publish_via", "SST 1.0 renders DDL directly"),
    _removed("generation.filters_to_instructions", "standalone filter prose always renders into the view"),
    _removed("generation.use_create_or_alter", "SST 1.0 renders DDL directly"),
    _removed("generation.emit_relationship_type", "relationship and join types are never rendered"),
    _key("dbt", _BLOCK, "How SST produces and reads the dbt manifest."),
    _key(
        "dbt.invoke",
        _B,
        "Run dbt before reading the manifest; false reads the one on disk and checks it against the "
        "model files (SST-DBT005).",
        default="true",
    ),
    _key(
        "dbt.command",
        KeyKind.ENUM,
        "The dbt command that writes the manifest; `compile` needs a warehouse.",
        default="parse",
        choices=("parse", "compile"),
    ),
    _key(
        "dbt.manifest_schema_versions",
        _L,
        "Manifest schema version numbers read, such as `[12]`. The list replaces the default set rather "
        "than adding to it, so name every version to accept; any other is refused (SST-DBT017) unless "
        "`--allow-unsupported-manifest-schema` is given.",
        default="the versions this release is tested against",
    ),
    _key("defer", _BLOCK, "Resolve dbt objects to another target's relations while publishing to `--target`."),
    _key("defer.target", _S, "The `profiles.yml` target relations resolve to; `--defer-target` overrides it."),
    _key(
        "defer.state_path",
        _S,
        "Directory holding that target's dbt `manifest.json`.",
        default="sst_target/defer/<target>",
    ),
    _key(
        "defer.auto_compile",
        _B,
        "Run dbt for the deferred target to write its manifest under `state_path` (dbt Core only).",
        default="false",
    ),
    _key(
        "vars",
        KeyKind.MAP,
        "Values read by `{{ var('<name>') }}`.",
        children=ChildPolicy.NAMES,
    ),
    _key("vars.<name>", KeyKind.ANY, "One variable value."),
    _removed("vars.sha_version", "SST supplies sha_version from the commit being published", code="SST-CFG040"),
    _key("state", _BLOCK, "Where apply records what it published, per target."),
    _key("state.+database", _ID, "State table database.", default="the target database"),
    _key("state.+schema", _ID, "State table schema.", default="the target schema"),
    _key("state.+table", _S, "State table name.", default="SST_STATE"),
    _key(
        "state.+table_kind",
        KeyKind.ENUM,
        "Whether the state and lock tables are standard or hybrid tables; hybrid locks per target.",
        default="standard",
        choices=("standard", "hybrid"),
    ),
    _key(
        "tags",
        _BLOCK,
        "Tag object names read by `{{ tag('<name>') }}`.",
        children=ChildPolicy.NAMES,
    ),
    _key("tags.default_prefix", _S, "`<database>.<schema>` for entries without `fqn:`."),
    _key("tags.<name>", _BLOCK, "One tag reference; empty takes `default_prefix`."),
    _key("tags.<name>.fqn", _S, "The full three-part tag name."),
    _key(
        "tools",
        _BLOCK,
        "Defaults for tool members, overridable per folder and by a member's own `config:`.",
        children=ChildPolicy.ROUTES,
    ),
    _key("tools.+database", _ID, "Database for defined tool objects.", default="the target database"),
    _key("tools.+schema", _ID, "Schema for defined tool objects.", default="the target schema"),
    _key(
        "tools.+warehouse",
        _S,
        "Warehouse for defined search services and procedures.",
        default="the target warehouse",
    ),
    _key("tools.+target_lag", _S, "Target lag for defined search services."),
    _key("tools.+embedding_model", _S, "Embedding model for defined search services."),
    _key(
        "tools.+execute_as",
        KeyKind.ENUM,
        "Rights a defined procedure runs with (`EXECUTE AS`).",
        default="caller",
        choices=EXECUTE_AS,
        folded=True,
    ),
    _key(
        "tools.+immutable",
        _B,
        "Members that may only reference an object (SST-VAL606), and may name one object for every target.",
        default="false",
    ),
    _removed("tools.+enabled", "omit the tools instead"),
    _key(
        "tools.<route>",
        _BLOCK,
        "Folder route: overrides for the tool members under that directory of `project.tools_dir`.",
    ),
    _key(
        "semantic_views",
        _BLOCK,
        "Defaults for semantic views, overridable per folder and by a view's own `config:`.",
        children=ChildPolicy.ROUTES,
    ),
    _key("semantic_views.+database", _ID, "Database for semantic views.", default="the target database"),
    _key("semantic_views.+schema", _ID, "Schema for semantic views.", default="the target schema"),
    _key(
        "semantic_views.+enabled",
        _B,
        "Whether views are built; false leaves them out of the project.",
        default="true",
    ),
    _key(
        "semantic_views.+tags",
        _L,
        "Tags, as `name` and `value` entries, rendered in the CREATE's WITH TAG; a closer setting replaces the list.",
    ),
    _key(
        "semantic_views.+max_staleness",
        _I,
        "`MAX_STALENESS` of views, in seconds; at least 120 (SST-CFG023).",
        minimum=120,
        code="SST-CFG023",
    ),
    _removed("semantic_views.+meta", "put metadata on the view itself"),
    _key("semantic_views.<route>", _BLOCK, "Folder route: overrides for views under that directory."),
    _key(
        "agents",
        _BLOCK,
        "Defaults for Cortex Agents, overridable per folder and by an agent's own `config:`.",
        children=ChildPolicy.ROUTES,
    ),
    _key("agents.+database", _ID, "Database for agents.", default="the target database"),
    _key("agents.+schema", _ID, "Schema for agents.", default="the target schema"),
    _key(
        "agents.+warehouse",
        _S,
        "Warehouse an agent's Analyst, search, and generic tools run on when they name none.",
        default="the target warehouse",
    ),
    _key(
        "agents.+orchestration_model",
        _S,
        "Orchestration model; must be in `snowflake.orchestration_models` (SST-CFG025 here, SST-VAL543 for the "
        "value an agent resolves). `auto` lets Snowflake choose, and SST-VAL560 warns.",
        default="auto",
    ),
    _key(
        "agents.+allow_auto_model",
        _B,
        "Keep an agent's orchestration model on `auto` deliberately: true silences SST-VAL560 for it, not SST-VAL544.",
        default="false",
    ),
    # A budget or timeout below 1 publishes, and Snowflake runs the agent as if none were set
    # (measured: 0 and negative values are stored verbatim and ignored), so each is refused.
    _key("agents.+query_timeout", _I, "Query timeout, in seconds, of an agent's tools that set none.", minimum=1),
    _key("agents.+budget_seconds", _I, "Orchestration time budget, in seconds.", minimum=1),
    _key("agents.+budget_tokens", _I, "Orchestration token budget.", minimum=1),
    _key(
        "agents.+tool_not_accessible",
        KeyKind.ENUM,
        "Behavior when a tool is not accessible.",
        choices=("accept", "reject", "legacy"),
    ),
    _key("agents.+analytical_search", _B, "Enable analytical search."),
    _key("agents.+alias", _S, "Version alias assigned after publication."),
    _key("agents.+enabled", _B, "Whether agents are compiled; false leaves them out of the project.", default="true"),
    _key("agents.+secure", _B, "Publish agents with `SECURE = TRUE`.", default="false"),
    _key(
        "agents.+scratch_schema",
        _SCH,
        "`DATABASE.SCHEMA` where `sst ask` and `sst test --suite evals --unpublished` make their "
        "scratch copies and temporary agents, and connected validate its model probes "
        "(SST-VAL551), for a role that cannot create them in the agent's own "
        "schema, or an agent in a personal database, which cannot hold a semantic view. Nothing "
        "publishes there.",
        default="the agent's own schema",
    ),
    _key(
        "agents.+tags",
        _L,
        "Tags, as `name` and `value` entries, set with ALTER AGENT ... SET TAG; a closer setting replaces the list.",
    ),
    _removed("agents.+copy_grants", "agents are never replaced, so there are no grants to copy"),
    _removed("agents.+meta", "put metadata on the agent itself"),
    _removed("agents.+create_mode", "agents are created once and versioned"),
    _key("agents.<route>", _BLOCK, "Folder route: overrides for agents under that directory."),
    _key(
        "cowork_profiles",
        _BLOCK,
        "Defaults for Snowflake CoWork profiles (Snowflake private preview; "
        "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles), overridable "
        "per folder and by a profile's own `config:`.",
        children=ChildPolicy.ROUTES,
    ),
    _key(
        "cowork_profiles.+database",
        _ID,
        "Database for CoWork profiles; CoWork lists only the profiles in SNOWFLAKE_INTELLIGENCE.PROFILES.",
        default="the target database",
    ),
    _key(
        "cowork_profiles.+schema",
        _ID,
        "Schema for CoWork profiles; CoWork lists only the profiles in SNOWFLAKE_INTELLIGENCE.PROFILES.",
        default="the target schema",
    ),
    _key(
        "cowork_profiles.+orchestration_model",
        _S,
        "Orchestration model the profile routes questions with; must be in `snowflake.orchestration_models` "
        "(SST-CFG025). `auto` lets Snowflake choose, and SST-VAL560 warns.",
        default="auto",
    ),
    _key(
        "cowork_profiles.+allow_auto_model",
        _B,
        "Keep a profile's orchestration model on `auto` deliberately: true silences SST-VAL560 for it, not SST-VAL544.",
        default="false",
    ),
    _key("cowork_profiles.+alias", _S, "Version alias assigned after publication."),
    _key(
        "cowork_profiles.+enabled",
        _B,
        "Whether CoWork profiles are compiled; false leaves them out of the project.",
        default="true",
    ),
    _key(
        "cowork_profiles.+attach",
        _B,
        "Attach each profile published to SNOWFLAKE_INTELLIGENCE.PROFILES to the account's CoWork object, "
        "and again when it was detached by hand; false leaves it unattached, and never detaches one "
        "(Snowflake private preview; "
        "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles).",
        default="true",
    ),
    _key(
        "cowork_profiles.+tags",
        _L,
        "Tags, as `name` and `value` entries, set with ALTER AGENT ... SET TAG; a closer setting replaces the list.",
    ),
    _key("cowork_profiles.<route>", _BLOCK, "Folder route: overrides for CoWork profiles under that directory."),
    _key(
        "evals",
        _BLOCK,
        "Defaults for agent evaluations, overridable per agent folder and by an eval config's own `config:`.",
        children=ChildPolicy.ROUTES,
    ),
    _key(
        "evals.+eval_tier",
        KeyKind.ENUM,
        "Whether a regression blocks; unset is `report`.",
        choices=("blocking", "report"),
    ),
    _key("evals.+metrics", _L, "System metrics, ungated, of an eval config whose `metrics:` lists no `system`."),
    _key(
        "evals.+metric_version",
        _S,
        "Version of each system metric that names none; only `v3` is accepted (SST-VAL723), and unset with "
        "none named is SST-VAL722.",
    ),
    _key("evals.+judge_model", _S, "Judge model of each custom metric that names none."),
    _key("evals.+agent_version", _S, "Agent version an eval runs against."),
    _key("evals.+retry", _I, "Retries per run; unset retries none. Each attempt is reported."),
    _key(
        "evals.+concurrency",
        _I,
        "Eval runs a suite starts and polls at once. At the root it is also the most a folder route or an "
        "eval's `config:` may ask (SST-VAL731).",
    ),
    _key("evals.+baseline_runs", _I, "Completed attempts a baseline needs; unset needs 1."),
    _key("evals.+min_dataset_rows", _I, "Smallest accepted question set (SST-VAL710); unset checks none."),
    _key(
        "evals.+candidate_models",
        _L,
        "Orchestration models `sst test --suite evals --compare-models` compares with the pinned one, "
        "each from `snowflake.orchestration_models` when that is set (SST-VAL771) and never `auto` (SST-VAL770); "
        "`--compare-models <list>` replaces it.",
    ),
    _key(
        "evals.+compare_runs",
        _I,
        "Runs of each model a comparison makes, at least 1; `--compare-runs` replaces it.",
        default="2",
        minimum=1,
    ),
    _key(
        "evals.+retention",
        KeyKind.ENUM,
        "Retention class recorded with each run; SST never deletes a run on the strength of it.",
        choices=("audit", "decision"),
    ),
    _removed("evals.+enabled", "omit the evals instead"),
    _removed("evals.+database", "eval objects resolve to the agent's schema", code="SST-CFG015"),
    _removed("evals.+schema", "eval objects resolve to the agent's schema", code="SST-CFG015"),
    _key(
        "evals.<route>",
        _BLOCK,
        "Folder route under `project.agents_dir`: overrides for the evals of the agents under that directory.",
    ),
    _key(
        "skills",
        _BLOCK,
        "Defaults for skills, overridable per folder and by a skill's own `config:`. A skill publishes as a "
        "Cortex Extension only when `+bundle_stage` resolves for it.",
        children=ChildPolicy.ROUTES,
    ),
    _key("skills.+database", _ID, "Database for skill extensions.", default="the target database"),
    _key("skills.+schema", _ID, "Schema for skill extensions.", default="the target schema"),
    _key(
        "skills.+bundle_stage",
        _OBJ,
        "Internal stage that `ADD VERSION` reads from; created when absent. Unset, the skill does not publish.",
    ),
    _key(
        "skills.+certified",
        _B,
        "Tag each new skill version `SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'`.",
        default="false",
    ),
    _key(
        "skills.+version_prefix",
        _S,
        "Prefix of every version alias, and of each profile's VERSION; the rest is 12 hex characters of the digest.",
        default=DEFAULT_VERSION_PREFIX,
    ),
    _key(
        "skills.+threads",
        _I,
        "Changes `apply` applies at once when neither `--threads`, `$SST_THREADS`, nor `generation.threads` "
        "says. Profiles always publish one at a time.",
        default=str(DEFAULT_THREADS),
        minimum=1,
        maximum=16,
        root_only=True,
    ),
    _removed("skills.+grant_read_to", "access is managed outside SST; SST never issues grants"),
    _removed("skills.+enabled", "a skill publishes only where +bundle_stage resolves for it"),
    _removed(
        "skills.catalog",
        "set +bundle_stage, +database and +schema on skills: and plugins: themselves",
        code="SST-CFG042",
    ),
    _removed("skills.stage", "profiles publish through the profiles: block", code="SST-CFG042"),
    _removed("skills.extensions", "consumed extensions are the top-level extensions: block", code="SST-CFG042"),
    _key("skills.<route>", _BLOCK, "Folder route: overrides for the skills under that directory."),
    _key(
        "plugins",
        _BLOCK,
        "Defaults for plugins, overridable per folder and by a plugin's own `config:`. A plugin publishes as a "
        "Cortex Extension only when `+bundle_stage` resolves for it.",
        children=ChildPolicy.ROUTES,
    ),
    _key("plugins.+database", _ID, "Database for plugin extensions.", default="the target database"),
    _key("plugins.+schema", _ID, "Schema for plugin extensions.", default="the target schema"),
    _key(
        "plugins.+bundle_stage",
        _OBJ,
        "Internal stage that `ADD VERSION` reads from; created when absent. Unset, the plugin does not publish.",
    ),
    _key(
        "plugins.+certified",
        _B,
        "Tag each new plugin version `SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'`.",
        default="false",
    ),
    _key(
        "plugins.+version_prefix",
        _S,
        "Prefix of every plugin version alias; the rest is 12 hex characters of the bundle digest.",
        default=DEFAULT_VERSION_PREFIX,
    ),
    _key("plugins.<route>", _BLOCK, "Folder route: overrides for the plugins under that directory."),
    _key(
        "profiles",
        _BLOCK,
        "Defaults for CoCo Desktop profiles, overridable per folder and by a profile's own `config:`. A "
        "profile publishes only when `+stage` resolves for it.",
        children=ChildPolicy.ROUTES,
    ),
    _key("profiles.+database", _ID, "Database of the profile stage and registry.", default="the target database"),
    _key("profiles.+schema", _ID, "Schema of the profile stage and registry.", default="the target schema"),
    _key(
        "profiles.+stage",
        _OBJ,
        "Internal stage that holds profile trees; created when absent. Unset, the profile does not publish.",
    ),
    _key(
        "profiles.+registry_table",
        _OBJ,
        "Profile registry table. Desktop reads `CORTEX_CODE.CONFIG.PROFILE_REGISTRY`.",
        default="PROFILE_REGISTRY",
    ),
    _key("profiles.<route>", _BLOCK, "Folder route: overrides for the profile in that directory."),
    _key(
        "extensions",
        _BLOCK,
        "Name map for extensions this project consumes but does not publish.",
        children=ChildPolicy.NAMES,
    ),
    _key("extensions.default_prefix", _S, "`<database>.<schema>` for entries without `fqn:`."),
    _key("extensions.<name>", _BLOCK, "One consumed extension; empty takes `default_prefix`."),
    _key("extensions.<name>.fqn", _S, "The full three-part extension name."),
    _key("apply", _BLOCK, "Stages apply uploads through."),
    _key(
        "apply.agent_spec_stage",
        _BLOCK,
        "Internal stage agent specs are uploaded to. Created when absent, server-side encrypted; "
        "an existing stage of another kind blocks the plan.",
    ),
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
    _key(
        "apply.create_schemas",
        _B,
        "Create each absent schema the plan publishes into, as dbt does, `CREATE SCHEMA IF NOT EXISTS`, "
        "before anything else; false makes an absent schema SST-PLN010. Plan reads nothing in one it creates, so "
        "everything published there, skills, profiles and evals among them, plans as a create. Never a database, "
        "and never dropped.",
        default="true",
    ),
    _deprecated("deploy", "apply"),
    _key("ask", _BLOCK, "How `sst ask` keeps and shows a conversation."),
    _key(
        "ask.transcripts",
        _B,
        "Keep a transcript of each run in `sst_target/ask/`, every turn whole with each specification it ran on; "
        "`--transcript` and `--no-transcript` override it.",
        default="false",
    ),
    _key(
        "ask.theme",
        _S,
        "The full-screen conversation's color theme: `sst-dark` or one of Textual's own; `--theme` and "
        "`$SST_ASK_THEME` override it, and an unknown one is SST-ASK011.",
        default="sst-dark",
    ),
    _key("plan", _BLOCK, "What plan compares beyond each object's ownership marker."),
    _key(
        "plan.drift_check",
        KeyKind.ENUM,
        "Which live properties SST published plan compares with what apply read back, to find an "
        "edit that kept the ownership marker (SST-PLN014). `full` also reads each procedure's "
        "DESCRIBE and each semantic view's GET_DDL, one query each; `basic` compares only what "
        "SHOW and an agent's versions list.",
        default=DRIFT_CHECK_FULL,
        choices=DRIFT_CHECKS,
    ),
    _key("snowflake", _BLOCK, "Allowlists for Snowflake surfaces the renderer accepts."),
    _key(
        "snowflake.orchestration_models",
        _L,
        "Orchestration models agents may name; the syntax check runs each one in a throwaway agent.",
        default="[auto]",
    ),
    _key(
        "snowflake.tool_types",
        _L,
        "Agent tool types added to the ones SST renders, for a type Snowflake shipped since this release.",
    ),
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
