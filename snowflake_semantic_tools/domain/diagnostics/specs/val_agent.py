"""Validation codes 5xx (VAL): Cortex Agents.

The live agent a secure or shared spec is published over, the rendered spec's completeness and
size, its tools (names, keys, descriptions, resources, and the objects they resolve to), its
instructions, skill sources, orchestration settings, profile, and versions.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL506",
        Severity.WARNING,
        "secure: true requires the owner role to round-trip",
        "agent '{artifact}' is secure; the round-trip check needs the owner role",
        "run CI as the owner role, or accept that the round-trip is skipped",
        condition=(
            "connected validation finds a secure agent's live object owned by a role other than the "
            "session's, so the round-trip comparison (SST-VAL509) is skipped"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL507",
        Severity.ERROR,
        "secure: false transition blocked by a share or application role",
        "agent '{artifact}' is granted to {value}; it cannot become non-secure",
        "keep secure: true, or revoke the share grant first",
        condition=(
            "an agent is authored `secure: false` while its live object is secure and granted to a "
            "share or an application role"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL508",
        Severity.ERROR,
        "Tag name does not resolve",
        "agent '{artifact}': tag '{field}' does not resolve to a tag object",
        "create the tag, or correct the name",
        condition="connected validation finds no tag object for a tag the agent sets",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL509",
        Severity.ERROR,
        "Rendered spec differs from the live spec",
        "agent '{artifact}': re-render differs from the live spec at {value}",
        "apply, or reconcile the authored file with the live object",
        condition=(
            "connected validation finds the live agent, which SST last published from this same "
            "definition, no longer matches the rendered spec: it was changed outside SST"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL510",
        Severity.ERROR,
        "Rendered agent spec omits a section",
        "agent '{artifact}': rendered spec omits {value}",
        "remove the passthrough key that sets the section to null; a partial spec deletes it from the live agent",
        demotable=False,
        condition=(
            "a top-level section the agent carries (models, orchestration, instructions, tools, "
            "tool_resources or skills) is absent or null in the rendered spec"
        ),
        note=(
            "A passthrough key set to null is how it happens; remove that key. The replacing spec "
            "would delete the section from the live agent."
        ),
    ),
    spec(
        "SST-VAL511",
        Severity.ERROR,
        "Rendered spec exceeds the size limit",
        "agent '{artifact}': rendered spec is {size} bytes, over the 100,000 limit",
        "move instructions into referenced files",
        condition="the rendered spec is 100,000 bytes or more, encoded as UTF-8",
    ),
    spec(
        "SST-VAL512",
        Severity.WARNING,
        "Rendered spec is near the size limit",
        "agent '{artifact}': rendered spec is {size} bytes, over 80% of the limit",
        "trim the instruction surface",
        condition="the rendered spec is 80,000 bytes or more and under the 100,000-byte limit",
    ),
    spec(
        "SST-VAL513",
        Severity.ERROR,
        "Resolved tool name is not 1-64 characters",
        "agent '{artifact}': resolved tool name '{name}' is {size} chars",
        "shorten the source name; for Analyst tools this is the derived name",
        condition=(
            "a tool's resolved name is empty or longer than 64 characters; an Analyst tool is named "
            "after its semantic view, an unnamed built-in after its type"
        ),
    ),
    spec(
        "SST-VAL514",
        Severity.ERROR,
        "Resolved tool names collide case-sensitively",
        "agent '{artifact}': tool name '{name}' is declared twice",
        "rename one of them",
        condition="two of an agent's tools resolve to exactly the same name",
    ),
    spec(
        "SST-VAL515",
        Severity.WARNING,
        "Resolved tool names collide case-insensitively",
        "agent '{artifact}': '{a}' and '{b}' differ only by case",
        "rename one; an eval must match the trace name exactly",
        condition="two tool names differ only in case",
    ),
    spec(
        "SST-VAL516",
        Severity.ERROR,
        "Tool type and key combination is illegal",
        "agent '{artifact}': tool '{name}' of type {found} declares '{key}'",
        "remove the key, or change the tool type",
        condition="a tool declares a key that another tool type takes and its own type does not",
    ),
    spec(
        "SST-VAL517",
        Severity.ERROR,
        "web_search tool is not named web_search",
        "agent '{artifact}': web_search tool is named '{name}'",
        "rename it to web_search; the trace name is not configurable",
        condition="a `web_search` tool is named anything other than web_search",
    ),
    spec(
        "SST-VAL518",
        Severity.ERROR,
        "Tool has no description",
        "agent '{artifact}': tool '{name}' has no description",
        "write a description; it is the router",
        condition=(
            "a tool has no description of its own, and its tool member, for a Cortex Search or generic "
            "tool, has none to inherit"
        ),
        note="The orchestrator chooses tools by their descriptions.",
    ),
    spec(
        "SST-VAL519",
        Severity.WARNING,
        "Two tool descriptions are near-duplicates",
        "agent '{artifact}': '{a}' and '{b}' have near-identical descriptions",
        "differentiate them",
        condition=(
            "two of an agent's tool descriptions are at least 90% similar, compared ignoring case and whitespace"
        ),
        note="Near-identical descriptions are the dominant cause of tool_selection_accuracy loss.",
    ),
    spec(
        "SST-VAL520",
        Severity.ERROR,
        "Analyst tool does not declare exactly one semantic view",
        "agent '{artifact}': tool '{name}' declares {count} semantic views",
        "declare exactly one semantic_view and no name",
        condition=(
            "a cortex_analyst_text_to_sql tool declares no `semantic_view`, or also declares a `name`: "
            "an Analyst tool is named after its view"
        ),
    ),
    spec(
        "SST-VAL521",
        Severity.ERROR,
        "Search or generic tool omits a required key",
        "agent '{artifact}': tool '{name}' omits {field}",
        "declare the tool's name, and its search_service or, for a generic tool, its identifier",
        condition=(
            "a cortex_search tool omits `name` or `search_service`, or a generic tool omits `name` or `identifier`"
        ),
        note="A generic tool declares `name` and `identifier`, the `tool()` reference to its member.",
    ),
    spec(
        "SST-VAL522",
        Severity.ERROR,
        "stage_path and relative_path_column not declared together",
        "agent '{artifact}': tool '{name}' declares '{field}' without '{other}'",
        "declare both, or neither; one alone is a silent no-op",
        condition="a cortex_search tool declares one of `stage_path` and `relative_path_column` without the other",
    ),
    spec(
        "SST-VAL523",
        Severity.ERROR,
        "Filter column is not marked filterable",
        "agent '{artifact}': tool '{name}' filters on '{column}', not marked filterable",
        "mark the column filterable: true",
        condition="a cortex_search tool's filter names a column not marked `filterable: true`; once per column",
    ),
    spec(
        "SST-VAL524",
        Severity.ERROR,
        "columns_and_descriptions entry is malformed",
        "agent '{artifact}': tool '{name}' column '{column}': {detail}",
        "use type string or datetime, with boolean searchable and filterable",
        condition=(
            "a `columns_and_descriptions` entry is not a mapping, or its `type`, `searchable` or "
            "`filterable` is missing or of the wrong kind"
        ),
    ),
    spec(
        "SST-VAL525",
        Severity.WARNING,
        "Searchable column is a vector index column",
        "agent '{artifact}': tool '{name}' marks vector column '{column}' searchable",
        "clear searchable on the vector column",
        condition=(
            "connected validation finds a column a cortex_search tool marks searchable is a VECTOR "
            "column of the indexed relation, which Cortex Search cannot search"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL526",
        Severity.ERROR,
        "generic tool declares no object input schema",
        "agent '{artifact}': tool '{name}' input_schema is {found}",
        "declare input_schema with type: object",
        condition="a generic tool's `input_schema` is absent or its `type` is not `object`",
    ),
    spec(
        "SST-VAL527",
        Severity.ERROR,
        "generic tool declares no warehouse",
        "agent '{artifact}': tool '{name}' declares no warehouse",
        "declare a warehouse",
        condition=(
            "a generic tool resolves no warehouse: none on the tool, its tool member, `agents.+warehouse` or the target"
        ),
    ),
    spec(
        "SST-VAL528",
        Severity.WARNING,
        "Agent toolset makes the tool surface non-static",
        "agent '{artifact}' inherits the tools of another agent through toolset '{name}'",
        "accept that tool-name uniqueness and eval coverage are incomplete here",
        condition="an agent declares an `agent_toolset` tool, which inherits another agent's tools",
        note=(
            "The inherited tools are not statically knowable from this spec, so tool-name "
            "uniqueness and eval coverage cannot be checked across them."
        ),
    ),
    spec(
        "SST-VAL529",
        Severity.ERROR,
        "Built-in tool type emits a tool_resources entry",
        "agent '{artifact}': built-in tool '{name}' would emit tool_resources",
        "remove the resources entry",
        condition=(
            "a built-in tool, such as web_search or data_to_chart, declares passthrough resources, or "
            "the rendered spec has a `tool_resources` entry for one"
        ),
    ),
    spec(
        "SST-VAL530",
        Severity.ERROR,
        "tool_resources key matches no rendered tool",
        "agent '{artifact}': tool_resources key '{key}' matches no tools[].name",
        "align the key with the rendered tool name",
        condition="a `tool_resources` key in the rendered spec names no rendered `tools[].tool_spec.name`",
    ),
    spec(
        "SST-VAL531",
        Severity.ERROR,
        "Resolved external object does not exist in the target",
        "agent '{artifact}': '{value}' does not exist in target '{target}'",
        "publish it first, or correct the reference",
        condition=(
            "connected validation finds no object for something an agent uses and SST does not "
            "publish: the object a tool calls, an `extension()` skill source, or the version it pins; an "
            "extension SST cannot see reads as absent"
        ),
        note=(
            "Snowflake accepts an agent whose reference does not exist, and the agent then reports "
            "success and lacks the tool or skill, so this is an error. `sst plan` looks up each "
            "consumed extension and pinned version on every connected plan unless `--no-validate`, "
            "even with `validation.snowflake_syntax_check` off. Demote it with "
            "`diagnostics.severity_overrides` for a target whose referenced objects may be absent."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL532",
        Severity.WARNING,
        "External object signature disagrees with the input schema",
        "agent '{artifact}': '{value}' signature {found} differs from input_schema {expected}",
        "align the schema with the object",
        condition=(
            "connected validation finds the arity, names or types of the live routine a generic tool "
            "calls differ from the tool's `input_schema`"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL533",
        Severity.INFO,
        "generic tool resource key unverified",
        "agent '{artifact}': resource key '{key}' for a generic tool is unverified",
        "confirm against DESCRIBE AGENT on a live agent",
        condition=(
            "a generic tool's `identifier` resource key is not confirmed by the live agent's spec: "
            "the agent does not exist yet, or its live spec lacks the key"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL534",
        Severity.WARNING,
        "query_timeout exceeds the warehouse statement timeout",
        "agent '{artifact}': query_timeout {found} exceeds STATEMENT_TIMEOUT_IN_SECONDS {expected}, "
        "which silently wins",
        "lower query_timeout, or raise the warehouse setting",
        condition=(
            "connected validation finds a tool's `query_timeout` greater than its warehouse's "
            "STATEMENT_TIMEOUT_IN_SECONDS; a warehouse timeout of 0 is no limit and never reported"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL535",
        Severity.WARNING,
        "Instructions name a tool that does not exist",
        "agent '{artifact}': instructions name tool '{name}', absent from tools:",
        "correct the instruction, or add the tool",
        condition=(
            "the orchestration or response instructions name a tool the agent does not have, as a "
            "backticked or snake_case name before the word 'tool' or a quoted name after it; once per "
            "name"
        ),
        note="The most common stale-instruction defect: a tool renamed or removed, its instructions left behind.",
    ),
    spec(
        "SST-VAL536",
        Severity.WARNING,
        "Instructions contradict a tool description",
        "agent '{artifact}': instructions route {value} to '{name}', documented as excluding it",
        "reconcile the instruction and the description",
        condition=(
            "an instruction sentence names a tool and a topic that tool's description excludes ('do "
            "not use for <topic>' or 'not for <topic>'), and negates neither"
        ),
    ),
    spec(
        "SST-VAL537",
        Severity.WARNING,
        "Sample question is a relative-dated duplicate of an eval question",
        "agent '{artifact}': sample question '{value}' is relative-dated and matches an eval row",
        "pin the date, and keep the two sets distinct",
        condition="a sample question is both relative-dated and byte-identical to an eval question",
    ),
    spec(
        "SST-VAL538",
        Severity.ERROR,
        "Skill source does not pin an immutable version",
        "agent '{artifact}': skill source '{name}' does not pin an immutable version",
        "pin a committed version; SST refuses LIVE and an omitted version, though Snowflake accepts both",
        condition=(
            "an agent's `extension()` skill source, for an extension the project consumes, pins no version "
            "-- none is written, or its `{{ var() }}` names no value under `vars:` -- or pins LIVE"
        ),
    ),
    spec(
        "SST-VAL539",
        Severity.ERROR,
        "Skill source points at a mutable stage path",
        "agent '{artifact}': skill source '{name}' is a STAGE path into a mutable bundle",
        "reference a CORTEX EXTENSION version instead",
        condition=(
            "an agent's skill source has source type STAGE, a path into a stage bundle that can change "
            "with no version and no error; the entry is kept"
        ),
    ),
    spec(
        "SST-VAL540",
        Severity.ERROR,
        "Skill entry declares a name",
        "agent '{artifact}': skill('{path}') takes no name; it is named after the skill, not '{name}'",
        "remove name",
        condition="an agent's `skill('<path>')` entry declares a `name`, which is the skill's own",
    ),
    spec(
        "SST-VAL541",
        Severity.WARNING,
        "Two skill entries contribute the same member name",
        "agent '{artifact}': '{a}' and '{b}' both contribute '{name}'; the later wins",
        "rename one of them",
        condition=(
            "two of an agent's skill sources contribute the same member name; a plugin with no `name` "
            "contributes every member"
        ),
        note="Snowflake applies the later entry silently; neither is an error there.",
    ),
    spec(
        "SST-VAL542",
        Severity.WARNING,
        "Consuming role lacks READ on an extension",
        "agent '{artifact}': {value} lacks READ on extension '{name}'",
        "grant READ before publishing",
        condition=(
            "connected validation finds the session's role, or a role granted USAGE on the agent, "
            "holds neither READ nor OWNERSHIP on an extension the agent pins"
        ),
        note=(
            "A warning rather than an error: SST compares the extension's direct grantees only, "
            "without following the role hierarchy, so a role that inherits READ is still reported."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL543",
        Severity.ERROR,
        "Orchestration model is not in the allowlist",
        "agent '{artifact}': orchestration_model '{found}' is not in the allowlist",
        "add it to the config allowlist, or use an allowed model",
        condition=(
            "an agent's resolved orchestration model, from its `config:` or the `agents:` block, is not listed in "
            "`snowflake.orchestration_models`, which defaults to [auto]; compared exactly"
        ),
    ),
    spec(
        "SST-VAL544",
        Severity.ERROR,
        "Orchestration model is auto while a blocking eval exists",
        "agent '{artifact}': orchestration_model is auto and a blocking eval is configured",
        "pin the model explicitly",
        condition=(
            "an agent's orchestration model is `auto` and its eval's tier, `run.tier` else "
            "`evals.+eval_tier`, is blocking"
        ),
        note="With `auto`, a score change cannot be told from a change of model.",
    ),
    spec(
        "SST-VAL545",
        Severity.ERROR,
        "tool_not_accessible is invalid",
        "agent '{artifact}': tool_not_accessible {detail}",
        "set config.tool_not_accessible, or agents.+tool_not_accessible, to accept, reject or legacy",
        condition="an agent's resolved `tool_not_accessible` is a value other than accept, reject or legacy",
    ),
    spec(
        "SST-VAL546",
        Severity.ERROR,
        "analytical_search enabled with no Cortex Search tool",
        "agent '{artifact}': analytical_search is true and no cortex_search tool is declared",
        "declare a cortex_search tool, or disable analytical_search",
        condition="an agent enables `analytical_search` and declares no cortex_search tool",
    ),
    spec(
        "SST-VAL547",
        Severity.WARNING,
        "Token budget read as a cost ceiling",
        "agent '{artifact}': config.budget_tokens covers orchestration only",
        "treat it as an orchestration bound, not a spend cap, and say so in a comment beside it",
        condition=(
            "the agent sets `config.budget_tokens` itself, with no comment on or just above it saying it "
            "bounds orchestration only; an `agents:` default or folder route is not reported per agent"
        ),
    ),
    spec(
        "SST-VAL548",
        Severity.WARNING,
        "Profile value outside its declared allowlist or form",
        "agent '{artifact}': {field} '{found}' {detail}",
        "use an allowlisted avatar, or a plain colour name or var(--token)",
        condition=(
            "an agent's avatar is not in `snowflake.profile.avatar_allowlist`, when that is set, or its "
            "color is neither a plain colour name nor `var(--token)`"
        ),
    ),
    spec(
        "SST-VAL549",
        Severity.WARNING,
        "display_name collides with another agent",
        "agent '{artifact}': display_name '{value}' is shared with {other}",
        "rename one of them; it is the conversational handle",
        condition="two agents share a display name, compared ignoring case",
    ),
    spec(
        "SST-VAL550",
        Severity.ERROR,
        "Deprecated agent still sets an alias",
        "agent '{artifact}': {detail}",
        "remove the alias, or stop deprecating the agent",
        condition="a deprecated agent still sets an `alias`, which points at a version of it",
    ),
)
