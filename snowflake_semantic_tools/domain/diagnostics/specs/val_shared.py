"""Validation codes 0xx (VAL): names, descriptions, prose, files and references, for every type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL001",
        Severity.ERROR,
        "Name is not unique within its type",
        "{type} '{name}' is declared more than once",
        "rename one of them",
        condition=(
            "two semantic views, metrics, agents, CoWork profiles, tool members or custom eval metrics share a "
            "name, compared case-insensitively across all files"
        ),
        note=(
            "A repeated filter, relationship, verified query or custom instruction name is `SST-PRS106` "
            "or `SST-PRS007`."
        ),
    ),
    spec(
        "SST-VAL002",
        Severity.ERROR,
        "Name is not unique across the extension namespace",
        "{type} '{name}' collides with {other} in another namespace",
        "rename one of them",
        condition=(
            "a skill or plugin this project publishes has the name of an extension it consumes under "
            "`extensions:`, compared case-insensitively"
        ),
    ),
    spec(
        "SST-VAL003",
        Severity.WARNING,
        "Description missing",
        "{type} '{name}' has no description",
        "add a description; it is how Analyst chooses between objects",
        condition=(
            "a semantic view's `description` is absent or blank, a metric has none, or a column of a dbt "
            "model a view uses has none"
        ),
    ),
    spec(
        "SST-VAL004",
        Severity.WARNING,
        "Description shorter than the configured floor",
        "{type} '{name}' description is {size} chars, under {expected}",
        "expand the description",
        condition=(
            "a semantic view's or metric's description is shorter than `validation.description_floor`; "
            "unset, nothing is checked"
        ),
    ),
    spec(
        "SST-VAL005",
        Severity.ERROR,
        "Description does not state when to invoke",
        "{type} '{name}' description describes what it is, not when to use it",
        "say when to use it, for example `Use when the question is about ...`",
        condition=(
            "a semantic view's description holds none of the words that state when to use it, such as "
            "`use`, `when`, `if`, `invoke`, `ask` or `for questions`"
        ),
        note=(
            "An agent's Analyst tool chooses between views by their descriptions alone, so a view whose "
            "description never says when it applies is chosen by chance, if at all. An absent "
            "description is `SST-VAL003`."
        ),
    ),
    spec(
        "SST-VAL006",
        Severity.ERROR,
        "Hardcoded fully-qualified name in an authored file",
        "{type} '{name}': '{field}' hardcodes '{value}'",
        "name the object through a template such as `{{ ref('<model>') }}`, or, in verified-query SQL, by "
        "its logical table name, so it resolves for each target",
        condition=(
            "a view's `tables:` entry, a metric or filter `expr`, or verified-query SQL writes a name of "
            "three or more parts outside a template, string or comment"
        ),
    ),
    spec(
        "SST-VAL007",
        Severity.WARNING,
        "Declared but referenced by nothing",
        "{type} '{name}' is referenced by nothing",
        "add its tables to a view (a custom instruction: list it in a view's `custom_instructions:`), or delete it",
        condition=(
            "an authored metric, filter, relationship, verified query or custom instruction attaches to "
            "no view, and `SST-MEM005` has not already reported it"
        ),
    ),
    spec(
        "SST-VAL008",
        Severity.ERROR,
        "Multi-line string uses a folded scalar",
        "{type} '{name}': '{field}' uses a folded scalar",
        "use |- so line breaks survive",
        condition="a multi-line value uses > or >-, which reflows prose the model reads",
    ),
    spec(
        "SST-VAL009",
        Severity.INFO,
        "File is not canonically formatted",
        "{path} is not canonically formatted",
        "run sst format",
        condition=(
            "a semantic-model file has a tab in its indentation, or not exactly one final newline; "
            "SST-LOD012 reports carriage returns and trailing whitespace, so each problem is reported once"
        ),
        note=(
            "Information only, so strict mode never promotes it; `sst format --check` is the gate for "
            "layout. `sst format` also rewrites indentation and key order; this check does not report those."
        ),
    ),
    spec(
        "SST-VAL010",
        Severity.ERROR,
        "Reference graph contains a cycle",
        "{type} reference cycle: {cycle}",
        "break the cycle",
        condition="the compiled artifacts depend on one another in a cycle, so no publish order satisfies them",
    ),
    spec(
        "SST-VAL011",
        Severity.ERROR,
        "Declared value would be dropped or altered by the renderer",
        "{type} '{name}': '{field}' would be {detail} by the renderer",
        "remove the value, or write it in a form the renderer keeps",
        condition=(
            "a filter carries a label other than `filter`, which the renderer drops, or a verified "
            "query's `verified_by` is not text or has surrounding whitespace, which the renderer "
            "converts, trims or drops"
        ),
    ),
    spec(
        "SST-VAL012",
        Severity.ERROR,
        "Deprecated key spelling in use",
        "{type} '{name}' uses '{field}'; the current spelling is '{expected}'",
        "rename the key to the spelling the message names",
        condition=(
            "a custom instruction writes the 0.3 key `sql_generation` or `question_categorization` "
            "without its 1.0 spelling"
        ),
        note=(
            "An error, so a 0.3 project does not publish under 1.0 unmigrated. The old key is still read, "
            "so a run that demotes the code renders what was meant. Written beside its 1.0 key, it is "
            "`SST-PRS020` instead."
        ),
    ),
    spec(
        "SST-VAL013",
        Severity.ERROR,
        "Unmodelled key would be rendered unvalidated",
        "{type} '{name}': '{key}' is not modelled and would be rendered as-is",
        "promote the key, remove it, or set `snowflake.allow_unknown_keys: true` to render it with a warning instead",
        condition=(
            "an agent spec renders a key SST does not model and `snowflake.allow_unknown_keys` is "
            "`false`; reported once per key"
        ),
    ),
    spec(
        "SST-VAL014",
        Severity.WARNING,
        "Unmodelled key reported",
        "{type} '{name}': {count} unmodelled keys rendered",
        "promote the keys to first-class fields if they are load-bearing",
        condition=(
            "an agent spec renders keys SST does not model and `snowflake.allow_unknown_keys` is `true`, "
            "its default; reported once per agent"
        ),
    ),
    spec(
        "SST-VAL016",
        Severity.ERROR,
        "Referenced object is not published by this project or declared external",
        "{type} '{name}' references '{value}', which is neither published nor declared",
        "enable the view, or point the tool at a view this project publishes",
        condition=(
            "an agent's Analyst tool names a semantic view the project declares but does not publish, "
            "because it is disabled"
        ),
        note="A view the project does not declare at all is `SST-REF011`.",
    ),
    spec(
        "SST-VAL017",
        Severity.WARNING,
        "Two objects express the same thing",
        "{type} '{name}' duplicates {other}",
        "say it once, in the enforceable place",
        condition=(
            "a custom instruction's text writes out a filter's predicate, compared with its `ref()` and "
            "`var()` calls resolved and ignoring case and whitespace"
        ),
    ),
    spec(
        "SST-VAL018",
        Severity.WARNING,
        "Composed prose surface exceeds the configured budget",
        "{artifact}: composed instruction surface is {size} chars, over {expected}",
        "shorten the view's description or its custom instructions, or raise `validation.instruction_budget`",
        condition=(
            "a semantic view's description plus the text of the custom instructions it attaches is longer "
            "than `validation.instruction_budget`; unset, nothing is checked"
        ),
    ),
    spec(
        "SST-VAL019",
        Severity.WARNING,
        "Prose names an object that does not resolve",
        "{type} '{name}': prose names '{value}', which does not resolve",
        "correct the name, or remove the mention",
        condition=(
            "a custom instruction's text names a metric or filter as `the <name> metric` or "
            "`the <name> filter`, and the project declares none by that name"
        ),
    ),
    spec(
        "SST-VAL020",
        Severity.INFO,
        "Rule skipped -- requirement unavailable",
        "{rule_id} skipped: {detail}",
        None,
        condition=(
            "a rule needs a Snowflake connection, a privilege or an observation that was unavailable: "
            "each connected validate rule when syntax checking is off or no connection was given, or a "
            "plan check the role may not read for"
        ),
    ),
)
