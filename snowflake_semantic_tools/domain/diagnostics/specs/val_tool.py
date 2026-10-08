"""Validation codes 6xx (VAL): agent tools -- members, types, ownership, sources, and live objects."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL601",
        Severity.ERROR,
        "Tool member name is not unique within its group",
        "tool group '{a}': member '{name}' is declared twice",
        "rename one of them",
        condition="two members of one tool group share a name, compared ignoring case",
    ),
    spec(
        "SST-VAL602",
        Severity.WARNING,
        "Tool member name is not globally unique",
        "tool member '{name}' is declared in {a} and {b}",
        "rename one; the single-argument ref form is unavailable for this name",
        condition="a member name repeats across groups",
    ),
    spec(
        "SST-VAL603",
        Severity.ERROR,
        "Tool type is not recognised",
        "tool member '{name}': type '{found}' is not a known tool type",
        "use one of {expected}",
        condition="a member declares an unregistered type",
    ),
    spec(
        "SST-VAL604",
        Severity.ERROR,
        "define member carries no creation properties",
        "tool member '{name}' is under define: and declares neither on: nor body_file:",
        "declare on: or body_file:, or move it to reference:",
        condition="a `define:` member other than a stage declares neither `on:` nor `body_file:`",
    ),
    spec(
        "SST-VAL605",
        Severity.ERROR,
        "reference member carries creation properties",
        "tool member '{name}' is under reference: and declares '{key}'",
        "remove the creation property; a reference is not owned",
        condition=(
            "a `reference:` member declares a creation key such as `on:` or `body_file:`, or a "
            "`define:` member declares `relations`"
        ),
        note="For a `define:` member, remove `relations`: SST creates the object, so it has no per-target relation.",
    ),
    spec(
        "SST-VAL606",
        Severity.ERROR,
        "define block present in an immutable group",
        "tool group '{a}' is immutable: true and declares define:",
        "remove the define: block",
        condition="a tool group sets `immutable: true` and declares a member under `define:`",
    ),
    spec(
        "SST-VAL607",
        Severity.ERROR,
        "Defined procedure signature does not match the consuming input schema",
        "tool member '{name}': signature {found} differs from input_schema {expected}",
        "align the signature and the schema",
        condition=(
            "the arity, names or types of a defined procedure's or function's signature differ from the "
            "input schema of the generic agent tool that calls it"
        ),
    ),
    spec(
        "SST-VAL608",
        Severity.ERROR,
        "on: does not resolve to a dbt model",
        "tool member '{name}': on: '{value}' is not a model in the dbt manifest",
        "correct the model name, or run dbt compile",
        condition=(
            "a defined search service's `on:` is not a single `{{ ref('<model>') }}`, or names a model "
            "the dbt manifest does not list"
        ),
    ),
    spec(
        "SST-VAL609",
        Severity.ERROR,
        "search_column or attribute_columns not on the indexed model",
        "tool member '{name}': '{column}' is not on '{value}'",
        "correct the column list",
        condition=(
            "a defined search service's `search_column` or an `attribute_columns` entry is not a "
            "column of its `on:` model"
        ),
    ),
    spec(
        "SST-VAL610",
        Severity.WARNING,
        "execute_as: owner on a procedure reachable from an agent",
        "tool member '{name}' runs as owner and is reachable from agent '{artifact}'",
        "review the privilege escalation, or run as caller",
        condition="an agent's tool reaches a defined procedure declared `execute_as: owner`",
        note="An owner's-rights procedure is a deliberate privilege escalation, now reachable from conversation.",
    ),
    spec(
        "SST-VAL611",
        Severity.WARNING,
        "embedding_model is null on an eval-gated search service",
        "tool member '{name}' declares embedding_model: null and is read by a gated agent",
        "pin the embedding model",
        condition=("a defined search service read by an agent whose eval is blocking sets no `embedding_model`"),
        note="Without a pinned model, retrieval can change between deploys with no change in the project.",
    ),
    spec(
        "SST-VAL612",
        Severity.ERROR,
        "DDL would be emitted for a reference member",
        "tool member '{name}' is under reference: and DDL was rendered for it",
        "emit no DDL for referenced members",
        condition=(
            "DDL was rendered for a `reference:` member, so SST would create, replace or prune an "
            "object it does not own"
        ),
    ),
    spec(
        "SST-VAL613",
        Severity.WARNING,
        "Declared column is absent from the live object",
        "tool member '{name}': column '{column}' is absent from the live object",
        "align the declaration with the object",
        condition=(
            "connected validation finds a column a defined search service declares that the live service does not have"
        ),
    ),
    spec(
        "SST-VAL614",
        Severity.ERROR,
        "Search service publish order violated",
        "tool member '{name}' would publish before '{value}', which it indexes",
        "include the source relation in the selection, or let dbt build it first -- the ORDER is not authorable",
        condition=(
            "a defined search service's `on:` relation has the name of an object another SST artifact "
            "publishes at the tools' position or later, so the service would be created before the "
            "object it indexes exists"
        ),
        note=(
            "The publish order is not authorable: tools publish after every semantic view, and dbt "
            "builds the `on:` model before SST runs. Rename the artifact that publishes to the "
            "relation's name, or point `on:` at the model dbt builds."
        ),
    ),
    spec(
        "SST-VAL615",
        Severity.INFO,
        "Inherited values overridden by an agent tool",
        "agent '{artifact}': tool '{name}' overrides {value}",
        None,
        condition=(
            "every inherited value an agent tool overrides is reported; the tool's own description is "
            "not an override, because it routes the agent"
        ),
    ),
    spec(
        "SST-VAL616",
        Severity.WARNING,
        "Referenced object privilege pre-flight failed",
        "tool member '{name}': {value} lacks {detail}",
        "grant the privilege before publishing",
        condition=(
            "connected validation finds the session's role, or a role granted USAGE on the agent, "
            "holds neither USAGE nor OWNERSHIP on an object a tool calls that SST does not publish"
        ),
        note=(
            "A warning rather than an error: SST compares the object's direct grantees only, without "
            "following the role hierarchy, so a role that inherits the privilege is still reported."
        ),
    ),
    spec(
        "SST-VAL617",
        Severity.WARNING,
        "Search service indexes a relation its dbt materialization rebuilds",
        "tool member '{name}' indexes {value}, materialized '{detail}' -- every dbt run rebuilds the "
        "relation, disabling change tracking and forcing a full re-embed",
        "make the model `incremental`, or set `refresh_mode: FULL` and accept the cost explicitly",
        condition=(
            "a search service's source relation is a dbt model whose materialization replaces it on "
            "every run, while the service is `INCREMENTAL`"
        ),
    ),
    spec(
        "SST-VAL618",
        Severity.WARNING,
        "Search service source lost change tracking",
        "{value} has change_tracking = OFF; service '{name}' cannot refresh incrementally and is serving stale data",
        "re-enable change tracking on the source, or replace the service",
        condition=("the observed source relation has change tracking disabled while the service is `INCREMENTAL`"),
    ),
    spec(
        "SST-VAL619",
        Severity.WARNING,
        "Search service replace will not preserve grants atomically",
        "tool member '{name}': {detail} explicit grant(s) will be captured and replayed -- they do not "
        "exist between commit and replay",
        "none -- Snowflake provides no `COPY GRANTS` for this object type",
        condition=(
            "connected validation finds explicit grants, other than OWNERSHIP, on a defined search "
            "service's live object; reported whether or not a replace is planned"
        ),
        note=(
            "When apply replaces the service it reads those grants first and re-issues them after, "
            "then checks none was lost (SST-APL009); between the replace and the replay the grantees "
            "cannot use the service. Plan the replace for a quiet window."
        ),
    ),
)
