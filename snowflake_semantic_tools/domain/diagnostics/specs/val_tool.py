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
        condition="two members of one group share a name",
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
        condition="an owned member has nothing to create from",
    ),
    spec(
        "SST-VAL605",
        Severity.ERROR,
        "reference member carries creation properties",
        "tool member '{name}' is under reference: and declares '{key}'",
        "remove the creation property; a reference is not owned",
        condition="an external member declares how to build itself",
    ),
    spec(
        "SST-VAL606",
        Severity.ERROR,
        "define block present in an immutable group",
        "tool group '{a}' is immutable: true and declares define:",
        "remove the define: block",
        condition="an immutable group claims ownership of an object",
    ),
    spec(
        "SST-VAL607",
        Severity.ERROR,
        "Defined procedure signature does not match the consuming input schema",
        "tool member '{name}': signature {found} differs from input_schema {expected}",
        "align the signature and the schema",
        condition="arity, names or types differ between the procedure and its agent tool",
    ),
    spec(
        "SST-VAL608",
        Severity.ERROR,
        "on: does not resolve to a dbt model",
        "tool member '{name}': on: '{value}' is not a model in the dbt manifest",
        "correct the model name, or run dbt compile",
        condition="a defined search service indexes a model the manifest does not list",
    ),
    spec(
        "SST-VAL609",
        Severity.ERROR,
        "search_column or attribute_columns not on the indexed model",
        "tool member '{name}': '{column}' is not on '{value}'",
        "correct the column list",
        condition="a search service names a column the model does not have",
    ),
    spec(
        "SST-VAL610",
        Severity.WARNING,
        "execute_as: owner on a procedure reachable from an agent",
        "tool member '{name}' runs as owner and is reachable from agent '{artifact}'",
        "review the privilege escalation, or run as caller",
        condition="a deliberate privilege escalation is reachable from an agent tool",
    ),
    spec(
        "SST-VAL611",
        Severity.WARNING,
        "embedding_model is null on an eval-gated search service",
        "tool member '{name}' declares embedding_model: null and is read by a gated agent",
        "pin the embedding model",
        condition="retrieval would change silently between deploys",
    ),
    spec(
        "SST-VAL612",
        Severity.ERROR,
        "DDL would be emitted for a reference member",
        "tool member '{name}' is under reference: and DDL was rendered for it",
        "emit no DDL for referenced members",
        condition="an external object would be created or pruned by SST",
    ),
    spec(
        "SST-VAL613",
        Severity.WARNING,
        "Declared column is absent from the live object",
        "tool member '{name}': column '{column}' is absent from the live object",
        "align the declaration with the object",
        condition="a columns_and_descriptions entry has no counterpart live",
    ),
    spec(
        "SST-VAL614",
        Severity.ERROR,
        "Search service publish order violated",
        "tool member '{name}' would publish before '{value}', which it indexes",
        "include the source relation in the selection, or let dbt build it first -- the ORDER is not authorable",
        condition="a defined search service is ordered before its source relation",
        note=(
            "`tool` publishes after every semantic view and the relation it indexes is a dbt model "
            "built before SST runs, so a violation is a missing selection or an unbuilt model, never "
            "a misordered config."
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
        condition="a deploying or consuming role lacks USAGE or READ on a referenced object",
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
            "an artifact whose `grant_preservation` is `REPLAY` is in a `ChangeSet` with explicit "
            "grants on the live object"
        ),
    ),
)
