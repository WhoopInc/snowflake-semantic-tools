"""Validation codes 6xx (VAL): agent tools -- members, types, ownership, sources, and live objects."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
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
        "tool member '{name}' is defined here and declares neither on: nor body:",
        "declare on: or body:, or reference an existing object with relations:",
        condition="a defined member other than a stage declares neither `on:` nor `body:`",
    ),
    spec(
        "SST-VAL605",
        Severity.ERROR,
        "reference member carries creation properties",
        "tool member '{name}' references an existing object and declares '{key}'",
        "remove the creation property; a reference is not owned",
        condition=(
            "a member that writes `relations:`, and so references an existing object, declares a "
            "creation key such as `on:` or `body:`"
        ),
        note="To have SST create the object instead, remove `relations:`; a member without it is defined.",
    ),
    spec(
        "SST-VAL606",
        Severity.ERROR,
        "Immutable tool member is not a reference",
        "tool member '{name}' is immutable and declares no relations:",
        "reference the existing object with relations:, or set immutable to false for it",
        condition=(
            "a tool member's settings resolve `immutable` to true -- its own `config:`, or a "
            "`tools:` folder route over its file -- and it writes no `relations:`, so SST would create it"
        ),
    ),
    spec(
        "SST-VAL607",
        Severity.ERROR,
        "Defined procedure signature does not match the consuming input schema",
        "tool member '{name}': signature {found} differs from input_schema {expected}",
        "align the signature and the schema",
        condition=(
            "the arity, names or types of a defined procedure's or function's signature differ from the "
            "input schema of the generic agent tool that calls it, or the schema's `required` leaves "
            "out a parameter, all of which are required"
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
        condition="an agent's tool reaches a defined procedure whose `execute_as` setting resolves to `owner`",
        note="An owner's-rights procedure is a deliberate privilege escalation, now reachable from conversation.",
    ),
    spec(
        "SST-VAL611",
        Severity.WARNING,
        "No embedding model pinned on an eval-gated search service",
        "tool member '{name}' pins no embedding_model and is read by a gated agent",
        "pin the embedding model with config.embedding_model, a folder route, or tools.+embedding_model",
        condition=(
            "a defined search service read by an agent whose eval is blocking resolves no `embedding_model` setting"
        ),
        note="Without a pinned model, retrieval can change between deploys with no change in the project.",
    ),
    spec(
        "SST-VAL612",
        Severity.ERROR,
        "DDL would be emitted for a reference member",
        "tool member '{name}' references an existing object and DDL was rendered for it",
        "emit no DDL for referenced members",
        condition=(
            "DDL was rendered for a member that writes `relations:`, so SST would create, replace or prune an "
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
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL621",
        Severity.ERROR,
        "Routine key its language does not take",
        "tool member '{name}' is written in {value}, which takes no '{key}'",
        "remove the key, or write the routine in Java, Python or Scala",
        condition=(
            "a defined procedure or function in JavaScript or SQL sets `runtime_version`, `handler`, "
            "`packages` or `imports`, which only Java, Python and Scala routines take; once per key"
        ),
        note=(
            "Snowflake's JavaScript and Snowflake Scripting CREATE PROCEDURE and CREATE FUNCTION have "
            "no such clauses, so SST would either render a CREATE Snowflake refuses or drop the key."
        ),
    ),
    spec(
        "SST-VAL622",
        Severity.ERROR,
        "Tool column names another model",
        "tool member '{name}': {field} names model '{found}', not its on: model '{expected}'",
        "reference a column of the on: model, as {{ ref('<on model>', '<column>') }}",
        condition=(
            "a column a defined search service names -- `search_column`, an attribute, an id, title "
            "or relative path column, or a `columns:` entry, on the member or on an agent's search "
            "tool over it -- or a column `ref()` in its `where`, names a model other than its `on:` model"
        ),
    ),
    spec(
        "SST-VAL623",
        Severity.ERROR,
        "Generic tool calls a table-returning routine",
        "agent '{artifact}': generic tool '{name}' calls '{member}', which returns a table",
        "return one value, such as a VARCHAR holding JSON, from the routine the tool calls",
        condition="a generic agent tool's procedure or function member declares `returns: TABLE (...)`",
        note=(
            "Snowflake's tool runtime reads a single cell back, so every call fails with 'expected a "
            "single cell result set'. Calling the routine outside a generic tool is unaffected."
        ),
    ),
    spec(
        "SST-VAL624",
        Severity.ERROR,
        "Generic tool cannot name a quoted parameter",
        "agent '{artifact}': generic tool '{name}' cannot pass '{member}' parameter '{field}', whose name is quoted",
        "rename the parameter to an unquoted identifier that is not a reserved word",
        condition=(
            "a parameter of a generic agent tool's procedure or function member renders quoted: it is "
            "a reserved word such as `select`, or written quoted"
        ),
        note=(
            "Snowflake's tool runtime names each argument unquoted (`select => ?`), so the call is a "
            "syntax error on every run. Calling the routine outside a generic tool is unaffected."
        ),
    ),
    spec(
        "SST-VAL625",
        Severity.ERROR,
        "Generic tool cannot pass a semi-structured parameter",
        "agent '{artifact}': generic tool '{name}' cannot pass '{member}' parameter '{field}' of type {found}",
        "take the value as VARCHAR, and parse it in the routine with PARSE_JSON",
        condition=("a parameter of a generic agent tool's procedure or function member is ARRAY, OBJECT or VARIANT"),
        note=(
            "Snowflake's tool runtime refuses those argument types on a warehouse, so every call fails. "
            "Calling the routine outside a generic tool is unaffected."
        ),
    ),
    spec(
        "SST-VAL626",
        Severity.WARNING,
        "Routine body hard-codes a project relation",
        "tool member '{name}': body hard-codes {relation}, which is {expected}",
        "write {expected} instead, so each target's routine reads its own relation",
        condition=(
            "a procedure's or function's body writes the three-part name of a relation that a dbt "
            "model, seed, snapshot or source of the project resolves to for the target"
        ),
        note=(
            "A literal name reads that one target's relation from every target, and lineage cannot "
            "see it. A name no project relation has, such as another team's table, and a two-part "
            "name are never reported. In a SQL body, names in strings and comments are skipped; any "
            "other language is scanned whole, since a table name there sits in a string. A "
            "deliberate literal can be baselined."
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
        precheck=Precheck.OBSERVE,
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
            "every run, and the service's `refresh_mode` is not `FULL` (unset, `AUTO`, or "
            "`INCREMENTAL`)"
        ),
    ),
    spec(
        "SST-VAL618",
        Severity.WARNING,
        "Search service source lost change tracking",
        "{value} has change_tracking = OFF; service '{name}' cannot refresh incrementally and is serving stale data",
        "re-enable change tracking on the source, or replace the service",
        condition=("the observed source relation has change tracking disabled while the service is `INCREMENTAL`"),
        precheck=Precheck.OBSERVE,
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
        precheck=Precheck.OBSERVE,
    ),
)
