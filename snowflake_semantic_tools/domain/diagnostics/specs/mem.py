"""Membership codes (MEM): how members attach to the artifacts that hold their tables."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Membership"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-MEM001",
        Severity.WARNING,
        "Member declares a table present in no artifact",
        "{member} declares table '{name}', which no {type} lists",
        "add the table to a view, or correct the member",
        condition="a member's declared table is a dbt model that no semantic view lists",
    ),
    spec(
        "SST-MEM002",
        Severity.WARNING,
        "tables absent and nothing inferable",
        "{member} declares no tables: and none can be inferred",
        "declare tables: explicitly",
        condition="a member that is not derived declares no `tables:`, and its expression references no table",
    ),
    spec(
        "SST-MEM003",
        Severity.ERROR,
        "Declared table does not name a known dbt model",
        "{member} declares table '{name}', which is not a known dbt model",
        "use a dbt model name the manifest knows",
        condition="a member's declared table names no dbt model in the manifest",
        note="A table naming a disabled model, or one with no relation, reports SST-DBT009 instead.",
    ),
    spec(
        "SST-MEM004",
        Severity.ERROR,
        "Member declares one table twice",
        "{member} lists table '{name}' more than once",
        "remove the duplicate entry",
        condition="a member's `tables:` list names one table more than once",
    ),
    spec(
        "SST-MEM005",
        Severity.WARNING,
        "Member attached to zero artifacts",
        "{member} attaches to no {type}",
        "add its tables to a view, or delete the member",
        condition=(
            "a member with tables, declared or inferred, or a derived one, attaches to no "
            "semantic view; a metric built on one that failed is not reported again"
        ),
    ),
    spec(
        "SST-MEM006",
        Severity.ERROR,
        "Derived member declares tables",
        "{member} is derived and declares tables:",
        "remove tables:; derived members are view-scoped",
        condition="a derived member declares `tables:`",
    ),
    spec(
        "SST-MEM007",
        Severity.ERROR,
        "Composed member reaches outside its declared tables",
        "{member}: transitive tables {outside} are not in its own tables:",
        "widen tables:, or narrow the expression",
        condition="a member declares tables, and a metric it references reaches a table they leave out",
    ),
    spec(
        "SST-MEM008",
        Severity.ERROR,
        "Member attaches to an artifact lacking a declared table",
        "{member} attaches to {artifact}, which lacks table '{name}'",
        "align the member's tables with the view",
        condition="a member is attached to a semantic view that lacks one of the tables it needs",
    ),
    spec(
        "SST-MEM009",
        Severity.ERROR,
        "Duplicate synonym within an artifact",
        "{artifact}: synonym '{value}' is claimed by {a} and {b}",
        "make synonyms unique within the view",
        condition=(
            "two claimants in one view share a synonym and at least one of them is a metric; a table "
            "and its columns may share one"
        ),
    ),
    spec(
        "SST-MEM010",
        Severity.WARNING,
        "Member declares a table the artifact reaches only by relationship",
        "{member} declares '{name}', reachable from {artifact} only through a join",
        "declare the joined table explicitly",
        condition=(
            "a table the member needs is missing from the view closest to holding it, which "
            "reaches it only through a declared relationship"
        ),
    ),
    spec(
        "SST-MEM011",
        Severity.INFO,
        "Member fan-out",
        "{member} attaches to {count} artifacts",
        None,
        condition="a member attaches to more than one semantic view",
    ),
    spec(
        "SST-MEM012",
        Severity.INFO,
        "Tables inferred for a member",
        "{member}: tables inferred as {value}",
        None,
        condition="a member declares no `tables:`, so they were inferred from its expression",
    ),
    spec(
        "SST-MEM013",
        Severity.ERROR,
        "Attachment differs between two membership calls",
        "{member} attached to {a} then {b}",
        "report this as a bug",
        condition="a member is attached somewhere other than where table membership places it",
    ),
    spec(
        "SST-MEM014",
        Severity.ERROR,
        "Member would be dropped at render after passing attachment",
        "{member} passed attachment for {artifact} and would be dropped at render",
        "fix the expression, or the view's table list",
        condition="a member attached to a semantic view is absent from what that view renders",
    ),
    spec(
        "SST-MEM015",
        Severity.WARNING,
        "Private member referenced from outside the view",
        "{member} is private and is referenced by {artifact}",
        "make it public, or drop the reference",
        condition="a verified query references a private metric",
    ),
    spec(
        "SST-MEM016",
        Severity.ERROR,
        "Member attaches to two artifacts with contradictory scoping",
        "{member} attaches to {a} and {b} with conflicting scope",
        "split the member",
        condition=(
            "a metric renders `USING` a relationship that one of its views holds and another "
            "lacks, so it means something different in each"
        ),
    ),
    spec(
        "SST-MEM100",
        Severity.ERROR,
        "Member attached to a type that accepts no members",
        "{type} accepts no members; {member} was attached",
        "move the member",
        condition="a member is attached to an artifact whose type has no member types",
    ),
    spec(
        "SST-MEM101",
        Severity.WARNING,
        "Inferred tables exceed declared tables",
        "{member}: expression reaches {outside}, beyond its declared tables:",
        "widen tables:, or narrow the expression",
        condition="a member's own expression reaches a table its declared `tables:` leave out",
    ),
    spec(
        "SST-MEM103",
        Severity.INFO,
        "Artifact member counts",
        "{artifact}: {value}",
        None,
        condition="what each semantic view holds once members are attached, counted by member type",
    ),
    spec(
        "SST-MEM104",
        Severity.ERROR,
        "Artifact has no members and its type requires at least one",
        "{artifact} resolves no members",
        "attach at least one dimension or metric",
        condition=("a semantic view whose tables all name dbt models holds no dimension, fact or metric"),
    ),
    spec(
        "SST-MEM105",
        Severity.WARNING,
        "Two artifacts attach contradictory custom instructions to one table",
        "{a} and {b} share table '{name}' with conflicting instructions",
        "reconcile the instruction blocks",
        condition=(
            "two semantic views share a table, and in one instruction channel each attaches "
            "custom instructions the other does not include"
        ),
    ),
    spec(
        "SST-MEM106",
        Severity.ERROR,
        "Verified query attaches through a different membership rule",
        "{member} used a verified-query-specific attachment path",
        "use the one membership function",
        condition="a verified query is attached somewhere other than where table membership places it",
    ),
    spec(
        "SST-MEM107",
        Severity.INFO,
        "Attachment skipped for a poisoned member",
        "{member} skipped: {count} unresolved references",
        None,
        condition="a member whose references did not resolve was not attached",
        note=(
            "A cascade of the member's first unresolved reference, which is reported already: "
            "counted rather than shown unless `--show-cascade`. Overriding it to warning reports "
            "every skip as a warning that is no longer a cascade."
        ),
    ),
    spec(
        "SST-MEM900",
        Severity.ERROR,
        "Attachment not idempotent",
        "attachment for {member} changed on a second pass",
        "report this as a bug",
        condition="running attachment twice produced different results",
    ),
)
