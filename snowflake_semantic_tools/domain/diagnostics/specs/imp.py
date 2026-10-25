"""Import codes (IMP): `sst import`, which writes live objects as files, and `sst state adopt`.

Import reads Snowflake and writes only files; adopt reads Snowflake and writes only state. What
they report is what a live object could not be read as, or did not come back as, and what the
files written for it leave for the user to check.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Import"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-IMP001",
        Severity.ERROR,
        "Object to import is not found",
        "{type} {value} is not found, or role {name} cannot see it",
        "check the name; Snowflake answers alike for an object that is absent and one the role "
        "may not see, so check the role's privileges too",
        condition="the object named to import is not listed, or SHOW refuses the schema or database",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP002",
        Severity.ERROR,
        "Object is of another type",
        "{value} is a {found}, not a {expected}",
        "import it as what it is: `sst import {found} {value}`",
        condition=(
            "an agent imported as a CoWork profile, or a CoWork profile, whose spec has an "
            "agent_router tool, imported as an agent"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP003",
        Severity.INFO,
        "Object is already published by SST",
        "{value} carries SST's ownership marker; it was not imported",
        "if this project published it, `sst state recover` records it; an object another "
        "project publishes is that project's",
        condition="the object's comment carries an `[sst:...]` ownership marker",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP004",
        Severity.ERROR,
        "Import destination already exists",
        "{path} already exists; {value} was not imported",
        "move or remove the file, or choose another folder with --to; import never overwrites",
        condition=(
            "a file the import would write is already in the project, or two imported objects would write one path"
        ),
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP005",
        Severity.ERROR,
        "Role cannot read the object",
        "role {name} cannot read {type} {value} ({step}): {detail}",
        "grant the role what reading it needs, or run as a role that has it; SST never grants privileges itself",
        condition="Snowflake refuses one of the reads an import of the object makes",
        note=(
            "The importing guide lists the reads each type makes. In a run over a schema or "
            "database, the other objects are still imported."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP006",
        Severity.INFO,
        "Nothing to import",
        "no {type} in {value}{detail}",
        "check the database and schema, and any --like or --exclude pattern",
        condition="listing the scope finds no object of the type, after the patterns are applied",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP007",
        Severity.INFO,
        "Object is the project's already",
        "{value} is what the project's {artifact} publishes; it was not imported",
        "when state does not record it, `sst state adopt --select {artifact}` takes it over",
        condition=(
            "the project compiles an artifact to the object, or a tool member references it; an "
            "extension carries no ownership marker, so this is how one SST publishes is known"
        ),
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP010",
        Severity.ERROR,
        "Table is no model of the dbt manifest",
        "{artifact}: table {value} is no model of the dbt manifest for target {target}; the view was not imported",
        "build the table as a dbt model, parse the manifest for that target, then import the view",
        condition="a semantic view's base table resolves to no dbt model's relation in the manifest",
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP011",
        Severity.WARNING,
        "Column metadata disagrees; the project's is kept",
        "{artifact}: {model}.{column} {key} is {found} in the project and {expected} live; the project's is kept",
        "reconcile it by hand if the live value is the right one",
        condition="an imported dimension or fact sets a key the dbt model's column already sets to another value",
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP012",
        Severity.WARNING,
        "Reference the project does not compile",
        "{artifact} names {type} {value}, which this project does not compile; written as {found}",
        "import it as well (`sst import {type} {value}`, or --with-dependencies), or declare it, before you compile",
        condition=(
            "an imported object names a semantic view, tool, skill or agent no compiled artifact "
            "of the project publishes at that name for the target"
        ),
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP013",
        Severity.WARNING,
        "Imported definition differs from the live one",
        "{artifact}: {detail}",
        "review the file before `sst state adopt`, which shows each difference that remains",
        condition=(
            "a live property SST cannot write as it is, such as a Cortex Analyst tool named other "
            "than its view, a STAGE skill source, or a setting SST does not model"
        ),
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP014",
        Severity.INFO,
        "No folder routes to the live location",
        "{artifact}: no {block} folder places it in {value} for target {target}; its `config:` names the location",
        "add a folder route for that location, move the file under it, and remove the `config:` override",
        condition=(
            "no folder of the type's directory, nor its root, resolves to the object's database "
            "and schema for the target, so the file sets its own"
        ),
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP015",
        Severity.INFO,
        "Sample values of PII-tagged columns dropped",
        "{artifact}: dropped the sample values of {count} PII-tagged column(s)",
        "nothing to do; a PII-tagged column publishes no sample values",
        condition="an imported column carries a PII tag and live sample values",
        precheck=Precheck.LOCAL,
    ),
    spec(
        "SST-IMP020",
        Severity.ERROR,
        "Live object differs from the compiled artifact",
        "{artifact}: {value} differs from what the project compiles: {detail}",
        "edit the files until adopt shows no difference, or pass --accept-differences to publish "
        "the project's definition over the live one at the next apply",
        condition=(
            "`sst state adopt` compares the live object with the compiled artifact, member by member, and they differ"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP021",
        Severity.INFO,
        "Object adopted",
        "{artifact}: recorded {value} in state as adopted; the next apply stamps SST's ownership marker",
        "run `sst plan` to see the stamp, then `sst apply`",
        condition="`sst state adopt` recorded a state entry for an object SST did not publish",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-IMP022",
        Severity.ERROR,
        "Artifact cannot be adopted",
        "{artifact}: {reason}",
        "adopt only an artifact the plan blocks as an existing object SST does not manage (SST-PLN024)",
        condition=(
            "an artifact selected for adopt is not blocked as unmanaged: it is absent, already "
            "recorded, or its live object could not be read"
        ),
        precheck=Precheck.OBSERVE,
    ),
)
