"""dbt seam codes (DBT): the manifest, the profile, invoking dbt, and SST metadata on dbt models."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "dbt"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-DBT001",
        Severity.ERROR,
        "dbt manifest absent or empty",
        "no dbt models available",
        "check that the dbt project has models; under --manifest, check the path",
        condition="the dbt manifest SST reads lists no model",
    ),
    spec(
        "SST-DBT002",
        Severity.ERROR,
        "dbt model referenced but absent from the manifest",
        "model '{model}' is not in the dbt manifest",
        "run dbt compile, or correct the name",
        condition=(
            "a model the semantic layer references, or a name `sst enrich` selects without a "
            "wildcard, is no model the manifest knows"
        ),
    ),
    spec(
        "SST-DBT003",
        Severity.ERROR,
        "config.meta.sst names an unknown role",
        "model '{model}': meta.sst role '{found}' is not a known role",
        "correct the role name",
        condition="a column's `meta.sst.column_type` is not a known column kind",
    ),
    spec(
        "SST-DBT004",
        Severity.WARNING,
        "Column type in dbt disagrees with the declared type",
        "model '{model}': column '{column}' is {found} in dbt and {expected} in the semantic layer",
        "reconcile the two, or add a contract",
        condition="a column's declared `data_type` disagrees with the type dbt records for it",
    ),
    spec(
        "SST-DBT005",
        Severity.ERROR,
        "dbt manifest is stale relative to the models on disk",
        "manifest is older than {value}",
        "let SST run dbt (drop --manifest, or set dbt.invoke: true), or rebuild the manifest",
        condition=(
            "under `--manifest` or `dbt.invoke: false`, a root-project model file's contents no longer "
            "match the checksum the manifest recorded; once per file"
        ),
    ),
    spec(
        "SST-DBT006",
        Severity.ERROR,
        "Resolved relation differs from the model name",
        "model '{model}' resolves to relation '{value}'",
        "point the semantic layer at the resolved relation",
        condition=(
            "a literal relation names a dbt model as `<database>.<schema>.<model name>` and the model's "
            "`alias:` makes it build under another name, so nothing exists there"
        ),
    ),
    spec(
        "SST-DBT009",
        Severity.ERROR,
        "Consumed model is disabled or ephemeral",
        "model '{model}' is {found} and produces no relation",
        "enable it, or change the materialisation",
        condition="a model the semantic layer consumes is disabled, or materialised so that it has no relation",
    ),
    spec(
        "SST-DBT010",
        Severity.ERROR,
        "Collapse safety rule violated",
        "models {a} and {b} collapse to '{value}' and differ in {column}",
        "rename one relation, or align the columns",
        condition="two models resolve to one relation and differ in a column the semantic layer uses",
    ),
    spec(
        "SST-DBT011",
        Severity.ERROR,
        "Source referenced by the semantic layer is not declared in dbt",
        "source '{value}' is not declared in dbt",
        "declare the source",
        condition="no dbt source declares a `source.table` pair the semantic layer uses, or it has no relation",
    ),
    spec(
        "SST-DBT012",
        Severity.WARNING,
        "Duplicate source name or source.table pair",
        "source '{value}' is declared more than once",
        "rename one of them",
        condition="the manifest declares one `source.table` pair more than once; once per pair",
        note="dbt tolerates the duplicate; SST cannot tell which declaration a reference means.",
    ),
    spec(
        "SST-DBT013",
        Severity.ERROR,
        "Model node has no name",
        "dbt node {value} has no name and was skipped",
        "report this against dbt; a named node is a manifest invariant",
        condition="a manifest model node has no name; it is skipped",
    ),
    spec(
        "SST-DBT014",
        Severity.ERROR,
        "Model node has no database or schema",
        "model '{model}' has empty {key}",
        "set database and schema for this target in profiles.yml or dbt_project.yml",
        condition="a manifest model has a relation but an empty `database` or `schema`; it is skipped",
    ),
    spec(
        "SST-DBT015",
        Severity.ERROR,
        "Model checksum absent on both sides of a comparison",
        "model '{model}' has no checksum; change detection is unusable",
        "check that the dbt installation populates checksum.checksum",
        condition="a model the semantic layer consumes has no checksum in the manifest",
        note="Two missing checksums compare equal, so every such model reads as unchanged.",
    ),
    spec(
        "SST-DBT016",
        Severity.INFO,
        "Model to artifact fan-out",
        "model '{model}' feeds {count} artifacts",
        None,
        condition="a dbt model feeds two or more SST artifacts",
        note=(
            "Attachment is implicit by table membership, so a change to the model reaches more "
            "than its own tests imply."
        ),
    ),
    spec(
        "SST-DBT017",
        Severity.ERROR,
        "dbt manifest schema version is unsupported",
        "manifest schema '{found}'; supported: {expected}",
        "upgrade or downgrade dbt to a version that emits a supported schema, or pass "
        "--allow-unsupported-manifest-schema",
        condition="the manifest's `dbt_schema_version` names a version outside the supported set",
    ),
    spec(
        "SST-DBT018",
        Severity.ERROR,
        "dbt manifest schema version key is absent or unparseable",
        "manifest schema version is {found}",
        "check the dbt version; pass --allow-unsupported-manifest-schema only if you accept the risk",
        condition="the manifest's `dbt_schema_version` is absent or names no version",
    ),
    spec(
        "SST-DBT019",
        Severity.ERROR,
        "dbt profile could not be parsed",
        "{path}: {detail}",
        "fix profiles.yml",
        condition="`profiles.yml` is not valid YAML, or its root is not a mapping of profiles",
    ),
    spec(
        "SST-DBT020",
        Severity.WARNING,
        "dbt installation type could not be determined",
        "dbt type detection returned {found}",
        "ignore this if a valid manifest exists; the manifest is the contract",
        condition=(
            "`dbt --version` runs and names no dbt installation SST knows, or exits non-zero; a dbt "
            "that cannot be started at all is SST-DBT027"
        ),
    ),
    spec(
        "SST-DBT021",
        Severity.ERROR,
        "auto_compile requested under an installation that cannot compile a non-default target",
        "defer.auto_compile is true under {found}",
        "use state_path instead",
        condition="`defer.auto_compile` is set while the installed dbt is the dbt Cloud CLI",
        note="The dbt Cloud CLI cannot compile a non-default target locally.",
    ),
    spec(
        "SST-DBT022",
        Severity.ERROR,
        "model-paths is unreadable and defaulted",
        "model-paths could not be read; defaulted to {expected}",
        "declare model-paths explicitly",
        condition="`model-paths` in dbt_project.yml is not a list of directories; dbt's default is used",
    ),
    spec(
        "SST-DBT023",
        Severity.WARNING,
        "dbt model referenced with no tests at all",
        "model '{model}' has no tests",
        "add at least a uniqueness test on the declared grain",
        condition="no dbt test is attached to a model the semantic layer consumes",
        note="The semantic layer then asserts business meaning over data nothing verifies.",
    ),
    spec(
        "SST-DBT024",
        Severity.WARNING,
        "dbt model feeding an artifact has no contract",
        "model '{model}' has no contract",
        "add a contract",
        condition=(
            "a model the semantic layer consumes enforces no contract and leaves a column's type to "
            "inference; a model that types every column with `data_type` has pinned them without one"
        ),
        note="An upstream type change would otherwise re-infer every dependent `data_type` unseen.",
    ),
    spec(
        "SST-DBT025",
        Severity.INFO,
        "dbt seam summary",
        "{value}",
        None,
        condition="a run reads the dbt seam; once, counting models and sources read, refs resolved, columns checked",
    ),
    spec(
        "SST-DBT026",
        Severity.ERROR,
        "dbt packages are not installed",
        "{count} package(s) in packages.yml are not installed",
        "run dbt deps",
        condition="`packages.yml` declares packages that are absent from the packages install path",
    ),
    spec(
        "SST-DBT027",
        Severity.ERROR,
        "dbt could not be invoked",
        "dbt could not be executed: {detail}",
        "install dbt, or pass --manifest to read a manifest that already exists",
        condition=("the dbt executable is absent from PATH or is not runnable, so no exit status is ever produced"),
    ),
    spec(
        "SST-DBT028",
        Severity.ERROR,
        "`dbt parse` exited non-zero",
        "dbt parse exited {found}",
        "read dbt's own output above; the failure is in the dbt project",
        condition="the parse subprocess ran and reported failure",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-DBT029",
        Severity.ERROR,
        "`dbt parse` reported success but wrote no manifest",
        "dbt parse exited 0 and {path} was not written",
        "check --project-dir and dbt's target-path; dbt wrote its manifest elsewhere",
        condition="the parse subprocess exited zero and the expected manifest is absent or unreadable",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-DBT030",
        Severity.ERROR,
        "Forbidden `meta.sst` location key",
        "model '{model}': meta.sst.{key} is forbidden -- delete it",
        "delete the key; the location comes from dbt's own `+database` / `+schema` and SST reads the RESOLVED "
        "value out of the manifest",
        condition=("a model's `meta.sst` sets `database` or `schema`, which 0.3 accepted and ignored; once per key"),
    ),
    spec(
        "SST-DBT031",
        Severity.WARNING,
        "Model has no relation to enrich",
        "model '{model}' has no relation, so sst enrich has no columns to read",
        "materialize the model as a table or a view; an ephemeral model has nothing to enrich",
        condition="a model selected for sst enrich by name produces no relation (ephemeral)",
    ),
    spec(
        "SST-DBT032",
        Severity.ERROR,
        "Key metadata is written in the 0.3 form",
        "model '{model}': meta.sst.{field} is written in the 0.3 form",
        "write primary_key as a list of columns and unique_keys as a list of column lists",
        condition="a model writes meta.sst.primary_key / unique_keys in the 0.3 value form, once per field",
    ),
)
