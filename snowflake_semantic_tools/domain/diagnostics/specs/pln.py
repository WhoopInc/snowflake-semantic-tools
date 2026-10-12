"""Planning codes (PLN): observation, preflight, ownership, drift, order, prune, smoke, and --partial."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Planning"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-PLN001",
        Severity.ERROR,
        "Observation query failed",
        "observation of {value} failed: {detail}",
        "check the connection and the role, then re-run plan",
        condition=(
            "a read of the target failed: an object listing, grants, the state table, "
            "a preflight read, or a composite artifact's resources; under `--use-cached-state`, also "
            "each composite artifact, which a recorded observation cannot decide. A definition "
            "`--capture-prior` cannot read is SST-PLN037 instead"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN002",
        Severity.ERROR,
        "An object of a different type exists at the target name",
        "{artifact}: {value} exists as a {found}",
        "rename the artifact, or drop the existing object",
        condition="the target name is occupied by another object kind",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN003",
        Severity.WARNING,
        "Prune candidate has no ownership marker",
        "{value} has no SST ownership marker; skipped",
        "drop it by hand if it is no longer wanted; SST prunes only objects carrying its marker",
        condition=(
            "under `--prune`, a live object of a prunable type that the project no longer "
            "declares carries no SST ownership marker, so it is not pruned"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN004",
        Severity.WARNING,
        "Marked prune candidate is absent from local state",
        "{value} carries an SST marker and is absent from state; skipped",
        "reconcile state, or delete it by hand",
        condition=(
            "under `--prune`, a candidate carries an SST ownership marker, but state has no entry "
            "for it, or its entry's manifest, fingerprint, DDL hash or name disagrees with the "
            "marker, so it is not pruned"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN005",
        Severity.ERROR,
        "Dependency cycle among artifacts",
        "artifact dependency cycle: {cycle}",
        "break the cycle",
        condition="the planned changes' dependencies form a cycle, so no change can be ordered",
    ),
    spec(
        "SST-PLN006",
        Severity.WARNING,
        "Impact-scoped plan requested with no previous manifest",
        "no previous manifest; a full plan was computed",
        "commit the manifest, or pass --state",
        condition=(
            "`state:modified` was selected without a previous manifest: no `--state`, or a "
            "`--state` directory holding no `manifest.json`, so the plan covers every artifact"
        ),
    ),
    spec(
        "SST-PLN007",
        Severity.ERROR,
        "Referenced object does not exist in the target",
        "{artifact} references {value}, absent from target '{target}'",
        "publish it first, or correct the reference",
        condition="a planned create or update references a relation the target does not hold",
        note="Plan blocks the change rather than let apply fail with SST-SNO003.",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN008",
        Severity.ERROR,
        "Deploying role lacks a required privilege",
        "{value} lacks {detail} on {target}",
        "grant the privilege to the session's primary role, or to a role it inherits",
        condition=(
            "the session's primary role does not hold, directly, through a role it inherits, or "
            "through a database role, the CREATE privilege a planned create needs on its schema, "
            "such as CREATE SEMANTIC VIEW, nor own the schema; or, for an agent create or "
            "update, CREATE STAGE on the schema of an absent `apply.agent_spec_stage` stage; or no "
            "role in the session holds READ and WRITE on an existing one"
        ),
        note=(
            "Plan blocks the change rather than let apply fail with SST-SNO004. Snowflake authorises "
            "a CREATE by the primary role alone, so a CREATE privilege only a secondary role holds "
            "does not count, and the message says a secondary role holds it. An update is not "
            "checked for its own CREATE privilege: it replaces an object the role already owns."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN009",
        Severity.WARNING,
        "An object already exists at the target name",
        "{artifact}: {value} already exists",
        "drop or rename the existing object, or give the artifact another name",
        condition="the name a planned create writes to is held by an object of its type that observation did not list",
        note="Unless the name is free by then, apply refuses the create (SST-APL012).",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN010",
        Severity.ERROR,
        "Target schema does not exist",
        "schema {value} does not exist or is not authorised",
        "create the schema, or correct the target",
        condition=(
            "the schema a planned write targets does not exist, or the role cannot see it; reported once per schema"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN011",
        Severity.ERROR,
        "Target database does not exist",
        "database {value} does not exist or is not authorised",
        "create the database, or correct the target",
        condition=(
            "the database a planned write targets does not exist, or the role cannot see it; reported once per database"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN012",
        Severity.ERROR,
        "Warehouse does not exist or is not authorised",
        "warehouse {value} does not exist or is not authorised",
        "grant USAGE, or name an existing warehouse",
        condition="the warehouse the profile names does not exist, or the role cannot see it; reported once per plan",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN013",
        Severity.WARNING,
        "Live object carries grants that a replace would drop",
        "{artifact}: {count} explicit grants exist on {value}",
        "confirm COPY GRANTS is emitted before applying",
        condition="a planned update replaces an object that carries explicit grants",
        note="Apply checks every one survives the replace, and fails the change if not (SST-APL009).",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN014",
        Severity.WARNING,
        "Live definition differs from the last recorded state",
        "{artifact} was changed out of band; apply refuses it",
        "review the live object, then restore the one SST wrote or remove it before applying",
        condition=(
            "state records the artifact, but the live object's ownership marker is absent, or "
            "names another manifest or fingerprint than state records, or the object is under "
            "another name"
        ),
        note=(
            "The change is blocked, so `apply` refuses the plan (SST-APL003) until the live object "
            "is again the one SST wrote, although this code is a warning."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN015",
        Severity.INFO,
        "No change for an artifact",
        "{artifact}: NOOP",
        None,
        condition="the proposed definition matches the observation",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN016",
        Severity.INFO,
        "Plan summary",
        "{value}",
        None,
        condition=(
            "a plan reports its counts by artifact type, as `view: 1 create, 0 replace, 2 noop, 0 "
            "prune`, counting updates as `update` for a type that alters or re-versions in place, "
            "such as an agent; under `--use-cached-state` it first reports when the recorded "
            "observation was taken and how old it is"
        ),
    ),
    spec(
        "SST-PLN017",
        Severity.WARNING,
        "Prune would remove an object a consumer references by name",
        "{value} is referenced outside this project",
        "exclude it from prune, or migrate the consumer",
        condition="a prune candidate is named by something SST does not manage",
        note="What names an object is read from `SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES`, "
        "which needs IMPORTED PRIVILEGES on the `SNOWFLAKE` database and lags by up to three hours. "
        "A role without it skips this check for each prune candidate, reported as `SST-VAL020`, "
        "and the prune goes ahead: this is a warning, never what stops a prune.",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN018",
        Severity.ERROR,
        "Plan computed against a stale observation",
        "observation for {value} is {detail}",
        "re-run plan",
        condition="observing and planning in one run took longer than the 15-minute observation lifetime",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN019",
        Severity.WARNING,
        "Concurrent DDL detected on the target",
        "{value} is being modified by another session",
        "retry, or serialise the publishers",
        condition="the observation shows a competing writer",
        note="Reading other sessions' locks needs MONITOR on the account. A role without it skips "
        "this check, reported as `SST-VAL020`, and the plan goes ahead.",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN020",
        Severity.ERROR,
        "Smoke query could not be constructed",
        "{artifact}: no smoke query can be built for metric '{member}'",
        "make the metric selectable, or exclude it from smoke checks",
        condition="a public metric of a semantic view has no smoke probe, because no query can name it",
        note="Raised by `sst test --suite smoke`, which then runs no probe.",
    ),
    spec(
        "SST-PLN021",
        Severity.WARNING,
        "Prune reconciliation found objects the repo does not declare",
        "{count} objects in {value} are not declared here",
        "declare them in the project, or drop them by hand",
        condition=(
            "under `--prune`, a schema holds live objects of a prunable type that the project does "
            "not declare, compared case-folded; one per schema, counting every candidate whether "
            "or not it is pruned"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN022",
        Severity.ERROR,
        "Change order is not computable",
        "change order could not be computed: {detail}",
        "resolve the dependency conflict",
        condition="two planned changes carry artifact keys that are the same once case-folded",
    ),
    spec(
        "SST-PLN023",
        Severity.WARNING,
        "Declared name differs from the live object only by case",
        "{artifact}: declared '{value}', live object is '{found}'",
        "normalise the declaration to the rendered case",
        condition="an unquoted declared identifier matches a live object only after case folding",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN024",
        Severity.ERROR,
        "Existing object is not managed by SST",
        "{artifact}: {value} exists without trusted SST ownership",
        "drop or rename the object by hand, or give the artifact another name; SST never takes over "
        "an object state does not record",
        demotable=False,
        condition=(
            "an object holds the target name and state records no SST entry for it; `sst drop` "
            "also reports it for an object with no trusted SST ownership marker"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN025",
        Severity.ERROR,
        "Artifact target moved",
        "{artifact}: recorded target '{found}' differs from declared target '{expected}'",
        "move the object explicitly, then reconcile state and re-run plan",
        demotable=False,
        condition="state records the artifact (or its resources) at a target other than the one declared",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN026",
        Severity.ERROR,
        "Stage is not an internal SSE stage",
        "{artifact}: stage {value} is {found}, expected INTERNAL NO CSE",
        "point the configuration at an internal stage with SNOWFLAKE_SSE encryption; SST never alters a stage",
        demotable=False,
        condition=(
            "the skill bundle stage, the profile stage, or the `apply.agent_spec_stage` stage exists "
            "but is not INTERNAL with SNOWFLAKE_SSE encryption"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN027",
        Severity.ERROR,
        "Version alias holds different content",
        "{artifact}: version alias {value} holds files that differ from the bundle ({detail})",
        "an earlier publish left a damaged version under this alias; remove the alias in Snowflake, then plan again",
        demotable=False,
        condition="a version alias SST would reuse holds files that differ from the bundle",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN028",
        Severity.ERROR,
        "Registry row changed since SST last wrote it",
        "{artifact}: registry row '{value}' has VERSION {found}, and SST last wrote {expected}",
        "another writer changed the row; reconcile it, then plan again",
        demotable=False,
        condition="a profile-registry row carries a VERSION neither state nor this release names",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN029",
        Severity.ERROR,
        "Registry table has the wrong shape",
        "{artifact}: {value} {detail}",
        "SST never alters the registry; fix the table or point skills.stage at a compatible one",
        demotable=False,
        condition="the profile registry table lacks a column SST writes, or types it differently",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN030",
        Severity.ERROR,
        "Pinned version is not part of this plan",
        "{artifact}: pins the published version of {value}; select {value} as well",
        "plan the pinned artifact in the same run; when its version is already published it plans as NOOP",
        demotable=False,
        condition="a create or update pins the published version of an artifact this plan leaves out",
    ),
    spec(
        "SST-PLN032",
        Severity.INFO,
        "Excluded from a partial run",
        "{artifact} has errors, or depends on something that does, so this partial run leaves it unpublished",
        "fix the errors reported for it; what is live stays as it is, and state keeps its record",
        condition="under --partial, an artifact with errors (or depending on one) is left out of the plan",
    ),
    spec(
        "SST-PLN033",
        Severity.INFO,
        "Partial run cannot go ahead",
        "--partial publishes nothing: {found} on {value} cannot be traced to the artifacts it would change",
        "fix that error first; a configuration error, or an error in a semantic view member such as a metric, "
        "stops every run, because the views it belongs to would otherwise publish without it",
        condition=("under --partial, an error names no artifact, so nothing can be excluded and the run stops"),
    ),
    spec(
        "SST-PLN034",
        Severity.INFO,
        "Prune is report-only",
        "{artifact} is no longer declared; SST never removes {value}: {detail}",
        "the plan lists it until the objects are removed by hand; it is never a write, and the plan exits 2 only "
        "until an apply records it",
        condition=(
            "an artifact recorded in state is no longer declared; prune reports the resources SST "
            "keeps and never executes"
        ),
    ),
    spec(
        "SST-PLN035",
        Severity.INFO,
        "Apply creates the agent specification stage",
        "stage {value} does not exist; apply creates it, server-side encrypted, before it stages an agent there",
        "nothing to do; to manage the stage yourself, create it first as an internal stage with "
        "SNOWFLAKE_SSE encryption",
        condition=(
            "a planned agent create or update stages its specification on the `apply.agent_spec_stage` "
            "stage, which does not exist; reported once per stage"
        ),
        note=(
            "Apply runs `CREATE STAGE IF NOT EXISTS ... ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')` once per "
            "stage before its first upload, and never replaces, alters, or drops the stage. Plan "
            "blocks the agent instead when the role lacks CREATE STAGE on the stage's schema (SST-PLN008)."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN037",
        Severity.WARNING,
        "Prior definition not captured",
        "the prior definition of {value} was not captured: {detail}",
        "grant the role REFERENCES or OWNERSHIP on the object, or plan without --capture-prior",
        condition=(
            "under `sst plan --capture-prior`, reading the current definition of a live object a "
            "planned artifact names failed: GET_DDL refused it, or SHOW VERSIONS IN AGENT did"
        ),
        note=(
            "The plan is otherwise unaffected: the change is planned as without the flag, and its "
            "`prior_definition` is null."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN100",
        Severity.ERROR,
        "Smoke query failed",
        "{artifact}: smoke query for metric '{member}' failed: {detail}",
        "fix the metric; a created view is not a working view",
        condition=("a smoke probe of a metric failed (`sst test --suite smoke`); any other probe reports `SST-APL100`"),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-PLN101",
        Severity.INFO,
        "Smoke check skipped",
        "{artifact}: smoke check skipped ({detail})",
        None,
        condition=(
            "under `sst test --suite smoke --fail-fast`, an artifact whose probes did not run "
            "because an earlier probe failed"
        ),
    ),
    spec(
        "SST-PLN900",
        Severity.ERROR,
        "Change order is not a valid topological order",
        "change order violates {value}",
        "report this as a bug",
        condition="the computed order places a dependent before its dependency",
    ),
)
