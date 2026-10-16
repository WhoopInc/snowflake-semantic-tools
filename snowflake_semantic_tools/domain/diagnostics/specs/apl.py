"""Apply codes (APL): a refusal or failure while SST executes changes and runs checks in Snowflake."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Apply"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-APL001",
        Severity.ERROR,
        "DDL execution failed",
        "{artifact}: {value} failed: {detail}",
        "fix the statement or the account as the error in the message says",
        condition=(
            "Snowflake rejected a statement `apply` or `drop` ran, or a change failed for a reason "
            "no other APL code names"
        ),
        note=(
            "The message's detail is the error itself; when SST recognises a Snowflake refusal it "
            "also reports it under its SST-SNO code."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL002",
        Severity.WARNING,
        "Skipped: a dependency failed",
        "{artifact} skipped: {blocker} failed",
        "fix the dependency, then re-apply",
        condition=(
            "a change this one depends on failed or was skipped, so it did not run; with "
            "`--fail-fast` the run instead stops at the first failure and skips every change left"
        ),
        note=(
            "The message names the change that failed, even when the dependency between them was "
            "only skipped. A dependent the plan had nothing to do for (NOOP) is skipped without it."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL003",
        Severity.ERROR,
        "Refused: artifact is blocked",
        "{artifact} is BLOCKED by {count} errors",
        "fix the errors, then re-apply",
        condition=(
            "the plan holds a blocked change, one carrying unresolved errors; apply then runs "
            "nothing, refusing the whole plan before it takes the lock"
        ),
    ),
    spec(
        "SST-APL004",
        Severity.ERROR,
        "Refused: replace would run without COPY GRANTS",
        "{artifact}: replace statement omits COPY GRANTS",
        "report this as a bug; SST renders COPY GRANTS on every such replace",
        demotable=False,
        condition=(
            "a statement about to replace an object whose grants survive only by COPY GRANTS does "
            "not say COPY GRANTS, so it would silently revoke consumer access"
        ),
        note=(
            "Nothing runs for the change. SST renders COPY GRANTS on every such replace it plans, "
            "so this points at a defect in SST rather than in the project."
        ),
    ),
    spec(
        "SST-APL005",
        Severity.ERROR,
        "Refused: plan was not computed for this target",
        "{artifact}: plan target '{found}' differs from the apply target '{expected}'",
        "re-run plan against the apply target",
        condition="the plan `apply --plan` was given was saved for another target than the run's",
    ),
    spec(
        "SST-APL006",
        Severity.ERROR,
        "Verification gate failed",
        "{count} published probes failed",
        "fix the metrics, then re-apply",
        demotable=False,
        condition=("one or more smoke queries failed in `sst test --suite smoke`, so the suite is not a success"),
        note=(
            "Smoke queries are not part of a deployment, so this can never fail an `apply`. Each "
            "failed probe is reported above it: SST-PLN100 for a metric, SST-APL100 for anything else."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL007",
        Severity.ERROR,
        "Tag application failed after a successful create",
        "{artifact}: tags could not be applied: {detail}",
        "grant the tag privilege, then re-apply",
        condition=(
            "the ALTER that tags a published skill or plugin version CERTIFIED failed, or the "
            "catalog did not report the version certified afterwards"
        ),
        note="When roles already hold grants on the extension, SST-APL021 is reported instead.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL008",
        Severity.WARNING,
        "Explicit grants could not be read for a replace",
        "{artifact}: its explicit grants could not be read",
        "check the grants by hand; if the change failed, re-apply once they can be read",
        condition=(
            "the explicit grants of an object an update replaces could not be read: before the "
            "replace, which fails the change with nothing written, or after it, which leaves the "
            "change applied with its grants unverified"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL009",
        Severity.ERROR,
        "Explicit grant did not survive a replace",
        "{artifact}: grant {value} was present before replace and is absent after",
        "confirm COPY GRANTS was emitted",
        demotable=False,
        condition=(
            "an explicit grant the object carried before a replace is absent after it, compared by "
            "privilege, grantee and grant option, so a changed grantor is no loss"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL010",
        Severity.WARNING,
        "Stale lock broken",
        "broke a stale lock held by {value}",
        "confirm no other run is live; a broken lock means the previous run did not exit cleanly",
        condition=(
            "`--break-stale-lock` was given and the target's run lock, in Snowflake or in the local "
            "cache, had expired, so this run took it over"
        ),
        note=(
            "The lock in Snowflake expires 30 minutes after its run last extended it, and a live "
            "run extends it every 5 minutes; the local lock goes stale 30 minutes after it was "
            "taken. The run that held the lock can no longer extend it or write state: should it "
            "still be running, it starts no further statement once its heartbeat finds the lock "
            "taken, and reports SST-APL011."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL011",
        Severity.ERROR,
        "Lock held by a live run",
        "{value} holds the apply lock",
        "wait for the other run to end; once its lock has expired, re-run with --break-stale-lock",
        condition=(
            "another run (`apply`, `drop`, or the eval suite of `sst test`) holds the target's run "
            "lock, in Snowflake or in the local cache; or the lock expired and "
            "`--break-stale-lock` was not given; or this run lost its lock while it ran"
        ),
        note=(
            "The message names the holder: its run id and, for the lock in Snowflake, the role and "
            "host it runs as and when its lock expires; for the local lock, the lock file, that "
            "it locks this checkout only, and when it goes stale. A live lock is never broken: "
            "wait for that run to end. Only once its lock has expired does `--break-stale-lock` "
            "take it over (SST-APL010). A run reads its lock again before each change and each "
            "write statement, so a run that lost its lock starts no further statement and writes "
            "no state."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL012",
        Severity.ERROR,
        "Object changed between plan and apply",
        "{artifact}: {value} changed since the plan",
        "re-run plan",
        condition=(
            "the target changed after the plan was computed: its state table no longer reads as the "
            "plan read it, an object the plan would create now exists, an ownership marker changed "
            "or was not installed, or a composite artifact's resources changed; `sst test` also "
            "reports it when an artifact it would smoke or eval is not published as the compiled "
            "manifest renders it"
        ),
        note="Under `sst test`, run `sst plan` and `sst apply` for the current manifest first.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL013",
        Severity.ERROR,
        "Write refused: target is a production-like environment under --temporary",
        "{artifact}: --temporary against target '{target}' is not permitted",
        "drop --temporary, or point at a scratch target",
        condition=(
            "a session-scoped artifact was requested against a prod-like target: one whose name, "
            "split on `_`, `-` and `.`, has a part `prod`, `production` or `prd`"
        ),
    ),
    spec(
        "SST-APL014",
        Severity.WARNING,
        "Temporary artifact shadows a permanent one",
        "{artifact} shadows a permanent object of the same name for this session",
        "remember the change is local",
        condition="an agent `apply --temporary` published has the name of a permanent agent the plan observed",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL015",
        Severity.WARNING,
        "alias ignored under --temporary",
        "{artifact}: alias is meaningless under --temporary and was ignored",
        "no action; the same committed file is reusable for a permanent apply",
        condition="an agent published by `apply --temporary` declares an alias",
    ),
    spec(
        "SST-APL016",
        Severity.ERROR,
        "Partial write left the artifact in an unusable state",
        "{artifact}: {detail}",
        "read the detail: when a statement failed, fix its cause and apply again; when what was "
        "written does not read back as rendered, restore it or drop the object by hand, then apply",
        condition=(
            "a composite publish (an eval, a skill or plugin, or a Desktop profile) wrote part of "
            "what it publishes, then a statement failed or what it wrote did not read back as "
            "rendered"
        ),
        note="State keeps the partial write, so the next plan publishes the artifact again.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL017",
        Severity.ERROR,
        "Per-channel publish reported aggregate success",
        "{artifact}: {detail}",
        "report per-channel status separately",
        condition=(
            "a Desktop profile's registry row was written, but a stage pointer it carries does not "
            "resolve to a tree the stage holds"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL018",
        Severity.ERROR,
        "Registry pointer write failed after an upload",
        "{artifact}: uploaded {path}, pointer write failed: {detail}",
        "re-run apply; the publish is half complete",
        condition="a Desktop profile's trees were uploaded to its stage, then writing its registry row failed",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL019",
        Severity.ERROR,
        "Deletion of a removed bundle file failed",
        "{artifact}: '{path}' could not be removed from the published version",
        "re-publish from empty",
        condition="a published skill or plugin version still holds a file its bundle no longer has",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL020",
        Severity.ERROR,
        "Version name collided under a parallel deploy",
        "{artifact}: version '{value}' was minted twice",
        "retry; derive versions from the content SHA",
        condition=(
            "ADD VERSION on a skill or plugin failed because the version name already exists: "
            "another deploy published it between the plan and this statement"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL021",
        Severity.ERROR,
        "Grant issued before certification completed",
        "{artifact}: grants were issued and certification did not succeed",
        "revoke, certify, then grant",
        condition=(
            "certifying a published skill or plugin version failed while roles already hold "
            "grants on the extension, so they reach a version that is not certified"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL022",
        Severity.ERROR,
        "Dataset version could not be added",
        "dataset '{artifact}': ADD VERSION failed: {detail}",
        "check OWNERSHIP on the dataset",
        condition="Snowflake refused the statement that creates an eval's dataset or adds its version",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL023",
        Severity.ERROR,
        "Eval run failed",
        "eval '{artifact}': {detail}",
        "fix what the detail names, then re-run the eval",
        condition=(
            "an eval could not run, or one of its attempts failed: it has no run configuration; "
            "its agent version could not be read or its config could not be staged; "
            "EXECUTE_AI_EVALUATION refused to start the run; its status could not be read (a read "
            "failed for good, or 5 reads in a row failed in transit); it did not reach a terminal "
            "status within 20 minutes; or it ended FAILED, CANCELLED, or in a status Snowflake does "
            "not document"
        ),
        note=(
            "The detail carries Snowflake's error or the run's status details. A missing run "
            "privilege is caught before any run starts (SST-VAL727, SST-VAL728), and a wrong "
            "config stage format too (SST-VAL729). A run that ends partially completed reports "
            "SST-APL024, and a completed run whose results cannot be read SST-SNO001. When a retry "
            "(the eval's `retry` setting) completes in its place, the attempt reports "
            "SST-APL029 instead; a CANCELLED run is not retried."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL024",
        Severity.WARNING,
        "Eval run reported a partial terminal status",
        "eval '{artifact}': status '{found}'",
        "treat this as a failure and re-run",
        condition=(
            "an attempt still read PARTIALLY_COMPLETED or INVOCATION_PARTIALLY_COMPLETED 3 minutes "
            "after it first read so, or at the 20-minute deadline, and `run.accept_statuses` does "
            "not name that status"
        ),
        note=(
            "SST already counts the attempt as not passing: although this is a warning, the eval "
            "is not accepted, and the eval suite fails, unless another attempt completes; then "
            "SST-APL029 replaces this warning. A partial status `run.accept_statuses` names "
            "reports SST-VAL730, an error, instead."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL027",
        Severity.ERROR,
        "Metadata table absent or the wrong shape",
        "{value}: {detail}",
        "re-run sst extract, then apply",
        condition=(
            "the Desktop profile registry table SST created is absent afterwards, or lacks the "
            "columns Desktop's query reads"
        ),
        note=(
            "There is no `sst extract` command in 1.0: check that the role may create the table in "
            "the registry's schema and that nothing else defines it with other columns, then re-apply."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL028",
        Severity.ERROR,
        "Eval config stage has the wrong FILE FORMAT",
        "eval config stage '{value}': FILE FORMAT is {found}, expected {expected}",
        "run one ALTER STAGE to set the required FILE FORMAT; SST will not alter a stage it did not create",
        condition=("`apply` found an existing eval config stage that does not carry the file format eval runs read"),
        note="`sst test` reports the same stage as SST-VAL729 before any eval run starts.",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-APL029",
        Severity.WARNING,
        "Eval attempt did not pass and was retried",
        "eval '{artifact}': run '{value}' ended {found} and was retried: {detail}",
        "the retry's results were used; if this recurs, investigate the run's non-determinism",
        condition=(
            "an eval attempt failed, ended partial, or its status or results could not be read, "
            "and later attempts completed every run the eval needed: one, or a baseline "
            "capture's `baseline_runs`"
        ),
        note=(
            "It replaces the error or warning the attempt reports on its own (SST-APL023, "
            "SST-APL024, SST-SNO001), and the gate judges only completed attempts whose results "
            "were read."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL100",
        Severity.ERROR,
        "Smoke query failed",
        "{artifact}: smoke probe failed: {detail}",
        "fix the artifact the probe names; a created view is not a working view",
        condition=(
            "a smoke probe of a view, a verified query, or a DESCRIBE of a published artifact "
            "failed, observed by `sst test --suite smoke` rather than by `apply`"
        ),
        note="Fix the artifact the probe names; a metric probe reports `SST-PLN100`.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL103",
        Severity.ERROR,
        "Connection to Snowflake lost mid-run",
        "the connection to Snowflake was lost mid-run; finished before it: {value}",
        "run sst plan, which shows what the run left to do, then apply again",
        condition=(
            "Snowflake ended a session apply was running on after the run took its lock: the "
            "network dropped, or the session was aborted or expired (error 390111 or 390112)"
        ),
        note=(
            "Apply opens one fresh connection to record in state the changes that finished and to "
            "release its lock, so the target is not left locked. A change whose statements had "
            "not returned is not recorded as written; the next plan sees what it left behind."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-APL105",
        Severity.INFO,
        "Eval of a temporary agent skipped",
        "{artifact}: skipped, because --temporary publishes {value} as a temporary agent",
        "apply without --temporary to publish the eval with its agent",
        condition=(
            "`apply --temporary` publishes an eval's agent as a temporary agent, which ends "
            "with the run's session, so the eval is neither published nor recorded"
        ),
        note="Only agents are temporary: the views, tools and skills the same run publishes are permanent.",
    ),
    spec(
        "SST-APL900",
        Severity.ERROR,
        "Outcome count does not match change count",
        "applied {found} outcomes for {expected} changes",
        "report this as a bug",
        condition="apply did not account for every planned change",
    ),
)
