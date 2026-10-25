# Troubleshooting

Symptoms first, then the causes that produce them. Every code below has an entry
in the [error code reference](../reference/error-codes.md), with its fix.

## `dbt parse failed` before anything else runs

SST runs `dbt parse --project-dir <project> --profiles-dir <dir>` itself, where
`<dir>` holds the first `profiles.yml` found in `--profiles-dir` (or
`$SST_PROFILES_DIR`), `$DBT_PROFILES_DIR`, the project root, and `~/.dbt`, in that
order. dbt's own output is printed above `SST-DBT028`.

- **No `profiles.yml` is found** (`SST-CFG009`, which lists every place searched).
  Put one in one of those places, or run `dbt parse` yourself and pass the
  manifest with `--manifest target/manifest.json`.
- **Packages are not installed** (`SST-DBT026`). Run `dbt deps` first.
- **`SST-DBT017`.** The manifest's schema version is not the one SST supports;
  use a dbt version that writes manifest schema v12, or pass
  `--allow-unsupported-manifest-schema` for one run.
- **`SST-DBT005`.** A model file changed after the manifest passed with
  `--manifest` was written. Regenerate it, or drop `--manifest` and let SST run
  `dbt parse`. `--allow-stale-manifest` reports it as a warning for one run, and
  the envelope's `invocation.overrides` records the flag; `--strict` still
  promotes that warning to an error.

## A metric or filter is missing from a view

Members attach by table membership: a member joins a view only when every table
in its `tables:` is also in the view's `tables:`.

- **`SST-MEM005`** says the member attached to no view at all. Compare its
  `tables:` with the views you expected.
- A **derived** metric has no `tables:`; SST infers them from the metrics it
  references (`SST-MEM002` when it cannot). It attaches where those do.
- The view may have `enabled: false`.

`sst compile --emit-ddl target/ddl/` shows exactly what each view contains.

## A query fails only when a particular field is involved

More than one relationship path joins two tables in the view, and nothing says
which to use. Validation reports it as `SST-VAL209` on the view and `SST-VAL116`
on each metric that crosses it. Add `using_relationships:` to those metrics.

## A number is too large by a round factor

- **Fan-out.** A metric over the many side of a relationship counted once per
  matching row. Check which table the metric's `tables:` names and which side of
  each relationship it is on.
- **A balance summed over time.** A snapshot metric, such as inventory or
  supply cost per month, summed across months. Mark it with
  `non_additive_dimensions:` so it takes the last value instead.

## `plan` shows changes when nothing changed

- **Someone changed the object in Snowflake.** `plan` compares against what is
  live, so a manual edit shows up as an update. Apply to restore it, or bring
  the change into the project.
- **The dbt project changed.** A column description, relation, or type change in
  dbt changes every view that uses the model.
- **A different target.** `sst debug --target <name>` shows what a target
  resolves to.

A commit that changes nothing SST renders plans as no change.

## `plan` stops at an object SST does not manage

`SST-PLN024` means the object already exists and SST did not publish it. Plan
never changes it on its own. To hand it to SST, make the project's files match it
(`sst import` writes them from the live object) and record it with
`sst state adopt --select <artifact>`, which compares the two member by member
first; the next apply then writes SST's marker on it (see
[Importing](importing.md)). Otherwise drop it by hand, or rename the artifact,
before applying. For a
profile registry row, the same code protects rows another tool wrote.
`SST-PLN028` means a profile's registry row changed since SST last wrote it:
another writer is active on that row.

## `plan` blocks an agent on its spec stage

There is no need to create the `apply.agent_spec_stage` stage yourself: apply
creates it when it is absent, and the plan says so (`SST-PLN035`). The plan
blocks each agent that stages on it when it cannot be used:

- **`SST-PLN026`**: the stage exists, but is not an internal stage with
  server-side encryption. SST never alters a stage; point
  `apply.agent_spec_stage` at another one, or replace the stage yourself.
- **`SST-PLN008`**: the role lacks `CREATE STAGE` on the schema of the absent
  stage, or `READ` or `WRITE` on the existing one. Grant it, then plan again.

## It went to the wrong database or schema

- Run `sst debug` and check the database and schema the target resolves to.
- Check `+database` / `+schema` for the artifact type, any folder route in
  `sst_config.yml`, and the artifact's own `config:`. `sst list --long` prints
  where each artifact's `database` and `schema` came from: `config`,
  `route:<folder>`, `root`, or `default`.
- A tool member that writes `relations:` resolves through that map; a
  target the map does not cover is an error rather than a fall-through.

## Privilege problems

### Snowflake says an object does not exist or is not authorized

Snowflake answers the same way for an object that is absent and for one the
role may not see, so the message alone does not say which. Check in this order:

- **It is absent.** The artifact has not been applied to this target, or the
  dbt model has not been built there. `sst plan` shows the first as a create,
  and blocks a change that names a relation the target does not hold
  (`SST-PLN007`) rather than let apply fail on it (`SST-SNO003`).
- **It is somewhere else.** The target resolves to another database or schema
  than the one you looked in; see
  [the entry above](#it-went-to-the-wrong-database-or-schema). Under
  `--defer-target`, dbt models resolve to the other target's relations.
- **The role lacks `USAGE` on the database or the schema.** Without both, every
  object inside reads as absent, whatever the role holds on the object itself.
  Plan reports a database it cannot see as `SST-PLN011`, a warehouse as
  `SST-PLN012`, and a schema as `SST-PLN010` with `apply.create_schemas: false`;
  with it on, a schema it cannot see is one apply tries to create, and a refusal
  is `SST-APL039`.
- **The privilege is on a database role your role does not hold.** A grant to
  a database role reaches a session only once that database role is granted to
  the session's role, or to a role it inherits. `SHOW GRANTS TO ROLE <role>`
  lists the database roles a role holds, and `SHOW GRANTS ON <type> <name>`
  who holds the object.

Drawn from: [plan preflight](../../snowflake_semantic_tools/domain/plan/preflight.py), [preflight reads](../../snowflake_semantic_tools/app/preflight.py), [plan diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/pln.py), [Snowflake error diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/sno.py).

### It works in a worksheet and fails in an eval run

A session can draw on its secondary roles as well as its primary role, but a
task runs as the primary role alone, and an eval runs as tasks. A privilege
held only through a secondary role therefore works interactively and fails in
the run. `sst eval` checks `CREATE TASK`, `CREATE STAGE`, and
`CREATE FILE FORMAT` on each agent's schema before any run starts:
`SST-VAL727` when only a secondary role holds one, `SST-VAL728` when no role in
the session does. Grant them to the primary role, or to a role it inherits;
SST follows the role hierarchy, database roles included, from the schema's
grants, and does not see a grant made at the database or account level.

Drawn from: [eval privileges](../../snowflake_semantic_tools/app/evals/privileges.py), [eval diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/val_eval.py).

### How SST reports a missing privilege

`sst plan` reads what each write needs before deciding, and blocks a change it
can show Snowflake would refuse, so apply does not fail part-way:

| Code | What plan found | Grant |
|---|---|---|
| [`SST-PLN008`](../reference/error-codes.md#sst-pln008) | the primary role does not hold, directly, through a role it inherits, or through a database role, the `CREATE` privilege a create needs, nor own the schema; or, for an agent, `CREATE STAGE` for an absent spec stage; or no role in the session holds `READ` and `WRITE` on an existing one | that privilege on the schema (to the primary role, or a role it inherits) or stage |
| [`SST-PLN010`](../reference/error-codes.md#sst-pln010), [`SST-PLN011`](../reference/error-codes.md#sst-pln011) | a schema or database the plan writes to that does not exist or the role cannot see | `USAGE`, or create it |
| [`SST-PLN012`](../reference/error-codes.md#sst-pln012) | the profile's warehouse does not exist or the role cannot see it | `USAGE` on the warehouse |
| [`SST-PLN001`](../reference/error-codes.md#sst-pln001) | a read the plan needs failed, such as the state table or an object's grants | check the role and the connection |
| [`SST-VAL020`](../reference/error-codes.md#sst-val020) (info) | a check skipped because the role may not make its read | optional; see [operating SST](security.md#operating-sst) |

`SST-PLN008` reads the schema's (or stage's) grants. Snowflake authorizes a
`CREATE` statement by the primary role alone, so for a `CREATE` privilege it
climbs from each role holding it through `SHOW GRANTS OF ROLE` (and, only for
what no account role passes up, `SHOW GRANTS OF DATABASE ROLE`) until it
reaches the primary role, `CURRENT_ROLE()`, at most 16 roles up. A `CREATE`
privilege that only a secondary role in the session holds is still reported,
and the message says so: grant it to the primary role, or to a role it
inherits. When the climb stops at 16 roles first, the message says only that
no inheritance path from the primary role was found within that depth. With no
secondary role active (`CURRENT_SECONDARY_ROLES()` names none), plan skips the
climb and asks the session alone, since its roles are then the primary role's. `READ` and `WRITE`
on an existing spec stage are not `CREATE` privileges, and a secondary role
does authorize them, so for those plan asks whether any role holding them,
directly or through a database role, is in the session. An update is not
checked for its own `CREATE` privilege: it replaces an object the role already
owns.

Two warnings concern the people who use an agent rather than the role that
publishes it: `SST-VAL616`, a consumer without `USAGE` on an object a tool
calls, and `SST-VAL542`, one without `READ` on an extension the agent pins (see
[what people who use an agent need](security.md#what-people-who-use-an-agent-need)).
A statement Snowflake still refuses is reported with its cause:
`SST-SNO003` for an object that does not exist or is not authorized,
`SST-SNO004` for insufficient privileges, and `SST-PRT004` for a refusal
outside the changes apply makes.

Drawn from: [plan preflight](../../snowflake_semantic_tools/domain/plan/preflight.py), [preflight reads](../../snowflake_semantic_tools/app/preflight.py), [connector preflight](../../snowflake_semantic_tools/adapters/snowflake/connector/preflight.py), [connector session](../../snowflake_semantic_tools/adapters/snowflake/connector/session.py), [live-object checks](../../snowflake_semantic_tools/app/compile/agents/observe.py).

## A column that does not exist was not caught

- The expression uses a raw identifier instead of `{{ ref('model', 'column') }}`.
  A metric expression with one gets a warning (`SST-VAL110`), but whether the
  column exists is only checked when validation compiles against Snowflake. Use
  `ref()`, which is checked offline against the dbt manifest.
- The dbt manifest is stale. Let SST run `dbt parse`, or regenerate the file you
  pass to `--manifest`.

## An agent says a skill does not resolve

- **`SST-REF032`** or **`SST-REF036`**: the project has no skill or plugin by that
  name. Check the spelling against the folder name.
- **`SST-VAL856`**: the skill exists but has no version to publish. The message
  names the cause: the skill has its own errors, or no `+bundle_stage` resolves
  for it.
- **`SST-VAL838`**: the reference sets `version:`. Remove it; SST pins project
  skills itself.

## A skill loads but cannot find its own files

Every channel publishes the skill folder as authored, so a path that resolves from
the file that names it, or from the skill folder, resolves where the agent reads
the skill. Check the validation output for the file:

- `SST-VAL808`: a Markdown link names a file or folder the skill does not hold;
- `SST-VAL810`: a path written from the repository root, which the published
  skill has no repository around; write it from the skill folder;
- a path that walks out of the skill folder (`../../other-skill/x.md`) names a
  file the published skill does not carry; move the file into the skill.

## A profile does not appear in CoCo Desktop

- **`SST-VAL854`** is reported: the project publishes to a registry table other
  than the one Desktop reads. That is right for rehearsal; unset
  `coco_desktop_profiles.+registry_table` to publish where Desktop reads.
- **Desktop shows `Database 'CORTEX_CODE' does not exist or not authorized`.**
  The account has no registry yet, or none of the user's roles can read it.
  Publish once to create it, then grant readers USAGE on `CORTEX_CODE` and
  `CORTEX_CODE.CONFIG`, SELECT on the registry, and READ on the profile stage;
  see [Plugins and CoCo Desktop profiles](plugins-and-coco-desktop-profiles.md#how-a-profile-publishes).
- **Desktop lists the profile under another name.** Each row is named by the
  profile's name and its target's `coco_desktop_profiles.+name_suffix`, such as
  `sales-analyst-dev`.
- The profile was deactivated by a `--prune` run after its folder was deleted.
- A pointer in the row does not resolve. `apply` checks every pointer after the
  write and reports the one that failed.

## `apply` refuses the plan

`sst apply --plan <file>` plans afresh and applies the saved plan only if the two
agree; otherwise it exits 4 and names the reason.

- **`SST-APL005`**: the plan was made for another target.
- **`SST-APL012`, the compiled manifest changed**: the project no longer
  compiles to the manifest the plan was made from.
- **`SST-APL012`, the live observation changed**, or the ordered changes or
  their statement hashes did: something in Snowflake moved since the plan read
  it.

Run `sst plan` again and apply the new plan. A saved observation more than an
hour old is ignored and re-read (`SST-MAN030`). Once the run holds the lock, a
change to state since the plan read it is `SST-APL012`. For `SST-APL011`, see
[the next entry](#another-run-holds-the-lock-sst-apl011).

## A warning started failing the build

`--strict` and `validation.strict: true` promote every warning to an error, except
the few codes the [error reference](../reference/error-codes.md) marks *never promoted*,
such as `SST-PLN042` for uncommitted files. Each
is printed as `error[...] (promoted from warning)`, and the JSON envelope's
`summary.promoted` counts them. A warning the baseline holds stays baselined and
does not block. A run narrowed by `--select` or `--exclude` promotes no warning
about an artifact it leaves out; a warning that names no artifact, such as one
about configuration, is promoted whatever the selection.

## `plan --prune` keeps listing a deleted skill, plugin, or eval

Those prunes are report-only (`SST-PLN034`, info): SST never removes them, so the
plan lists them until the objects are removed by hand. They never fail `--strict`.
While state has no entry for one, `plan` exits 2, because the apply that records
it still changes the state table; run `apply --prune` once so state records it,
and `plan` exits 0 from then on.

## A reference does not resolve

- **`SST-CFG029`**: `var()` names no project variable; declare it under `vars:`.
- **`SST-REF007`**: `custom_instructions()` names no custom instruction.
- **`SST-REF028`**: `tag()` names no tag declared under `tags:`.
- **`SST-REF029`**, **`SST-REF030`**, **`SST-REF031`**: `relationship()`,
  `filter()` or `verified_query()` names no declared member of that type.
- **`SST-REF003`**: a call's arguments are not quoted strings, as in
  `{{ ref(orders) }}`, or a field that takes exactly one call, such as a tag
  name, is something else.
- **`SST-REF004`**: the function is not a template function at all.
- **`SST-REF041`**: the function is not allowed in that field, such as
  `metric()` inside a filter.
- **`SST-REF015`**: a function has the wrong number of arguments.
- **`SST-REF033`**: a `{{ ... }}` span is not a call, such as `{{ name }}` or a
  call missing its closing parenthesis.
- **`SST-MEM002`**: a metric, filter or verified query names no model with
  `ref()`, so it has no tables and attaches to no view. Write its columns, or
  its SQL's tables, as `{{ ref() }}` calls.

## SST cannot connect, or connects the wrong way

`sst debug` shows the authentication method SST resolved from `profiles.yml`.

- **`SST-CFG048`**: a field SST does not read, often a misspelled credential
  field such as `private_key_pth`.
- **`SST-CFG049`**: a template other than `env_var()` (with or without dbt's
  `as_number`, `as_bool`, `as_native` or `as_text` filter), such as
  `{{ var() }}` or `{% if %}`, or a number or boolean field that holds something
  else.
- **`SST-CFG050`**: `oauth_client_id` and `oauth_client_secret` (use an access
  token in `token`), a key set both inline and as a file, or a key that cannot
  be read.
- **`SST-CFG051`**: `insecure_mode: true` turns off certificate revocation
  checks. It is a debugging switch; remove it.
- **`SST-PRT002`**: Snowflake rejected the credential; **`SST-PRT001`**: the
  connection failed for another reason, with the driver's message (credentials
  in it replaced by `<redacted>`). Both exit 5.
- **The command never returns.** See
  [A command hangs while signing in](#a-command-hangs-while-signing-in).

## Another run holds the lock (`SST-APL011`)

`apply`, `drop`, and `eval` take a run lock on the target: a row in
the lock table beside the state table (`SST_STATE_LOCK` by default) and a file
beside the local state cache, `sst_target/state.<target>.json.lock`. A live run
extends its lock every 5 minutes; a lock not extended for 30 minutes has
expired.

- **Another run is live.** The message names it, with its role, host, and expiry.
  Wait for it to finish.
- **A run on this machine was killed and left its lock.** Just apply again: the
  local lock file records the host and process that took it, so a run that finds
  that process gone releases both of its locks first and warns `SST-APL010`. The
  lock row in Snowflake is released only while it names that run and this host.
- **A run elsewhere was killed, or its machine went away.** Once the lock has
  expired, rerun with `--break-stale-lock`; the run takes it over and warns
  `SST-APL010` naming the run it replaced. Without the flag an expired lock is
  still refused, and with it a lock that has not expired is refused all the same.
  To clear it at once, check the named run really is gone, then release it:
  `sst state unlock --target <target> --run <run id> --yes` (`SST-APL040`). It
  releases only that run's lock, so a lock another run took meanwhile is kept
  (`SST-APL041`).

## An apply was interrupted or killed

Ctrl-C, `SIGTERM` (a CI cancellation, `timeout`, `docker stop`), and `SIGHUP` all
end a run the same way: `apply` starts no further change, waits for those
running, records in state the ones that finished, releases both locks, and exits
130 with `SST-PRT107`. A further `SIGTERM` or `SIGHUP` is ignored while it does
this; a second Ctrl-C stops the running changes at their next statement. A
signal the process inherited as ignored stays ignored: under `nohup`, closing
the terminal does not interrupt the run. Run `sst plan` to see what remains;
what was recorded plans as no change.

`SIGKILL`, or a machine that went away, cannot be handled:

- **The lock stays held** until it expires, the next apply on the same machine
  releases it, or `sst state unlock` does; see
  [the previous entry](#another-run-holds-the-lock-sst-apl011).
- **An object it updated but did not record** still carries SST's ownership
  marker for the definition being planned. `plan` updates it, with reason
  `unrecorded_publish`, and the next apply records it. Apply again before you
  change that artifact: once its definition moves on, the marker no longer
  vouches for it and it blocks as below.
- **An agent** has its marker written before anything else of its publish, so
  the marker means SST was writing that definition, whether or not the new
  version was committed. When the newest committed version already holds the
  planned spec (its comment carries `sst-spec:<fingerprint>`), the next apply
  points the alias and the live version at it and sets the agent's properties,
  without committing another version. When it does not, the next apply commits
  the spec once. Either way, apply again before you change the agent.
- **An object it created** has no entry in state, so `plan` blocks it as
  unmanaged (`SST-PLN024`), as it would any object SST did not publish. Drop it
  by hand (`DROP <type> <name>`, with a role that may), then apply again.
- **An object it updated that someone then changed again** blocks as changed
  out of band (`SST-PLN014`): restore the definition SST wrote, or publish the
  planned one over it with `sst apply --select <artifact> --overwrite`.

## `SST-PLN014`: an object was changed out of band

The message says what changed: the comment that carries SST's ownership marker,
a marker naming another publish, the name, or a property SST published, such as
an agent's specification, with the version that holds the hand edit. To keep
the edit, put it in the project and apply. To discard it, run
`sst plan --select <artifact> --overwrite`, review the update (`SST-PLN043`
names what it replaces), then `sst apply --select <artifact> --overwrite`. An
agent keeps its versions: a hand-committed version stays, and SST's
specification is committed after it, with the alias and default on it. Plan
exits 1 while anything is blocked, and its verdict counts them:
`plan failed (1 blocked)`.

## `sst apply` or `sst baseline add` refuses with `SST-PRT109`

Both ask before they write, on stderr, and only when stdin and stderr are both
terminals: a question written into a redirected log, or answered from a pipe,
would wait for ever. Off a terminal they refuse at once, before planning or
connecting, and say so. Pass `--yes` in a script or a CI job.

## A command hangs while signing in

`authenticator: externalbrowser` signs in through a browser. The Snowflake
driver prints the sign-in URL (on stderr; SST keeps stdout for its own output),
opens a browser, and waits for it to call back, without a time limit. In a shell
with no one at the browser -- CI, a remote session, a script, an agent -- that
wait never ends; when no browser can open, the driver asks for the redirected
URL on the terminal instead. `sst validate` and `sst debug` connect by default,
so both can wait like this. Press Ctrl-C (exit 130).

- For a job, use key-pair authentication (see
  [authentication](configuration.md#authentication)).
- To inspect configuration without signing in, run `sst debug --no-connect`;
  to validate without signing in, add `--no-snowflake-syntax-check`.
- On macOS and Windows the driver caches the sign-in token after one
  interactive sign-in, when the account allows it, and later connections reuse
  it until it expires. On Linux it does not, because SST does not pass
  `client_store_temporary_credential` (setting it is `SST-CFG048`).

## A plan with nothing to change still takes a minute

Most of a connected plan is many small reads, each a round trip to Snowflake.
Within one plan, SST makes each SHOW, DESCRIBE and LIST once and reuses the
answer. It looks up an object by name in one listing of its schema, and checks
a table exists with one `INFORMATION_SCHEMA` read per schema. Nothing is reused
from one command to the next, and `apply` reads afresh before it writes.

A plan reads on 4 sessions at once by default, as `apply` applies. `--threads`
(else `$SST_THREADS`, else `generation.threads`) sets how many it runs the
validation checks, the listings and the reads of each listed object, and the
preflight reads on; `--threads 1` reads on one session at a time. The plan, its
diagnostics and the saved plan file are the same for any count.

## `sst eval` is slow

An eval run takes minutes in Snowflake. `sst eval` reads each run's status every 5
seconds and prints progress to stderr (see [Running](evals.md#running)): a line
when an attempt starts, when its status changes, and when it ends, and a line
naming the runs still in flight at most once a minute. What bounds it:

- **Per attempt**, 20 minutes, then one last status read of up to 30 seconds. A
  partial status adds a wait of up to 3 minutes inside that bound.
- **Per eval**, `1 + retry` attempts; `evals.+retry` defaults to 0.
- **Per suite**, runs go `--threads` at a time (else `$SST_THREADS`, else
  `generation.threads`, else 1), or `concurrency` at a time for evals that set
  it, so a suite of ten evals at the defaults takes ten runs back to back.

A run that reaches the 20-minute bound fails as `SST-APL023`, naming the last
status read. Raise `--threads` or `evals.+concurrency` to overlap runs, and keep
the dataset to the questions the gate needs.

## An eval stays `PARTIALLY_COMPLETED`, or reports `SST-APL024`

Snowflake can report `PARTIALLY_COMPLETED` for up to a minute while it finalizes
a run that then reads `COMPLETED`. The suite therefore waits up to 3 minutes for
a partial status to change before accepting it, and prints the wait once:
`PARTIALLY_COMPLETED after 6m55s (waiting up to 3m00s for it to settle)`. A run
that turns `COMPLETED` meanwhile is kept, with no retry. A partial status that
holds for the 3 minutes, or is still read at the 20-minute bound, ends the run:
as `SST-APL024`, a warning, and the attempt is retried when `retry` allows. Listing
the status in the config's `run.accept_statuses` does not make it a pass; it is
then `SST-VAL730`, an error. Look at the run in Snowsight before re-running.

## A status read fails during an eval

A read that fails on the network is retried at the next poll and printed as
`status read failed (1/5), retrying`. Only five failed reads in a row fail the
attempt (`SST-APL023`), and then only after one last read, so a run that
completed while the network was down is still kept.

A read that times out (`status read timed out, polling until the deadline`)
most often means Snowflake answered too slowly because the warehouse is busy
with the runs' own judges. It does not count toward the five:
polling goes on until the run's 20-minute deadline. If many runs share a small
warehouse, pace them (`evals.+concurrency` or `--threads`) or let the warehouse
scale out (`MAX_CLUSTER_COUNT` above 1). A run SST gives up on is canceled in
Snowflake; when the cancel fails, the `SST-APL023` message names the run, so
you can cancel it yourself with `EXECUTE_AI_EVALUATION('CANCEL', ...)`.

## A baselined warning is shown, or the baseline will not prune

A baseline entry matches a diagnostic by its fingerprint alone, so it
suppresses the diagnostic in any run, offline or against any target.

- **A warning that only a connected validate reports**, such as `SST-VAL415`
  (a verified query returns no rows) or `SST-VAL212` (cardinality disagrees with
  the data), is not seen by an offline `sst baseline add`. Baseline it
  with `sst baseline add <CODE> --target <name>`, which runs the connected
  validate against that target and records the target in each entry it adds.
- **`sst baseline prune` keeps a connected entry.** An offline prune cannot tell
  whether a connected entry is stale, so it keeps it. Run
  `sst baseline prune --target <name>` for the target the entry records;
  `sst baseline show` lists each entry as `offline` or `connected:<target>`.
- **The baseline expired.** Past its date it matches nothing and every warning
  it held blocks again. `sst baseline renew` moves the date.
- **An artifact was renamed or its file moved.** An entry is keyed on the
  artifact's name and file, so its warning comes back under the new name.
  When it blocks, `SST-INT011` names the entry it may have been:
  run `sst baseline add <CODE>` for the new name, then `sst baseline prune` to
  drop the old entry.

The summary counts baselined warnings apart, as `1 warning (3 baselined)`;
`--show-baselined` prints them.

## An expression or verified query is refused (`SST-VAL418`)

Before anything renders it, SST checks that each authored expression is one
expression and each verified query one `SELECT` or `WITH` query (see
[how SST builds SQL](security.md#how-sst-builds-the-sql-it-sends)). The
message names the reason and quotes the text from where it starts, such as
`';' ends the statement at '; DROP ...', so SST will not send it to Snowflake`;
it reads `expression failed to compile` even though nothing was sent. Offline,
this is the only cause of `SST-VAL418`; under connected validation it also
reports an expression or query Snowflake will not compile. A verified query is
compiled in its view's schema, or on the plain session while that schema does
not exist yet, before the first apply creates it.

## An allowed orchestration model fails the check (`SST-VAL551` to `SST-VAL556`)

`CREATE AGENT` accepts any model name, and Snowflake keeps no list of the
models agents accept, so the connected check runs each model
`snowflake.orchestration_models` lists, `auto` aside, in a throwaway agent:
TEMPORARY, no tools, a small budget, created where the first agent the project
publishes makes its temporary agents (`agents.+scratch_schema`, else its own
schema), and dropped once it answers. Before the first apply creates an agent's
schema, only a scratch schema that already exists lets the probe run; otherwise
it is `SST-VAL556`. `SHOW CORTEX BASE MODELS`
adds each model's lifecycle.

- **`SST-VAL551`**: the run refused the model. "not an allowed model for Agent
  requests" means agents never accept it, such as `llama3.1-8b`; "not
  authorized or not available in your region" means the account cannot reach
  it, such as an embedding model, a retired one, or a misspelling. Remove it
  from the list.
- **`SST-VAL552`**: the model ran, but Snowflake marks it `INTERNAL` or does
  not list it. Snowsight may still offer it; it can go without notice.
- **`SST-VAL553`, `SST-VAL554`, `SST-VAL555`**: end-of-life (an error),
  legacy, or preview.
- **`SST-VAL556`**: nothing was learned, because the project publishes no
  agent, or the role may not create one in that schema, or the run did not
  answer within 60 seconds, or the SHOW failed. A model that times out once
  may answer on the next run; one that keeps timing out is not one to rely on.

Run the check as the role that deploys. ACCOUNTADMIN reaches models that
other roles cannot, so a check it passes says little about theirs.

The synonym model `sst enrich` uses is checked apart: connected validate
reports its lifecycle (`SST-VAL557`), and enrich asks it for one token before
it reads anything, stopping on a refusal (`SST-SNO032`).

## A column carries a governance tag its YAML does not record (`SST-VAL338`)

With `governance.read_column_tags: true` and `governance.sensitive_tags` naming at least one
tag, connected `sst validate` reads the column tags of each table a view reads, once per table,
beside its `DESCRIBE`. A column carrying a sensitive tag, set on it or inherited from its table,
schema or database, is a warning when its YAML has neither `pii_tags` nor the tag in
`meta.sst.governance_tags`. A column the YAML does not describe yet is reported too; an excluded
column, and a table read through `source()`, are not.

Run `sst enrich` on the model with the same settings: it records each sensitive tag it finds
in `meta.sst.governance_tags`, and stops sampling the column. Or add `pii_tags` to the column by
hand. A table whose tags cannot be read is reported as skipped (`SST-VAL020`) rather than
checked. Offline, `sst validate` reports sample values left on a column the YAML marks sensitive
(`SST-VAL328`); a view never publishes them.

## A file name is refused

**`SST-VAL857`**: a stage accepts only letters, digits, `.`, `_`, `-`, and `$` in
a file or folder name, so SST checks every skill, hook, and command file before
uploading anything. Rename the file; a plugin whose member has such a file is
blocked too (`SST-VAL836`).

## A configured directory is reported missing

**`SST-CFG047`**: a `project.*_dir` key names a directory that does not exist.
Without this check SST would find nothing there, and `--prune` would remove
everything that directory used to publish.

## I need to ship while one artifact is broken

Run `compile`, `plan`, and `apply` with `--partial`. Everything without errors,
and not depending on anything with errors, goes ahead; each artifact left out is
listed (`SST-PLN032`), and the command still exits 1. A configuration error
still stops the run, and `--partial` cannot be combined with `--prune`.

## The catalog still offers an older skill version

**`SST-VAL841`**: once any version of an extension is certified, the catalog
serves the latest certified version rather than the default, which apply
otherwise points at the version the project declares. Publish with
`certified` resolving to true for the skill -- `skills.+certified: true`, a
folder route, or the skill's own `config: {certified: true}` -- or certify the
new version; a later certified version
has to be un-certified in Snowsight. Agents pin their version and are
unaffected.
