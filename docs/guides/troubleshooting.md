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

`SST-PLN024` means the object already exists and SST did not publish it. SST
will not change it, and has no command to take it over. Drop it by hand, or
rename the artifact, before applying. For a
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
- Check `+database` / `+schema` for the artifact type and any folder route in
  `sst_config.yml`.
- A tool from a `reference:` group resolves through its `relations:` map; a
  target the map does not cover is an error rather than a fall-through.

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
  names the cause: the skill has its own errors, or `skills.catalog` is missing
  or incomplete.
- **`SST-VAL838`**: the reference sets `version:`. Remove it; SST pins project
  skills itself.

## A skill loads but cannot find its own files

Agents read supporting files only beside `SKILL.md`, which is why SST flattens
the bundle. Check the validation output for the file:

- `SST-VAL808`: a referenced file does not exist;
- `SST-VAL810`: a path written from the repository root, or a link to a
  directory, which cannot survive flattening;
- `SST-VAL833`: a script names a path flattening renamed.

## A profile does not appear in CoCo Desktop

- **`SST-VAL854`** is reported: the project publishes to a registry table other
  than the one Desktop reads. That is right for rehearsal; point
  `skills.stage.+registry_table` at Desktop's table to publish for real.
- The profile was deactivated by a `--prune` run after its folder was deleted.
- A pointer in the row does not resolve. `apply` checks every pointer after the
  write and reports the one that failed.

## `apply` refuses the plan

`sst apply --plan <file>` plans afresh and applies the saved plan only if the two
agree; otherwise it exits 4 and names the reason.

- **`SST-APL005`**: the plan was made for another target.
- **A different manifest**: the project no longer compiles to the manifest the
  plan was made from.
- **The live observation changed**, or the ordered changes or their statement
  hashes did: something in Snowflake moved since the plan read it.

Run `sst plan` again and apply the new plan. A saved observation more than an
hour old is ignored and re-read (`SST-MAN030`). Once the run holds the lock, a
change to state since the plan read it is `SST-APL012`. For `SST-APL011`, see
[the next entry](#another-run-holds-the-lock-sst-apl011).

## A warning started failing the build

`--strict` and `validation.strict: true` promote every warning to an error. Each
is printed as `error[...] (promoted from warning)`, and the JSON envelope's
`summary.promoted` counts them. A warning the baseline holds stays baselined and
does not block. A run narrowed by `--select` or `--exclude` promotes no warning
about an artifact it leaves out; a warning that names no artifact, such as one
about configuration or a tool group, is promoted whatever the selection.

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
- **`SST-REF043`**: an expression's `ref('model')` names a model that is not in
  the member's own `tables:`. Add the model there, or reference one of them.

## SST cannot connect, or connects the wrong way

`sst debug` shows the authentication method SST resolved from `profiles.yml`.

- **`SST-CFG048`**: a field SST does not read, often a misspelled credential
  field such as `private_key_pth`.
- **`SST-CFG049`**: a template other than `env_var()`, such as `| as_number`, or
  a number or boolean that is not one.
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

`apply`, `drop`, and `test --suite evals` take a run lock on the target: a row in
the lock table beside the state table (`SST_STATE_LOCK` by default) and a file
beside the local state cache, `target/sst/state.<target>.json.lock`. A live run
extends its lock every 5 minutes; a lock not extended for 30 minutes has
expired.

- **Another run is live.** The message names it, with its role, host, and expiry.
  Wait for it to finish.
- **A run was killed and left its lock.** Once the lock has expired, rerun with
  `--break-stale-lock`. The run takes the lock over and warns `SST-APL010` naming
  the run it replaced. Without the flag an expired lock is still refused, and
  with it a lock that has not expired is refused all the same, so after a crash
  wait out the 30 minutes. Check first that the named run really is gone: a
  broken lock means it did not exit cleanly.

There is no command that clears a live lock: a lock is only ever released by its
own run, or taken over once it has expired.

## An apply was interrupted or killed

Ctrl-C, `SIGTERM` (a CI cancellation, `timeout`, `docker stop`), and `SIGHUP` all
end a run the same way: `apply` starts no further change, waits for those
running, records in state the ones that finished, releases both locks, and exits
130 with `SST-PRT107`. A further `SIGTERM` or `SIGHUP` is ignored while it does
this; a second Ctrl-C stops the running changes at their next statement. Run
`sst plan` to see what remains; what was recorded plans as no change.

`SIGKILL`, or a machine that went away, cannot be handled:

- **The lock stays held** until it expires; see
  [the previous entry](#another-run-holds-the-lock-sst-apl011).
- **An object it updated but did not record** still carries SST's ownership
  marker for the definition being planned. `plan` updates it, with reason
  `unrecorded_publish`, and the next apply records it. Apply again before you
  change that artifact: once its definition moves on, the marker no longer
  vouches for it and it blocks as below.
- **An object it created** has no entry in state, so `plan` blocks it as
  unmanaged (`SST-PLN024`), as it would any object SST did not publish. Drop it
  by hand (`DROP <type> <name>`, with a role that may), then apply again.
- **An object it updated that someone then changed again** blocks as changed
  out of band (`SST-PLN014`): restore the definition SST wrote, or drop it by
  hand.

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

## The eval suite is slow

An eval run takes minutes in Snowflake. The suite reads each run's status every 5
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
reports an expression or query Snowflake will not compile.

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
serves the latest certified version rather than the newest one. Publish with
`skills.+certified: true`, or certify the new version; a later certified version
has to be un-certified in Snowsight. Agents pin their version and are
unaffected.
