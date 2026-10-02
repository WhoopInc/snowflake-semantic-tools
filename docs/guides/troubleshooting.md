# Troubleshooting

Symptoms first, then the causes that produce them. Every code below has an entry
in the [error code reference](../reference/error-codes.md), with its fix.

## `dbt parse failed` before anything else runs

SST runs `dbt parse --project-dir <project> --profiles-dir <project>` itself.

- **`profiles.yml` is not in the project root.** SST does not read
  `~/.dbt/profiles.yml`. Move or generate the file into the project, or run
  `dbt parse` yourself and pass the manifest with `--manifest target/manifest.json`.
- **Packages are not installed.** Run `dbt deps` first.
- **`SST-PRT007`.** The manifest's schema version is not the one SST supports;
  use a dbt version that writes manifest schema v12.

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
will not change it. Remove it, or rename the artifact, before applying. For a
profile registry row, the same code protects rows another tool wrote.
`SST-PLN028` means a profile's registry row changed since SST last wrote it:
another writer is active on that row.

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

- **The plan is stale.** The project no longer compiles to the manifest the plan
  was made from. Run `sst plan` again and apply the new plan.
- **`SST-APL011`.** Another apply holds the lock for this target. Wait for it, or
  use `--break-stale-lock` if that run no longer exists.

## A warning started failing the build

`--strict` and `validation.strict: true` promote every warning to an error. The
summary's `promoted` count says how many diagnostics that affected.

## `plan --prune` keeps listing a deleted skill, plugin, or eval

Those prunes are report-only (`SST-PLN034`, info): SST never removes them, so the
plan lists them until the objects are removed by hand. They never fail `--strict`.
Until state records them under the current manifest, `plan` exits 2, because the
apply that records them still changes the state table; run `apply --prune` once
so state records that the source is gone, and `plan` exits 0 from then on.

## A reference does not resolve

- **`SST-REF038`**: `var()` names no project variable; declare it under `vars:`.
- **`SST-REF039`**: `custom_instructions()` names no custom instruction.
- **`SST-REF040`**: `tag()` names no declared tag, or a tag name is not written
  as one `tag()` call.
- **`SST-REF041`**: the function is not allowed in that field, such as
  `metric()` inside a filter.
- **`SST-REF042`**: a function has the wrong number of arguments.
- **`SST-REF043`**: an expression refs a model that is not one of the view's
  `tables:`.

## SST cannot connect, or connects the wrong way

`sst debug` shows the authentication method SST resolved from `profiles.yml`.

- **`SST-CFG048`**: a field SST does not read, often a misspelled credential
  field such as `private_key_pth`.
- **`SST-CFG049`**: a template other than `env_var()`, such as `| as_number`, or
  a number or boolean that is not one.
- **`SST-CFG050`**: `oauth_client_id` and `oauth_client_secret` (use an access
  token in `token`), a key set both inline and as a file, or a key that cannot
  be read.

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
