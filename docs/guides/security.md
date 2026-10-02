# Security

SST turns files in a repository into objects in a Snowflake account. Whoever can change those
files can change what runs in the account, with the role SST connects as. This guide sets out
what SST trusts, what it checks, and how to run it with the least access.

## Trust model

**The project is trusted code.** SST does not sandbox what a project declares; it validates
its shape and publishes it. Treat each of these like SQL you would run yourself:

- **Semantic YAML**: semantic views, and the expressions of their metrics, dimensions, facts,
  and filters, are rendered into DDL that runs with the target's role.
- **Verified queries** are SQL. Connected validation runs `EXPLAIN` on each one, and on each
  metric, dimension, and filter expression, with the target's role; publishing writes them
  into the semantic view.
- **Agent instructions, tool definitions, and skills** are published as written, and decide
  what an agent does when someone calls it. `sst test --suite evals` runs the agents.
- **profiles.yml** chooses the account, the role, and the credentials.

**Never run a credentialed command on an untrusted change.** `sst plan`, `sst apply`,
`sst test --suite smoke|evals`, `sst debug --test-connection`, `sst enrich`, and `sst validate`
with Snowflake syntax checks all connect. A pull request from a fork can carry any SQL in its
YAML, so run only the offline commands on it -- `sst validate --no-snowflake-syntax-check`,
`sst compile`, and `sst test --suite golden` -- and give that job no Snowflake credentials. In
GitHub Actions, trigger such jobs with `pull_request`, which withholds repository secrets from
forks, and never with `pull_request_target`, which runs with them.

**Enrich sends data to Cortex.** `sst enrich` with `column-synonyms` or `table-synonyms` calls
`AI_COMPLETE` inside the account. A column-synonym prompt carries the model's and columns'
names, types, and descriptions, and up to five example values per column: values read in the
same run, or the `sample_values` already written. Columns with `pii_tags` contribute no
examples and are never sampled. Set `enrichment.allow_sample_value_collection: false` to stop
`sst enrich` reading row data at all (see [Enriching models](enrich.md#sample-values-and-enums)).

**Generated synonyms need human review.** A model's answer is not checked for meaning. SST drops
a proposed synonym that repeats a name, is longer than 100 characters, or holds a quote, a
control character, or template syntax, and reports each one it refuses for its content
(`SST-PRS030`). Review the rest in the diff before merging, as you would any other change.

## What SST checks

- **Files stay inside the project.** SST does not follow symbolic links when it walks skill,
  plugin, profile, hook, MCP, and command folders: a linked file or folder is reported
  (`SST-PRT009`) and left out, so a link cannot publish a file from elsewhere on the machine.
  A `{{ file() }}` sidecar or an eval file that resolves outside the project is refused
  (`SST-REF027`), and so is a dbt `target-path` outside it (`SST-PRT009`).
- **Credentials stay out of reports.** A connector error is reported with its errno, its
  SQLSTATE, and its message; a `password`, `token`, `passphrase`, `secret`, or `private_key`
  setting in that message, and any path to a key file, is replaced by `<redacted>`. An inline
  `private_key` that cannot be read is reported by the exception type only.
- **SST changes only what it published.** `plan` proves ownership by the marker SST writes in
  each object's comment; an object without one is reported as unmanaged and left alone. SST
  never grants new access (see [state and ownership](../concepts.md#state-and-ownership)).

## Operating SST

- **Use a dedicated, least-privileged role.** Give the role SST connects as the privileges to
  create and replace the object types it publishes in its target schemas, read access to the
  tables the semantic views select from, and nothing else. Grant consumers access to the
  published objects separately; SST does not.
- **Develop against a scratch schema.** Point a personal target in `profiles.yml` at a schema
  of your own, and run `plan` and `apply` there before any change reaches a shared target.
- **Apply only a reviewed plan.** Plan on pull requests from the repository's own branches,
  review the plan, and apply exactly that plan from the protected branch with
  `sst apply --plan target/sst/plan.json`.
- **Prune deliberately.** `--prune` drops semantic views and agents whose source was deleted,
  and SST recognises its objects by the ownership marker in their comments. Anyone who can
  alter an object's comment can write that marker, so treat it as a record, not an access
  control: limit who can alter objects in the schemas SST publishes to, and read the plan's
  drops before `apply --prune`.
- **Keep credentials out of the repository.** Read them from the environment with
  `{{ env_var('NAME') }}` in `profiles.yml`, and prefer key-pair authentication for jobs (see
  [authentication](configuration.md#authentication)).
- **Pin what CI runs.** Install SST at an exact version, pin each action by commit SHA, and
  let a dependency bot propose updates.

Report a vulnerability as [SECURITY.md](../../SECURITY.md) describes.
