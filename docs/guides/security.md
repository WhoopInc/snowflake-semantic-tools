# Security

SST turns files in a repository into objects in a Snowflake account. Whoever can change those
files can change what runs in the account, with the role SST connects as. This guide sets out
what SST trusts, what it checks, and how to run it with the least access.

## Trust model

**The project is trusted code.** SST does not sandbox what a project declares; it validates
its shape and publishes it. Treat each of these like SQL you would run yourself:

- **Semantic YAML**: semantic views, and the expressions of their metrics, dimensions, facts,
  and filters, are rendered into DDL that runs with the target's role. An authored expression
  is SQL by design: SST checks that it is one expression (see
  [how SST builds SQL](#how-sst-builds-the-sql-it-sends)), not what it computes.
- **Verified queries** are SQL. Connected validation runs `EXPLAIN` on each one, and on each
  metric, dimension, and filter expression, with the target's role; publishing writes them
  into the semantic view.
- **Agent instructions, tool definitions, and skills** are published as written, and decide
  what an agent does when someone calls it. `sst test --suite evals` runs the agents.
- **Profiles, hooks, MCP configs, and commands** are published to the profile registry, and
  CoCo Desktop installs them for everyone who selects the profile: a hook's script and an MCP
  server's command run for those people, not for you.
- **profiles.yml** chooses the account, the role, and the credentials.

**Never run a credentialed command on an untrusted change.** These commands connect:

- `sst plan`, `sst apply`, `sst drop`, and `sst enrich`;
- `sst validate`, unless `--no-snowflake-syntax-check` is given or
  `validation.snowflake_syntax_check: false` is set (the default is `true`);
- `sst test` with `--suite smoke` or `--suite evals`, and `sst test` with no `--suite`, which
  runs every suite that applies, the connected ones included, once `sst compile` has run;
- `sst debug`, unless `--no-connect` is given;
- `sst diff` whenever either side is a dbt target, which `--to` is by default;
- `sst baseline add` and `sst baseline prune` with `--target`.

A pull request from a fork can carry any SQL in its YAML, so run only the offline commands on
it -- `sst validate --no-snowflake-syntax-check`, `sst compile`, and `sst test --suite golden`
-- and give that job no Snowflake credentials. In GitHub Actions, trigger such jobs with
`pull_request`, which withholds repository secrets from forks, and never with
`pull_request_target`, which runs with them.

**Enrich sends data to Cortex.** `sst enrich` with `column-synonyms` or `table-synonyms` calls
`AI_COMPLETE` inside the account. A column-synonym prompt carries the model's and columns'
names, types, and descriptions, and up to five example values per column, each cut to 40
characters: values read in the same run, or the `sample_values` already written. Columns with
`pii_tags` contribute no examples and are never sampled. Set
`enrichment.allow_sample_value_collection: false` to refuse every `sst enrich` run that would
read row data, that is, one including `sample-values` or `enums` (`SST-CFG038`; see
[Enriching models](enrich.md#sample-values-and-enums)). A column-synonym prompt still carries
the `sample_values` already written; delete those to keep them out of it.

**Generated synonyms need human review.** A model's answer is not checked for meaning. SST drops
a proposed synonym that repeats a name, is longer than 100 characters, or holds a quote, a
control character, or template syntax, and reports each one it refuses for its content
(`SST-PRS030`). Review the rest in the diff before merging, as you would any other change.

## What SST checks

### How SST builds the SQL it sends

- **Every statement comes from SST's own templates.** SQL is a sealed type that only SST's SQL
  module can construct, from fixed templates and from names and literals its constructors
  quote. The connector refuses anything else, and tells Snowflake to run exactly one statement
  per call, so text that another reader would split into two statements is refused by the
  server rather than run.
- **A name that is a reserved word is quoted.** An unquoted name that is a Snowflake reserved
  word, such as a column called `select`, is written double-quoted in upper case (`"SELECT"`):
  it names the same object, and no reader takes it for the keyword.
- **Authored SQL passes a guard first.** Each metric, fact, dimension, and filter expression, a
  window frame, and a search tool's `where:` must be exactly one expression, checked after its
  `{{ ... }}` references are resolved. Outside strings and quoted identifiers it may hold no
  `;`, comment, or `$$`; its brackets must balance; and it may hold no statement keyword
  (`ALTER`, `BEGIN`, `CALL`, `COMMIT`, `COPY`, `CREATE`, `DELETE`, `DESCRIBE`, `DROP`,
  `EXECUTE`, `GRANT`, `INSERT`, `LIST`, `MERGE`, `PUT`, `REMOVE`, `REVOKE`, `ROLLBACK`, `SET`,
  `SHOW`, `TRUNCATE`, `UNDROP`, `UNSET`, `UPDATE`, `USE`) or query keyword (`SELECT`, `WITH`,
  `UNION`, `INTERSECT`, `MINUS`, `EXCEPT`) as a bare word; `INSERT(` and `TRUNCATE(` are allowed
  as the functions they also are.
- **A verified query** must begin with `SELECT` or `WITH`, after any leading comments, which are
  never sent, and may end with one `;`. After its first word the same rules hold, except that
  query keywords are allowed.
- **Neither may call a `SYSTEM$` function**, written bare or as a quoted name, because those
  can act on the account.
- **The text checked is the text sent.** `\r\n` becomes `\n` and each line loses its trailing
  whitespace; any other control character but tab, and any Unicode line or paragraph
  separator, is refused. The guard runs again where the text becomes SQL.

A refusal is `SST-VAL418`, reported offline and naming the reason and where it starts. The
guard decides only that the text is one expression or one query. What the expression reads
and computes is still the author's, and runs with the target's role.

### Files stay inside the project

- **Linked files in extension folders are refused.** SST does not follow symbolic links when
  it walks skill, plugin, profile, hook, MCP, and command folders: a linked file or folder, or
  one reached through a linked folder, is reported (`SST-PRT009`) and left out, even when it
  points inside the project.
- **Linked authored YAML is refused too.** Semantic-model discovery, and the agent, tool, and
  eval-metric loaders, never follow a symbolic link either: a linked YAML file or a linked
  folder is reported (`SST-PRT009`) and left out, wherever it points, and a link that loops
  back into its tree, or to itself, is `SST-DIS005`. The semantic-models directory itself must
  not be a link, be reached through one, or resolve outside the project (`SST-PRT009`). Only
  the project directory may itself be a link.
- **Paths a project names stay inside it.** A `{{ file() }}` instructions sidecar or an eval
  file that resolves outside the project is refused (`SST-REF027`), and so is a dbt
  `target-path` outside it (`SST-PRT009`). A verified query's `sql_file:` that climbs out of
  the project, is absolute, or is or passes through a symbolic link is refused and never read
  (`SST-PRT009`); so is a tool's `body_file:`, which is read relative to the project. A
  `define:` member whose body is refused is also reported as missing (`SST-LOD018`).
- **Checksums are read from the project only.** The input checksums a manifest records skip
  every symbolic link, and every file reached through one, as the loaders refuse them, so no
  checksum is read from outside the project.
- **Writes stay inside a root.** Every file SST writes -- the manifest, rendered DDL, formatted
  or enriched YAML, a baseline, a lock -- goes under the project, or under the output
  directory you named. Each folder on the way is opened without following links, so a `..` or
  a symbolic link refuses the write (`SST-PRT008`) instead of reaching outside.

### Credentials stay out of what SST prints

- **Connector errors are scrubbed.** A connector error is reported with its errno, its
  SQLSTATE, and its message. In that message, the value of any setting whose name contains
  `password`, `passwd`, `pwd`, `token`, `passphrase`, `secret`, or `private_key`, and any path
  ending in `.p8`, `.pem`, `.key`, `.der`, `.p12`, or `.pfx`, is replaced by `<redacted>`.
- **An unreadable key is not echoed.** An inline `private_key` that cannot be read is reported
  by the exception type only (`SST-CFG050`).
- **Output that would carry a credential is withheld.** Every credential value the target
  resolves -- `password`, `token`, `private_key`, `private_key_passphrase`,
  `private_key_file_pwd` -- is checked against everything SST prints: the JSON envelope, YAML,
  CSV, and human reports, diagnostics, and progress lines. The check matches the value as
  written and as JSON escapes it. Output that contains one is replaced by `SST-PRT012`; a
  withheld envelope or report makes the run exit 1. `sst debug` shows the authentication
  method, never the credential.
- **`insecure_mode: true` is reported.** The setting turns off OCSP certificate revocation
  checks. SST honours it, and warns (`SST-CFG051`) on every run that uses that target.

### Rendering touches nothing

SST renders each member under a run-time audit hook: a render that opens a file, a socket, or
a process stops with `SST-INT002`, which is an SST bug to report. The offline commands --
`sst validate --no-snowflake-syntax-check`, `sst compile`, and `sst test --suite golden` --
need no network, and SST's own test suite fails any in-process test outside its live layer
that reaches it.

### SST changes only what it published

`plan` treats an object as SST's only when the ownership marker SST writes in its comment
matches the entry state records for it. An object at a name SST would publish that state does
not record as SST's is reported as unmanaged (`SST-PLN024`) and left alone. SST never grants
new access: after replacing an object it restores only the explicit grants the object held
before (see [state and ownership](../concepts.md#state-and-ownership)).

## Operating SST

- **Use a dedicated, least-privileged role.** Give the role SST connects as the privileges to
  create and replace the object types it publishes in its target schemas, read access to the
  tables the semantic views select from, and nothing else. Grant consumers access to the
  published objects separately; SST does not. `plan` and `apply` need nothing account-wide.
  Two account-level grants are optional; each only feeds a warning, so a role without one
  skips that warning with an info (`SST-VAL020`, shown with `--show-info`) and goes ahead:

  | Optional grant | What it reads | Without it |
  |---|---|---|
  | `MONITOR` on the account | other users' locks (`SHOW LOCKS IN ACCOUNT`) | no concurrent-writer warning (`SST-PLN019`) |
  | `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database | `SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES` | under `--prune`, no warning that a dropped object is still named by something outside the project (`SST-PLN017`) |

  Without the second, `apply --prune` drops each orphan the plan lists without checking who
  names it, so read the plan's drops yourself before confirming with `--yes`. Even with the
  grant the check is a warning, never a block, and Snowflake's dependency record can lag by up
  to three hours.
- **Give evals one primary role.** An eval runs as Snowflake tasks, and a task runs with the
  session's primary role only, never its secondary roles. That role needs `CREATE TASK`,
  `CREATE STAGE`, and `CREATE FILE FORMAT` on each agent's schema, directly or through a role
  granted to it. `sst test --suite evals` checks this before any run starts: a privilege held
  only through a secondary role is `SST-VAL727`, one held by no role in the session
  `SST-VAL728`.
- **Develop against a scratch schema.** Point a personal target in `profiles.yml` at a schema
  of your own, and run `plan` and `apply` there before any change reaches a shared target.
- **Apply only a reviewed plan.** Plan on pull requests from the repository's own branches,
  review the plan, and apply exactly that plan from the protected branch with
  `sst apply --plan target/sst/plan.json`.
- **Prune deliberately.** `--prune` drops semantic views and agents whose source was deleted,
  and SST recognises its objects by the ownership marker in their comments together with the
  state table's record. Anyone who can alter an object's comment and write the state table can
  forge both, so treat them as a record, not an access control: limit who can alter objects
  and write state in the schemas SST publishes to, and read the plan's drops before
  `apply --prune`.
- **Keep credentials out of the repository.** Read them from the environment with
  `{{ env_var('NAME') }}` in `profiles.yml`, and prefer key-pair authentication for jobs (see
  [authentication](configuration.md#authentication)). Browser SSO needs someone at a browser,
  so it suits interactive use only (see
  [troubleshooting](troubleshooting.md#a-command-hangs-while-signing-in)).
- **Pin what CI runs.** Install SST at an exact version, pin each action by commit SHA, and
  let a dependency bot propose updates.

Report a vulnerability as [SECURITY.md](../../SECURITY.md) describes.
