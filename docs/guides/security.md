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
  metric, dimension, and filter expression, with the target's role, after `DESCRIBE TABLE` on
  each table the views read; publishing writes them into the semantic view.
- **Agent instructions, tool definitions, and skills** are published as written, and decide
  what an agent does when someone calls it. `sst eval` runs the agents.
- **Profiles, hooks, MCP configs, and commands** are published to the profile registry, and
  CoCo Desktop installs them for everyone who selects the profile: a hook's script and an MCP
  server's command run for those people, not for you.
- **profiles.yml** chooses the account, the role, and the credentials.

**Never run a credentialed command on an untrusted change.** These commands connect:

- `sst plan`, `sst apply`, `sst drop`, `sst state recover`, `sst state convert`, and `sst enrich`;
- `sst validate`, unless `--no-snowflake-syntax-check` is given or
  `validation.snowflake_syntax_check: false` is set (`sst init` writes `true`);
- `sst test`, which queries every published object it probes, and `sst eval`, which runs
  the agents;
- `sst ask`, which also creates scratch copies of the edited views and tools and
  runs the agent on them, with the target's role;
- `sst state unlock`;
- `sst debug`, unless `--no-connect` is given;
- `sst diff` whenever either side is a dbt target, which `--to` is by default;
- `sst baseline add --target` for a code only a connection finds, or with `--all-warnings`, and
  `sst baseline prune --target` when an entry records that target and only a connection finds its
  code; for any other code both stay offline.

A pull request from a fork can carry any SQL in its YAML, so run only the offline commands on
it -- `sst validate --no-snowflake-syntax-check` and `sst compile` -- and give that job no
Snowflake credentials. Whatever the CI system, a job that builds a fork's change
must not receive the Snowflake secrets. In GitHub Actions, trigger such jobs with
`pull_request`, which withholds repository secrets from forks, and never with
`pull_request_target`, which runs with them.

**Enrich sends data to Cortex.** `sst enrich` with `column-synonyms` or `table-synonyms` calls
`AI_COMPLETE` inside the account. A column-synonym prompt carries the model's and columns'
names, types, and descriptions, and up to five example values per column, each cut to 40
characters: values read in the same run, or the `sample_values` already written. A sensitive
column -- one with `pii_tags` or `meta.sst.governance_tags`, or, with `governance.read_column_tags`
on, a `governance.sensitive_tags` tag in Snowflake -- contributes no examples and is never sampled,
and no sample value of it is published in a semantic view. A tag read that fails fails its model
rather than reading it unprotected; what a role that cannot see a tag is answered is unmeasured
(see [Enriching models](enrich.md#governance-tags)). Set
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
  it names the same object, and no reader takes it for the keyword. That holds inside
  expressions too: `SUM({{ ref('orders', 'order') }})` renders `SUM(ORDERS."ORDER")`.
- **A quoted column keeps its case.** A dbt column declared with `quote: true` was created
  quoted, so its name is case-sensitive; SST writes it double-quoted exactly as dbt names it,
  as a member name, in every expression and in keys and relationships
  (`ORDERS."MixedCaseAmount"`). `validate --verify-schema` compares such a column with the
  table's exactly, so a case mismatch is reported (`SST-VAL325`) rather than failing at
  create.
- **Authored SQL passes a guard first.** Each metric, fact, dimension, and filter expression, a
  window frame, and a search tool's `where:` must be exactly one expression, checked after its
  `{{ ... }}` references are resolved. Outside strings and quoted identifiers it may hold no
  `;`, comment, or `$$`; its brackets must balance; and it may hold no statement keyword
  (`ALTER`, `BEGIN`, `CALL`, `COMMIT`, `COPY`, `CREATE`, `DELETE`, `DESCRIBE`, `DROP`,
  `EXECUTE`, `GRANT`, `INSERT`, `LIST`, `MERGE`, `PUT`, `REMOVE`, `REVOKE`, `ROLLBACK`, `SET`,
  `SHOW`, `TRUNCATE`, `UNDROP`, `UNSET`, `UPDATE`, `USE`) or query keyword (`SELECT`, `WITH`,
  `UNION`, `INTERSECT`, `MINUS`, `EXCEPT`) as a bare word; `INSERT(` and `TRUNCATE(` are allowed
  as the functions they also are.
- **A verified query** must begin with `SELECT` or `WITH`, and may end with one `;`. After its
  first word the same rules hold, except that query keywords and comments are allowed. A
  comment inside the query is sent as written; one before or after it is not, so a trailing
  `--` comment cannot swallow what SST appends when it checks or runs the query.
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
- **Paths a project names stay inside it.** A `{{ file() }}` sidecar -- an agent's
  instructions, a verified query's SQL, a tool member's body, or a hook's script -- is
  read relative to the folder of the file that names it, and one that is absolute or
  resolves outside the project is refused
  (`SST-REF027`), and so is a dbt `target-path` outside it (`SST-PRT009`). A sidecar that is
  or passes through a symbolic link is refused and never read (`SST-PRT009`), as is a linked
  eval file or `evals/` folder; eval files are found in the agent's own `evals/` folder, never
  by a path. A defined tool member whose body is refused does not compile.
- **Checksums are read from the project only.** The input checksums a manifest records skip
  every symbolic link, and every file reached through one, as the loaders refuse them, so no
  checksum is read from outside the project.
- **Writes stay inside a root.** Every file SST writes -- the manifest, rendered DDL, formatted
  or enriched YAML, a baseline, a lock, an `sst ask` transcript -- goes under the project, or
  under the output directory you named. Each folder on the way is opened without following
  links, so a `..` or a symbolic link refuses the write (`SST-PRT008`, or `SST-ASK012` for a
  transcript) instead of reaching outside.

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
  checks. SST honors it, and warns (`SST-CFG051`) on every run that uses that target.

### Rendering touches nothing

SST renders each member under a run-time audit hook: a render that opens a file, a socket, or
a process stops with `SST-INT002`, which is an SST bug to report. The offline commands --
`sst validate --no-snowflake-syntax-check` and `sst compile` -- need no network, and SST's own
test suite fails any in-process test outside its live layer
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
- **Schemas.** With `apply.create_schemas` on, its default, apply runs
  `CREATE SCHEMA IF NOT EXISTS` for each absent schema the plan lists as
  `+ schema`, before it takes the run lock, so the role needs `CREATE SCHEMA` on
  those databases; a refusal is `SST-APL039`, and nothing else runs. The schema
  is owned by the role that ran apply, as dbt's are, with no grant and no
  comment. Set `apply.create_schemas: false` to keep `CREATE SCHEMA` out of the
  role's privileges: plan then reports an absent schema as `SST-PLN010`. SST
  never creates a database (`SST-PLN011`).
- **`sst ask` and unpublished evals.** They publish nothing, but they create
  scratch copies, `<NAME>__SST_ASK_<id>`, of what you edited, in
  `agents.+scratch_schema` (else the agent's schema), and drop them when the
  session ends: the role needs the `CREATE` privilege of each kind copied there
  ([asking an agent](ask.md#where-the-copies-go)). `sst ask` also calls the
  Cortex Agents REST API with the connector session's own token, so it runs as
  that session's role and sends no other credential; the token never leaves the
  `Authorization` header, and no diagnostic or log line carries it.
- **An `sst ask` transcript holds what the agent returned.** Off unless `--transcript` or
  `ask.transcripts: true` asks: it then keeps, in `sst_target/ask/`, every question and
  answer, each tool's SQL and every row a tool returned, and the specification sent. That is
  warehouse data on your disk, readable as your own files are. `sst_target/` belongs in
  `.gitignore`, as `target/` does, so no transcript is committed; `sst clean` removes them.
  A transcript never holds a credential or the session token.
- **Give evals one primary role.** An eval runs as Snowflake tasks, and a task runs with the
  session's primary role only, never its secondary roles. That role needs `CREATE TASK`,
  `CREATE STAGE`, and `CREATE FILE FORMAT` on each agent's schema, directly or through a role
  granted to it. `sst eval` checks this before any run starts: a privilege held
  only through a secondary role is `SST-VAL727`, one held by no role in the session
  `SST-VAL728`.
- **Develop against a scratch schema.** Point a personal target in `profiles.yml` at a schema
  of your own, and run `plan` and `apply` there before any change reaches a shared target.
- **Apply only a reviewed plan.** Plan on pull requests from the repository's own branches,
  review the plan, and apply exactly that plan from the protected branch with
  `sst apply --plan sst_target/plan.json`.
- **Prune deliberately.** `--prune` drops semantic views and agents whose source was deleted,
  and SST recognizes its objects by the ownership marker in their comments together with the
  state table's record. Anyone who can alter an object's comment and write the state table can
  forge both, so treat them as a record, not an access control: limit who can alter objects
  and write state in the schemas SST publishes to, and read the plan's drops before
  `apply --prune`.
- **Keep credentials out of the repository.** Read them from the environment with
  `{{ env_var('NAME') }}` in `profiles.yml`, and prefer key-pair authentication for jobs (see
  [authentication](configuration.md#authentication)). Browser SSO needs someone at a browser,
  so it suits interactive use only (see
  [troubleshooting](troubleshooting.md#a-command-hangs-while-signing-in)).
- **Pin what CI runs.** Install SST at an exact version, pin each third-party CI step (in
  GitHub Actions, each action by commit SHA), and
  let a dependency bot propose updates.

## What people who use an agent need

SST publishes an agent and the objects its tools call, and grants nobody access to any of
them; whoever administers the account's roles does. Snowflake checks each call with the
caller's role, so a person needs `USAGE` on the agent and on the database and schema that
hold it, and access to what each of its tools reaches:

| Tool or source | What the caller's role needs |
|---|---|
| `cortex_search` | `USAGE` on the Cortex Search service |
| `generic` | `USAGE` on the procedure or function |
| `agent_toolset` | `USAGE` on the agent whose tools it inherits, and access to what those tools reach; without it, Snowflake silently drops the inherited tools |
| a skill or plugin the agent pins | `READ` on the Cortex Extension |
| `cortex_analyst_text_to_sql` | the privileges Snowflake requires to query the semantic view |

Snowflake's [Cortex Agents documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
is the full list, including what a role needs to use Cortex at all and the warehouse each
tool runs on.

**What SST checks.** With connected validation on, `sst validate` and `sst plan` read the
grants on the live agent and treat as its consumers the session's role and every role
granted `USAGE` on it. Each consumer that holds neither `USAGE` nor `OWNERSHIP` on an object a
tool calls that SST does not publish -- a referenced tool member, or another project's agent --
is a warning (`SST-VAL616`), and so is each one without `READ` or `OWNERSHIP` on an
extension the agent pins (`SST-VAL542`). Both compare an object's direct grantees only and do
not follow the role hierarchy, so a role that holds the privilege through another role is
reported too. An agent not yet published has no grants to read, so only the session's role
is checked. Semantic views and the objects defined tool members publish are not checked.

**When a caller lacks a tool.** What the agent does then is Snowflake's behavior, chosen by
the agent's `config.tool_not_accessible` (`accept`, `reject`, or `legacy`), or
`agents.+tool_not_accessible`. SST writes the value into the specification as given and
leaves it out when neither sets one, so set it to keep the behavior in the project.

**An eval shows one role's view.** An eval runs as the session's primary role (see
[operating SST](#operating-sst)), so a passing eval says what the agent answers for that
role, not for a consumer who holds less.

**Publishing keeps the access consumers have.** An agent is never replaced, so its grants
survive every publish. A procedure, function, or semantic view is replaced with
`COPY GRANTS`. A Cortex Search service has no such clause: apply reads its explicit grants,
re-issues them after the replace, and checks that all are back (`SST-APL009`); until they
are, its grantees cannot use it, which `SST-VAL619` warns of beforehand. None of this adds a
grant the object did not hold.

Drawn from: [live-object checks](../../snowflake_semantic_tools/app/compile/agents/observe.py), [agent tool resolution](../../snowflake_semantic_tools/app/compile/agents/resolve_tools.py), [agent renderer](../../snowflake_semantic_tools/domain/render/agent.py), [agent diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/val_agent.py), [tool diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/val_tool.py), [tool renderer](../../snowflake_semantic_tools/domain/render/tool.py).

Report a vulnerability as [SECURITY.md](../../SECURITY.md) describes.
