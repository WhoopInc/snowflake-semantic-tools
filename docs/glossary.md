# Glossary

The words SST's pages, commands, and diagnostics use, each in one sense, with
where it is explained.

## Terms

- **Agent.** A Cortex Agent SST publishes from `agent.yml`, created once and
  then given a new version for each change. See [agents](guides/agents.md).
- **Agent kit.** The skills `sst agent-kit` installs for a coding agent, Cortex
  Code or Claude Code, into the project; never published. Not the project's
  `skills/`. See [the agent kit](guides/agent-kit.md).
- **Alias.** The version alias an agent's `alias` setting names (`config.alias`,
  or `+alias` in `agents:`), which apply moves to each new version; promotion
  and rollback are moves of it. See
  [publishing agents](guides/agents.md#publishing).
- **Apply.** The command that executes a plan's changes against a target and
  records them in state. See [the pipeline](concepts.md#the-pipeline).
- **Artifact.** One thing SST publishes and tracks: a semantic view, tool,
  skill, plugin, profile, agent, or eval. See [artifacts](concepts.md#artifacts).
- **Association.** How metrics, relationships, filters, and verified queries
  join views: a member attaches to every view whose `tables:` include all of
  its own. See [association](concepts.md#association-how-members-find-views).
- **Baseline.** The file `.sst/baseline.json` unless `--baseline` names another,
  holding known warnings that are counted rather than shown and do not block
  until the baseline expires. See
  [known warnings](guides/ci-cd.md#known-warnings-the-baseline).
- **Connected validation.** The checks `sst validate` and `sst plan` make
  against Snowflake, such as compiling expressions and reading live objects.
  They run unless `--no-snowflake-syntax-check` is given,
  `validation.snowflake_syntax_check` is `false`, or `plan` runs with
  `--no-validate`. See the
  [trust model](guides/security.md#trust-model).
- **Consumer.** A role that uses a published object, such as one granted
  `USAGE` on an agent. SST grants consumers nothing. See
  [what people who use an agent need](guides/security.md#what-people-who-use-an-agent-need).
- **Defer target.** A second target whose dbt relations `ref()` resolves to,
  while publishing still goes to `--target`. See [targets](concepts.md#targets).
- **Diagnostic.** A problem SST reports, with a stable code such as
  `SST-VAL405` and a severity of error, warning, or info. See
  [diagnostics](concepts.md#diagnostics).
- **Eval.** A fixed set of questions an agent is run against and scored on,
  published with the agent. See [evals](guides/evals.md).
- **Eval baseline.** The recorded pass flags of an agent's eval runs, which a
  later run is gated against; stored in Snowflake, and unrelated to the
  baseline file. See [eval baselines](guides/evals.md#baselines).
- **Extension.** A Cortex Extension: the Snowflake object a skill or plugin
  publishes versions of, and that an agent pins. See [skills](guides/skills.md).
- **Fingerprint.** A SHA-256 digest of what an artifact renders, which `plan`
  compares with state to decide whether it changed. See
  [terms used in the code](concepts.md#terms-used-in-the-code).
- **Folder route.** An unprefixed key under an artifact block of
  `sst_config.yml` -- `semantic_views:`, `tools:`, `agents:`, `evals:`,
  `skills:`, `plugins:`, or `profiles:` -- that names a folder, whose `+` keys
  apply to what is in it. See
  [folder routes](guides/configuration.md#folder-routes).
- **Key.** An artifact's or member's identity, `<type>:<name>`, such as
  `semantic_view:sales`. It never names a database or schema. See
  [identity and versions](concepts.md#identity-and-versions).
- **Lock.** The row a writing run holds in the lock table beside the state
  table, so one run at a time writes to a target. See
  [the run lock](concepts.md#concurrent-applies-and-the-run-lock).
- **Manifest.** SST's record of one compile, `sst_target/manifest.json`. See
  [the pipeline](concepts.md#the-pipeline).
- **Member.** A fact, dimension, time dimension, metric, relationship, filter,
  verified query, or custom instruction: it reaches Snowflake inside the views
  it attaches to, never on its own. See [members](concepts.md#members).
- **Ownership marker.** The `[sst:<manifest id>:<fingerprint>]` tag SST writes
  at the end of a published object's comment. See
  [terms used in the code](concepts.md#terms-used-in-the-code).
- **Partial.** `--partial`: go ahead with every artifact that has no errors and
  depends on none that has. See
  [ownership, grants, and prune](concepts.md#ownership-grants-and-prune).
- **Plan.** The command that compares the project with state and with what is
  live, and saves the changes to `sst_target/plan.json` without writing to
  Snowflake. See [the pipeline](concepts.md#the-pipeline).
- **Plugin.** A bundle of skills published as one plugin-type Cortex
  Extension. See [plugins](guides/plugins-and-profiles.md#plugins).
- **Poisoned.** Said of a member or view with an error: still checked, but not
  attached or built. See [terms used in the code](concepts.md#terms-used-in-the-code).
- **Profile.** A CoCo Desktop profile SST publishes from `profile.yml`. See
  [profiles](guides/plugins-and-profiles.md#profiles).
- **Prune.** `--prune`: extend a plan to managed artifacts whose source was
  deleted. See [ownership, grants, and prune](concepts.md#ownership-grants-and-prune).
- **Reference.** A `{{ ... }}` call, such as `{{ ref('orders') }}`, that SST
  resolves for the target being built. See [references](concepts.md#references).
- **Semantic view.** A Snowflake semantic view SST assembles from a
  `semantic_view:` file, or an entry under `semantic_views:`, and the members
  that attach to it. See
  [semantic views](guides/semantic-views.md).
- **Setting.** One `+` key of an artifact block, such as `schema` or
  `certified`, as it resolves for one artifact: its own `config:`, else the
  closest folder route, else the block root, else SST's default.
  `sst list --long` prints where each came from. See
  [precedence](guides/configuration.md#precedence).
- **Skill.** A `SKILL.md` folder SST publishes as a version of a skill-type
  Cortex Extension. See [skills](guides/skills.md).
- **Smoke suite.** The part of `sst test` that checks deployed objects exist as
  planned. See [`sst test`](reference/cli.md#sst-test).
- **State.** The table, `SST_STATE` by default, in which apply records what it
  published to a target. See [state and ownership](concepts.md#state-and-ownership).
- **Strict.** `--strict` or `validation.strict: true`, which promotes every
  warning to an error. See [diagnostics](concepts.md#diagnostics).
- **Subject.** The key a diagnostic is about. See
  [terms used in the code](concepts.md#terms-used-in-the-code).
- **Target.** The `profiles.yml` target a command runs against, which decides
  where everything lands. See [targets](concepts.md#targets).
- **Tool.** An entry in an agent's `spec.tools`, and also the `tool` artifact a
  defined tool member publishes for such an entry to call. See
  [tools](guides/agents.md#tools).
- **Tool member.** One object an agent tool calls, declared once by a `tool:`
  file or a `tools:` list entry: defined, which SST publishes, or, when it writes
  `relations:`, referenced, which SST only locates. See
  [tool members](guides/agents.md#tool-members).
- **Unmanaged.** Said of an object at a name SST would publish to that state
  does not record as SST's; plan blocks it (`SST-PLN024`). See
  [ownership, grants, and prune](concepts.md#ownership-grants-and-prune).
- **Verified query.** A question with the SQL that answers it, published inside
  every view it attaches to. See [verified queries](guides/semantic-views.md#verified-queries).

Drawn from: [artifact keys](../snowflake_semantic_tools/domain/model/artifact_key.py), [ownership marker](../snowflake_semantic_tools/domain/model/lifecycle/marker.py), [tool model](../snowflake_semantic_tools/domain/model/tool.py), [diagnostic baseline](../snowflake_semantic_tools/domain/diagnostics/baseline.py), [state](../snowflake_semantic_tools/app/state.py), [manifest](../snowflake_semantic_tools/app/manifest.py).

## Words that mean different things in Snowflake, dbt, and SST

| Word | In Snowflake | In dbt | In SST |
|---|---|---|---|
| model | a language model, such as an agent's orchestration model or a search service's embedding model | a SQL model and the relation it builds | a dbt model, as `{{ ref('<model>') }}` names it; an agent's `orchestration_model` setting and a search service's `embedding_model` keep Snowflake's sense |
| target | no object; `TARGET_LAG` is the freshness a search service keeps | an output of a profile in `profiles.yml` | the same `profiles.yml` target, chosen by `--target`, which decides where every artifact lands |
| manifest | an application package's `manifest.yml` | `target/manifest.json`, which SST reads | `sst_target/manifest.json`, the record of one SST compile; a plugin's `plugin.yml` is also called its manifest |
| state | -- | `--state`, a directory of an earlier run's artifacts | the state table apply writes; `--state` also names an earlier build directory, as in dbt |
| version | a numbered version of an agent or extension, such as `VERSION$1` | a model version, or dbt's own release | an extension version named for its content, or an agent version added by each change |
| alias | a name on an agent or extension version | the name a model's relation takes instead of the model's | an agent's `alias` setting, moved to each new version |
| profile | an agent's display name, avatar, and color | a connection in `profiles.yml` | dbt's `profiles.yml`, which SST also reads, or a CoCo Desktop profile from `profile.yml`; an agent's `profile:` keeps Snowflake's sense |
| catalog | the catalog that serves Cortex Extensions to users; also a catalog integration for Iceberg tables | `catalog.json`, written by `dbt docs generate` | the same Cortex Extension catalog, which a skill or plugin publishes to wherever its `+bundle_stage` resolves |
| tool | an entry in an agent specification's `tools` | -- | an agent's tool, or the `tool` artifact a defined tool member publishes |
| metric | an entry in a semantic view's `METRICS` | a metric in dbt's semantic layer | a `metric:` file or a `snowflake_metrics:` entry; an eval's judge metric is a different thing, named with `eval_metric()` |

Drawn from: [configuration keys](../snowflake_semantic_tools/domain/model/config_schema/keys.py), [agent model](../snowflake_semantic_tools/domain/model/agent.py), [tool model](../snowflake_semantic_tools/domain/model/tool.py), [dbt model](../snowflake_semantic_tools/domain/model/dbt.py), [skill model](../snowflake_semantic_tools/domain/model/skill/model.py), [manifest](../snowflake_semantic_tools/app/manifest.py).
