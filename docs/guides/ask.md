# Asking an agent as it compiles now

`sst ask` talks to an agent before anything about it is published: the agent as
the project compiles now, with every edit you made to it and to what it calls.
Nothing is published to the target, no lock is taken, and no state is written.

```bash
sst ask                                    # pick an agent, then talk to it
sst ask sales_analyst                      # talk to sales_analyst
sst ask sales_analyst "How many orders were placed in March?"   # ask once
echo "Why did returns rise?" | sst ask sales_analyst - --output json
```

## What runs

1. The project compiles in memory, as `sst test` compiles it.
2. SST reads the target's state table, read-only, and finds each semantic view,
   search service, procedure, function, skill and sub-agent the agent reaches
   that compiles differently from what state records: an edit you have not
   published, or something never published to this target.
3. Each of those is copied into the agent's scratch schema as
   `<NAME>__SST_ASK_<id>`: a view or tool from its own rendered statements, a
   skill from its bundle, uploaded to a stage of the session's own, and a
   sub-agent from its specification, pointed at the copies too. So an edit
   reaches the agent through every agent between them.
4. The agent's specification, pointed at the copies, runs over the
   [Cortex Agents Run API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-run),
   `POST /api/v2/cortex/agent:run` with the specification in the body, so no
   agent object is created for it. The run streams, and each turn is a turn on a
   [thread](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-threads),
   so the conversation carries over from one question to the next.
5. When the session ends, however it ends, the thread is deleted and every copy
   is dropped.

A run on copies sets `orchestration.tool_not_accessible: reject`, unless the
agent already says what to do, so an agent that cannot reach a tool fails
loudly (`SST-ASK010`) rather than answering without it, as the default does
([inaccessible tool handling](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-inaccessible-tool-handling)).

`--published` copies nothing: the agent calls what the target holds now, and
each dependency that compiles differently is warned of (`SST-ASK001`).

## Where the copies go

Copies are made in `agents.+scratch_schema` when it is set, else in the agent's
own schema. A semantic view cannot be created in a personal `USER$` database
(Snowflake refuses with 060119), so a project whose agents live there sets a
scratch schema in a real database. The role needs, in that schema, the `CREATE`
privilege of each kind it copies (`CREATE SEMANTIC VIEW`, `CREATE CORTEX SEARCH
SERVICE`, `CREATE FUNCTION`, `CREATE PROCEDURE`, `CREATE STAGE` and `CREATE
CORTEX EXTENSION` for a skill, `CREATE AGENT` for a sub-agent); a refusal is
`SST-ASK008`, and the copies made before it are dropped.

Connected `sst validate` runs its orchestration-model probes (`SST-VAL551`)
there too. A scratch schema created ahead of the first apply therefore lets the
probes run before apply has created the agents' own schemas.

A copy carries no SST ownership marker, and a view or tool keeps its own
comment. A session that died, from a crash or a killed terminal, can leave
copies and a thread behind: each new session removes those older than 12 hours,
found by `__SST_ASK_` in their name and their age, and `sst ask --cleanup`
removes every one now, in each agent's scratch schema, together with the
`sst_ask` threads. Run `--cleanup` when no other session of yours is open.

Measured on a live account: a view copy takes about 2 seconds, a function 1, a
skill about 4 (stage, upload, extension), an agent 1, and each drop under half a
second. A Cortex Search service copy takes as long as its first index build,
from about 30 seconds to two minutes for a small table, so a session copies one
only when the search member itself is edited.

## The conversation

On a terminal, `sst ask` without a question opens a full-screen conversation.
Without an agent named, it starts with a picker: type to filter, Enter opens the
first match. The header names the agent and what was copied for it.

The answer streams in as it is generated. Reasoning, each tool call (its name,
type and input) and each tool result (the SQL an Analyst tool ran, its rows as a
table, and the semantic view it queried) sit in collapsible blocks above the
answer; the status line says what the agent is doing.

| Key | Does |
|---|---|
| Enter | Sends the question |
| Ctrl-C | Cancels the reply in flight; quits when none is |
| Ctrl-T | Shows or hides the reasoning and tool blocks |
| Ctrl-A | Picks another agent: a new thread, and new copies |
| Ctrl-R | Compiles again and rebuilds the copies; the thread, and so the conversation, goes on |
| Ctrl-D, `/exit` | Quits |
| `/copy`, `/copy-chart` | Copies the last answer, or the last chart's Vega-Lite specification |

A cancelled reply is saved to the thread as far as it got, and the next question
follows it. On exit `sst ask` prints the agents, the turns, the tokens they used,
and how many scratch objects were removed.

## Asking once

With a question, one turn runs. On a terminal its answer streams to stdout as it
is generated; otherwise, or with `--output json`, the answer is reported once
the turn ends. `--output json` carries the answer, the run's status and id, the
tokens used, what was copied, and each tool call and result, with the SQL and
the semantic view it queried. `--show-tools` prints each tool call and result on
stderr. `-` reads the question from stdin, and `--timeout` (default 600 seconds,
at most 900) cancels a turn still running.

Exit 0 when the agent answered; 1 when the project does not compile, a copy is
refused (`SST-ASK008`), a tool is unreachable (`SST-ASK010`) or the run fails
(`SST-ASK002`); 3 for an empty question, or none without a terminal; 4 when no
agent of that name compiles (`SST-ASK003`); 5 when Snowflake is unreachable or
refuses the session.

## How SST reaches the API

`sst ask` calls the REST API on the session the connector already opened, with
its session token, as the
[Cortex Agent SDK's session-token authentication](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agent-sdk/authentication)
does: `Authorization: Snowflake Token="<token>"`, against the account host with
`_` spelled `-`. Measured on a live account, Snowflake then runs every call as
the session's role and ignores an `X-Snowflake-Role` header, so SST sends none:
the REST calls run as the same role as SQL. A session that expired while you
were idle is renewed by a cheap statement on the connector's session, and the
call is sent once more. Requests go through Python's standard library with
certifi's CA bundle, and honour `HTTPS_PROXY` as the connector does. The
role needs `CORTEX_USER` or `CORTEX_AGENT_USER`, `USAGE` on each tool's
warehouse, and access to each tool's object
([access control](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-setup)).

Each turn is a background run, streamed: what lets Ctrl-C
[cancel](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-run)
it (a synchronous run's cancel is refused as already completed while it still
streams, measured), and a stream that breaks off resumes after the last event's
`sequence_number`. Threads are opened with the origin `sst_ask`
([Threads API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-threads-rest-api)),
which is how `--cleanup` finds them. An event of a type SST does not know is
ignored, as the Run API page asks; measured, and not on that page, a run also
sends `response.suggested_queries` and ends with `done`.

## Unpublished evals

`sst test --suite evals --unpublished` makes the same copies, once per agent, and
runs each question on the agent pointed at them, so its scores reflect your
edits to what the agent calls too; see [unpublished eval
runs](evals.md#unpublished-eval-runs).
