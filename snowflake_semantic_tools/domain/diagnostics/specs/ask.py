"""Unpublished-run codes (ASK): `sst ask`, and evals run against an agent's edit before it publishes.

Both run the agent's compiled specification without publishing it, so what they report is
about that run: an agent that answered with an error, a name that compiles no agent, the
published objects the edited agent still calls, and what an unpublished eval run could and
could not score.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Unpublished runs"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-ASK001",
        Severity.WARNING,
        "Agent calls the published dependency, not your edit",
        "{artifact} calls the published {value}, not your edit: {detail}",
        "publish it first (`sst plan`, then `sst apply`) to try the two together",
        condition=(
            "a semantic view, tool, skill, plugin or agent the agent's specification names compiles "
            "to a fingerprint the target's state does not record for it, or the state cannot be read"
        ),
        note=(
            "Reported where nothing stands in for your edit: under `sst ask --published`, and for "
            "an unpublished eval run. Otherwise `sst ask` runs the agent on a scratch copy of each "
            "such dependency, so your edit is what it calls."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-ASK002",
        Severity.ERROR,
        "Unpublished agent run returned an error",
        "{artifact} returned an error: {detail}",
        "fix what the message names, then run it again",
        condition="the temporary agent answered a question with an error message instead of a reply",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK003",
        Severity.ERROR,
        "No such agent",
        "no agent named '{name}' compiles in this project; it compiles {detail}",
        "name an agent `sst list agents` lists",
        condition="`sst ask` names an agent that is not among the agents the project compiles",
    ),
    spec(
        "SST-ASK004",
        Severity.WARNING,
        "Metric cannot be scored unpublished",
        "eval '{artifact}': metric '{name}' is not scored without publishing: {reason}",
        "run the published eval suite, `sst test --suite evals`, to score it",
        condition=(
            "an unpublished eval run cannot judge a custom metric whose prompt uses a placeholder "
            "only a published run's trace fills (`start_timestamp`, `span_*`, `llm_model`), or that "
            "has no prompt, score bands or judge model; or `answer_correctness`, when neither the "
            "eval's `judge_model` nor the agent's orchestration model names a model"
        ),
    ),
    spec(
        "SST-ASK005",
        Severity.INFO,
        "System metric is not scored unpublished",
        "eval '{artifact}': system metric '{name}' is not scored without publishing",
        "run the published eval suite, `sst test --suite evals`, to score it",
        condition=(
            "an unpublished eval run's config enables `tool_execution_accuracy` or "
            "`logical_consistency`, which only Snowflake's evaluation service scores"
        ),
    ),
    spec(
        "SST-ASK006",
        Severity.WARNING,
        "Judge gave no score",
        "eval '{artifact}': the judge of '{name}' gave no score for question {index}: {detail}",
        "run it again; a judge that keeps failing names a model the account cannot use",
        condition=(
            "an unpublished eval run's judge completion failed, or answered no number on the "
            "metric's scale; the metric is unscored on that question"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK007",
        Severity.ERROR,
        "Unpublished eval fell outside a threshold",
        "eval '{artifact}': {name} {expected}, and {count} of {total} question(s) fell short: {detail}",
        "fix the agent until the metric passes, or the expectation if it is wrong",
        condition=(
            "in an unpublished eval run, an exact metric -- `tool_selection_accuracy` -- scored a "
            "question outside the metric's threshold"
        ),
        note=(
            "Only exact metrics gate an unpublished run. A judged metric is approximate: its "
            "scores are reported, and never fail the run."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK008",
        Severity.ERROR,
        "Scratch copy could not be created",
        "{artifact}: the scratch copy {value} could not be created: {detail}",
        "grant the role the CREATE privilege the message names in the scratch schema, or run `sst ask --published`",
        condition=(
            "`sst ask` copies each dependency your edit changed into `agents.+scratch_schema`, and "
            "Snowflake refused a copy's statement; the copies made before it are dropped"
        ),
        note="Nothing is published: the copies are throwaway objects named `<NAME>__SST_ASK_<id>`.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK009",
        Severity.WARNING,
        "Scratch copy or thread left behind",
        "{value} could not be removed: {detail}",
        "run `sst ask --cleanup`, which removes what an earlier session left",
        condition="ending a session, `sst ask` could not drop a scratch copy or delete its thread",
        note="Each session also removes the copies and threads older sessions left, after a while.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK010",
        Severity.ERROR,
        "Agent cannot reach a tool",
        "{artifact} cannot reach a tool: {detail}",
        "grant the role access to the object the message names, then ask again",
        condition=(
            "the run sets `orchestration.tool_not_accessible: reject`, as a run on scratch copies "
            "does, and Snowflake refused it because a tool's object is missing or the role cannot use it"
        ),
        note=(
            "Without `reject`, an agent answers without a tool it cannot reach, and says nothing; "
            "with it, the refusal names each such tool."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK011",
        Severity.ERROR,
        "No such conversation theme",
        "`ask.theme` names no theme: '{value}'; the themes are {detail}",
        "set `ask.theme` to one of the themes the message lists, or remove it for `sst-dark`",
        condition=(
            "a conversation opens and the configuration's `ask.theme` names a theme the full-screen "
            "conversation does not have"
        ),
        note="`--theme` and `$SST_ASK_THEME`, which win over the key, are usage errors instead (exit 3).",
    ),
    spec(
        "SST-ASK012",
        Severity.WARNING,
        "Transcript could not be written",
        "the transcript {value} could not be written: {detail}",
        "make `sst_target/ask/` writable, or ask without `--transcript`",
        condition="`sst ask --transcript` could not replace its transcript file in `sst_target/ask/`",
        note="Reported once; the conversation goes on, and each later turn tries the write again.",
        precheck=Precheck.RUNTIME,
    ),
)
