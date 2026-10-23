"""Validation codes 770-789 (VAL): comparing orchestration models on an eval's published subject.

`sst eval --compare-models` runs an eval on a version of its agent or CoWork
profile per candidate model: which models it may compare, and what its report notices about the
cost it read and the models it leaves out of its verdict.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL770",
        Severity.ERROR,
        "Comparison candidate is auto",
        "eval '{artifact}': {field} names `auto`, which is no model a comparison can run",
        "name the orchestration models to compare, such as `claude-opus-4-8`; `auto` lets "
        "Snowflake pick one per request, so a version on it compares nothing",
        condition=(
            "`evals.+candidate_models`, a folder route's, an eval's `config.candidate_models`, or "
            "`sst eval --compare-models`, names `auto`, compared casefolded"
        ),
    ),
    spec(
        "SST-VAL771",
        Severity.ERROR,
        "Comparison candidate is not an allowed orchestration model",
        "eval '{artifact}': {field} names '{model}', which snowflake.orchestration_models does not list",
        "add the model to snowflake.orchestration_models, or compare only models it lists",
        condition=(
            "`snowflake.orchestration_models` is set, and a candidate model of an eval's comparison "
            "is not one of them, compared casefolded"
        ),
        note=(
            "A comparison commits a version of the published agent or CoWork profile on each candidate, "
            "so it is held to the list an agent's own model is (SST-VAL543)."
        ),
    ),
    spec(
        "SST-VAL772",
        Severity.ERROR,
        "Comparison has no candidate model",
        "eval '{artifact}' names no candidate model to compare with its pinned one",
        "list the models in `evals.+candidate_models`, or give them as `--compare-models <model>,<model>`",
        condition=(
            "`sst eval --compare-models` was given no list, and the eval's `candidate_models` setting, "
            "from its config, a folder route, or the `evals:` root, names none"
        ),
    ),
    spec(
        "SST-VAL773",
        Severity.INFO,
        "Comparison cost not landed yet",
        "eval '{artifact}': the usage of {count} model(s)' runs has not landed in Snowflake's usage views yet, "
        "so their cost is pending",
        "run `sst eval --refresh-cost {path}` in about an hour to fill it in",
        condition=(
            "a comparison read its cost before every agent request, judge call, or SQL credit its runs "
            "made had a row in ACCOUNT_USAGE"
        ),
        note=(
            "Snowflake documents CORTEX_AGENT_USAGE_HISTORY as lagging up to an hour, and its SQL credits "
            "up to eight; the report marks each value it could not read yet `pending`."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL774",
        Severity.INFO,
        "Comparison cost unavailable",
        "eval '{artifact}': the cost of {count} model(s)' runs is unavailable: {reason}",
        "read the cost with a role that may read SNOWFLAKE.ACCOUNT_USAGE and the AI observability events, "
        "as `sst eval --refresh-cost {path}`; the scores stand without it",
        condition=(
            "reading CORTEX_AGENT_USAGE_HISTORY, the evaluation's eval spans, or CORTEX_REST_API_USAGE_HISTORY "
            "was refused, or nothing named the requests to read"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL775",
        Severity.INFO,
        "Comparison candidate is not generally available",
        "eval '{artifact}': candidate '{model}' is {kind}, so the verdict reports it but never names it",
        "compare generally available models to choose one to pin",
        condition=(
            "SHOW CORTEX BASE MODELS lists a candidate model as in preview (PUPR or PRPR), INTERNAL, LEGACY "
            "or past end-of-life"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL776",
        Severity.INFO,
        "CoWork profile cost is its router's",
        "eval '{artifact}': the CoWork profile's cost is its router's requests alone; the subagent runs "
        "they routed to are not attributed",
        "compare models on each subagent's own eval for what the subagents cost",
        condition="a comparison read the cost of a CoWork profile's eval",
        note=(
            "Snowflake private preview: CoWork profiles. CORTEX_AGENT_USAGE_HISTORY records the router's "
            "request for each question; each subagent call it makes is a request of its own, which nothing "
            "in the run names."
        ),
    ),
)
