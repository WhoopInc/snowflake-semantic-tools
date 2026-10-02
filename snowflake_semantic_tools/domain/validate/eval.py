"""Validate an eval catalog statically, without consulting Snowflake or persisted state.

The result lists what the catalog already carries, then every custom metric's diagnostics
in catalog order, then each eval's: agent agreement, keys the eval files may not set, the
dataset, then the config. Evals are checked in catalog order because their dataset and
run names are claimed in maps they share.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval.model import (
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDefaults,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.resolve.eval_name import probe_name
from snowflake_semantic_tools.domain.validate.eval_config import validate_eval_config
from snowflake_semantic_tools.domain.validate.eval_dataset import validate_eval_dataset
from snowflake_semantic_tools.domain.validate.eval_metric import validate_custom_metric


def validate_eval_catalog(
    catalog: EvalCatalog,
    *,
    agent_tool_names: Mapping[str, Iterable[str]] | None = None,
    allowed_models: Iterable[str] = (),
) -> DiagnosticBag:
    """Validate the static eval contract without consulting Snowflake or persisted state.

    ``agent_tool_names`` is the compiler projection of exact trace-facing names. The
    validator accepts the projection instead of reproducing name resolution here.

    Args:
        agent_tool_names: Each agent's exact tool names, keyed by agent name, compared
            casefolded. None skips every tool check; an agent absent from the mapping, one
            that did not compile, skips its own.
        allowed_models: The judge models a custom metric may name, compared casefolded;
            empty allows any explicit model.

    Diagnostics:
        SST-VAL001: two custom metrics share a name.
        SST-VAL737: a custom metric takes a system metric's name.
        SST-VAL733: a threshold on an ungated metric, or a gated one without a usable bound.
        SST-VAL738: a custom metric's judge model is absent or `auto`.
        SST-VAL739: a custom metric's judge model is not allowed.
        SST-VAL740: a judge prompt reads as a system metric's intent.
        SST-VAL741: a judge prompt declares no output contract.
        SST-VAL742: a judge prompt asks for reasoning with no parseable score.
        SST-VAL743: a rubric has no tie-break or insufficient-information branch.
        SST-PRS114: score bands are inverted, or leave a gap or overlap.
        SST-VAL748: an invariant check declares a scale wider than three values.
        SST-VAL747: a judge prompt can produce a score outside the declared bands.
        SST-PRS115: a custom metric's default threshold is not usable within its bands.
        SST-PRS116: a judge prompt uses an unsupported placeholder.
        SST-REF012: the dataset or config names another agent, or they disagree.
        SST-VAL703: the dataset or config sets `database`, `schema` or `enabled`.
        SST-VAL705: a dataset row has no question, or nothing to score against.
        SST-VAL707: a question repeats one of the agent's sample questions.
        SST-PRS015: `immutable` and `immutable_reason` are not declared together, or an
            expected output with a number lacks them.
        SST-VAL708: an expected invocation names a tool the agent does not have.
        SST-VAL709: an expected web tool is not named `web_search`.
        SST-VAL706: a question or expectation contains a relative date.
        SST-VAL710: the dataset has fewer rows than `evals.+min_dataset_rows`.
        SST-VAL711: how many of the agent's tools no row expects, when any (info).
        SST-VAL712: CREATE DATASET takes no properties (info).
        SST-PRS002: the dataset name template has no agent token.
        SST-VAL701: two evals render the same dataset name.
        SST-VAL702: a rendered dataset name is longer than 128 characters.
        SST-VAL719: the effective agent version is LIVE, missing, or not a pinned form.
        SST-VAL721: a metric is neither a system metric nor a resolved custom metric.
        SST-VAL762: a dataset template is not set.
        SST-PRS010: the rendered source table name is empty or longer than 128 characters.
        SST-PRS101: the config declares no metric.
        SST-VAL722: a system metric's version is not pinned.
        SST-PRS013: a system metric version other than v3, or an accepted status other than
            COMPLETED.
        SST-VAL723: a system metric pins a legacy version.
        SST-VAL724: a system metric sets judge_model.
        SST-VAL725: tool_selection_accuracy uses no LLM judge (info).
        SST-VAL726: logical_consistency is gated.
        SST-VAL718: the rendered run name does not include the commit SHA.
        SST-VAL717: two evals of one agent render the same run name.
        SST-PRS016: retry, concurrency or baseline_runs is below its minimum.
        SST-VAL731: run concurrency exceeds `evals.+concurrency`.
        SST-VAL735: a threshold is set with no baseline runs.
        SST-VAL732: the agent has tool types an eval run skips (info).
    """

    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    tool_names = {name.casefold(): frozenset(values) for name, values in (agent_tool_names or {}).items()}
    allowed = frozenset(model.casefold() for model in allowed_models)
    name_counts = Counter(metric.name.casefold() for metric in catalog.metrics)
    for metric in catalog.metrics:
        diagnostics.extend(validate_custom_metric(metric, name_counts, allowed))
    dataset_names: dict[str, str] = {}
    run_names: dict[tuple[str, str], str] = {}
    for resolved in catalog.evals:
        # An agent that did not compile has no projection. Its own diagnostics say why;
        # checking the dataset against an empty tool set would only repeat that failure as
        # one "absent tool" per expected invocation.
        agent_tools = tool_names.get(resolved.agent.name.casefold())
        diagnostics.extend(_validate_eval(resolved, catalog.defaults, agent_tools, dataset_names, run_names))
    return DiagnosticBag(tuple(diagnostics))


def eval_placement(resolved: ResolvedEval, agent_target: QualifiedName) -> tuple[Diagnostic, ...]:
    """Report each eval object template that qualifies itself into another schema than its agent's.

    An eval's dataset and source table live in its agent's schema, so that a scratch agent's
    evals land in scratch. A template that renders a bare name is placed there; one that
    renders `<schema>.<name>` or `<database>.<schema>.<name>` must name that same schema.

    Diagnostics:
        SST-VAL704: a dataset or source table template names another database or schema.
    """
    dataset = resolved.config.dataset
    if dataset is None:
        return ()
    expected = (agent_target.database.sql, agent_target.schema.sql)
    diagnostics: list[Diagnostic] = []
    for template in (dataset.name_template, dataset.source_table_template):
        rendered = probe_name(template, resolved.agent.name) if template is not None else None
        parts = rendered.split(".") if rendered is not None else []
        if len(parts) not in (2, 3):
            continue
        found = (expected[0], parts[0]) if len(parts) == 2 else (parts[0], parts[1])
        if tuple(part.casefold() for part in found) != tuple(part.casefold() for part in expected):
            diagnostics.append(
                D(
                    "SST-VAL704",
                    artifact=rendered,
                    found=".".join(found),
                    value=resolved.agent.name,
                    expected=".".join(expected),
                    origin=resolved.config.origin,
                    subject=resolved.key,
                )
            )
    return tuple(diagnostics)


def _validate_eval(
    resolved: ResolvedEval,
    defaults: EvalDefaults,
    tool_names: frozenset[str] | None,
    dataset_names: dict[str, str],
    run_names: dict[tuple[str, str], str],
) -> tuple[Diagnostic, ...]:
    return (
        *_agent_agreement(resolved),
        *_forbidden_keys(resolved),
        *validate_eval_dataset(resolved, defaults, tool_names, dataset_names),
        *validate_eval_config(resolved, defaults, run_names),
    )


def _agent_agreement(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    # Each file must name the eval's agent, compared casefolded. Separately, a dataset and a
    # config that name different agents are reported against the config, whichever is wrong.
    dataset = resolved.dataset
    config = resolved.config
    agent_name = resolved.agent.name.casefold()
    diagnostics = [
        D("SST-REF012", name=declared, origin=origin, subject=resolved.key)
        for declared, origin in ((dataset.agent, dataset.origin), (config.agent, config.origin))
        if declared is not None and declared.casefold() != agent_name
    ]
    if dataset.agent and config.agent and dataset.agent.casefold() != config.agent.casefold():
        diagnostics.append(D("SST-REF012", name=config.agent, origin=config.origin, subject=resolved.key))
    return tuple(diagnostics)


def _forbidden_keys(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    files: tuple[EvalDataset | EvalConfig, ...] = (resolved.dataset, resolved.config)
    return tuple(
        D("SST-VAL703", artifact=file.source_file, key=key, origin=file.origin, subject=resolved.key)
        for file in files
        for key in file.forbidden_keys
    )
