"""Validate an eval catalog statically, without consulting Snowflake or persisted state.

The result lists what the catalog already carries, then every custom metric's diagnostics
in catalog order, then each eval's: keys the eval files may not set, the dataset, the config,
then the routing fields its subject's kind allows (`eval_subject`). Evals are checked in
catalog order because their dataset and run names are claimed in maps they share.
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
from snowflake_semantic_tools.domain.validate.eval_subject import subject_rules


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
            that did not compile, skips its own. A CoWork profile's eval has no tool check:
            its rows expect subagents, not tool calls.
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
        SST-VAL888: a CoWork profile's config declares no metric once the inherited tool metrics
            blind to its router are left out.
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
        SST-VAL731: an eval's concurrency setting exceeds the `evals.+concurrency` root.
        SST-VAL735: a threshold is set with no baseline runs.
        SST-VAL732: the agent has tool types an eval run skips (info).
        Every code `subject_rules` lists.
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
        agent_tools = None if resolved.subject.is_profile else tool_names.get(resolved.name.casefold())
        diagnostics.extend(_validate_eval(resolved, catalog.defaults, agent_tools, dataset_names, run_names))
    return DiagnosticBag(tuple(diagnostics))


def eval_placement(resolved: ResolvedEval, agent_target: QualifiedName) -> tuple[Diagnostic, ...]:
    """Report each eval object template that qualifies itself into another schema than its subject's.

    An eval's dataset and source table live in the schema of the agent, or CoWork profile, it
    evaluates, so that a scratch agent's evals land in scratch. A template that renders a bare
    name is placed there; one that renders `<schema>.<name>` or `<database>.<schema>.<name>`
    must name that same schema.

    Diagnostics:
        SST-VAL704: a dataset or source table template names another database or schema.
    """
    dataset = resolved.config.dataset
    if dataset is None:
        return ()
    expected = (agent_target.database.sql, agent_target.schema.sql)
    diagnostics: list[Diagnostic] = []
    for template in (dataset.name_template, dataset.source_table_template):
        rendered = probe_name(template, resolved.name) if template is not None else None
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
                    value=resolved.name,
                    expected=".".join(expected),
                    origin=resolved.config.origin,
                    subject=resolved.key,
                )
            )
    return tuple(diagnostics)


# The prefix of a personal database's name: Snowflake gives each user `USER$<user>`.
PERSONAL_DATABASE_PREFIX = "USER$"


def personal_database_placement(
    resolved: ResolvedEval, agent_target: QualifiedName, *, mints: bool
) -> tuple[Diagnostic, ...]:
    """Report an eval whose objects would be created in a personal database, which holds none of them.

    An eval's source table, dataset and config stage live in its agent's schema, so an agent
    routed into `USER$<user>` takes them there. The agent alone may live there.

    Args:
        mints: SST creates the source table and dataset, as `dataset.mint: auto` does; with
            `never`, only the config stage is created and the dataset must already exist.

    Diagnostics:
        SST-VAL766: the agent's database is a personal one.
    """
    if not agent_target.database.folded.upper().startswith(PERSONAL_DATABASE_PREFIX):
        return ()
    kind = "its source table, dataset and config stage" if mints else "its config stage"
    found = f"{agent_target.database.sql}.{agent_target.schema.sql}"
    return (
        D(
            "SST-VAL766",
            artifact=resolved.name,
            kind=kind,
            found=found,
            origin=resolved.config.origin,
            subject=resolved.key,
        ),
    )


def _validate_eval(
    resolved: ResolvedEval,
    defaults: EvalDefaults,
    tool_names: frozenset[str] | None,
    dataset_names: dict[str, str],
    run_names: dict[tuple[str, str], str],
) -> tuple[Diagnostic, ...]:
    return (
        *_forbidden_keys(resolved),
        *validate_eval_dataset(resolved, defaults, tool_names, dataset_names),
        *validate_eval_config(resolved, defaults, run_names),
        *subject_rules(resolved),
    )


def _forbidden_keys(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    files: tuple[EvalDataset | EvalConfig, ...] = (resolved.dataset, resolved.config)
    return tuple(
        D("SST-VAL703", artifact=file.source_file, key=key, origin=file.origin, subject=resolved.key)
        for file in files
        for key in file.forbidden_keys
    )
