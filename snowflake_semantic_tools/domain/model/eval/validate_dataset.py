"""Check an eval's dataset: each row, the row count, tool coverage, and the dataset's name.

Rows are checked in order, each rule of a row before the next row. The dataset name is
claimed in a map shared by every eval of the catalog, so the evals must be checked in
catalog order: a name that collides is reported on the eval that claims it second.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic
from snowflake_semantic_tools.domain.model.eval.model import EvalDefaults, EvalGroundTruth, EvalQuestion, ResolvedEval
from snowflake_semantic_tools.domain.model.eval.naming import NAME_LIMIT, has_agent_token, probe_name
from snowflake_semantic_tools.domain.model.validation import Emitter

_RELATIVE_DATE = re.compile(
    r"\b(?:last|this|current|recent)\s+(?:day|week|month|quarter|year)\b|"
    r"\b(?:ytd|mtd|yesterday|today|tomorrow|q[1-4])\b|"
    r"\b(?:first|second|third|fourth)\s+quarter\b",
    re.IGNORECASE,
)
_NUMBER = re.compile(r"(?<![A-Za-z0-9_])[-+]?\d+(?:\.\d+)?%?(?![A-Za-z0-9_])")


def validate_eval_dataset(
    resolved: ResolvedEval,
    defaults: EvalDefaults,
    tool_names: frozenset[str] | None,
    dataset_names: dict[str, str],
) -> tuple[Diagnostic, ...]:
    """Check one eval's dataset, and claim its rendered name in `dataset_names`.

    Args:
        tool_names: The agent's exact trace-facing tool names, or None when the compiler
            projected none for it; every tool check is then skipped.
        dataset_names: Rendered dataset names claimed so far, casefolded, each mapped to
            the eval that claimed it; this eval's name is added when it is free.

    Diagnostics:
        SST-VAL705: a row has no question, or nothing to score against.
        SST-VAL707: a question repeats one of the agent's sample questions exactly.
        SST-PRS015: `immutable` and `immutable_reason` are not declared together, or an
            expected output with a number lacks them.
        SST-VAL708: an expected invocation names a tool the agent does not have.
        SST-VAL709: an expected web tool is not named `web_search`.
        SST-VAL706: a question or its expectation contains a relative date.
        SST-VAL710: the dataset has fewer rows than `evals.+min_dataset_rows`.
        SST-VAL711: how many of the agent's tools no row expects (info).
        SST-VAL712: CREATE DATASET takes no properties (info, for every dataset).
        SST-PRS002: the dataset name template has no agent token.
        SST-VAL701: another eval already claimed the rendered dataset name.
        SST-VAL702: the rendered dataset name is longer than 128 characters.
    """
    return (
        *(
            diagnostic
            for index, row in enumerate(resolved.dataset.questions)
            for diagnostic in _row(resolved, index, row, tool_names)
        ),
        *_row_floor(resolved, defaults),
        *_tool_coverage(resolved, tool_names),
        D("SST-VAL712", artifact=resolved.dataset.source_file, origin=resolved.dataset.origin, subject=resolved.key),
        *_dataset_name(resolved, dataset_names),
    )


def _row(
    resolved: ResolvedEval, index: int, row: EvalQuestion, tool_names: frozenset[str] | None
) -> tuple[Diagnostic, ...]:
    return (
        *_row_completeness(resolved, index, row),
        *_immutable_ground_truth(resolved, index, row.ground_truth),
        *_expected_tools(resolved, index, row.ground_truth, tool_names),
        *_relative_date(resolved, index, row),
    )


def _row_completeness(resolved: ResolvedEval, index: int, row: EvalQuestion) -> tuple[Diagnostic, ...]:
    emit = Emitter(subject=resolved.key, origin=row.origin, artifact=resolved.dataset.source_file, index=index)
    ground_truth = row.ground_truth
    if not row.question or ground_truth is None or not ground_truth.has_expectation:
        emit("SST-VAL705", detail="no question" if not row.question else "no expected field")
    if row.question in resolved.agent.sample_questions:
        emit("SST-VAL707")
    return emit.diagnostics


def _immutable_ground_truth(
    resolved: ResolvedEval, index: int, ground_truth: EvalGroundTruth | None
) -> tuple[Diagnostic, ...]:
    """Check that `immutable` and `immutable_reason` come together, and that a numeric output has both."""
    if ground_truth is None:
        return ()
    emit = Emitter(subject=resolved.key, origin=ground_truth.origin, artifact=resolved.dataset.source_file)
    field = f"questions[{index}].ground_truth"
    if ground_truth.immutable_reason and not ground_truth.immutable:
        emit("SST-PRS015", field=f"{field}.immutable_reason", other="immutable: true")
    if ground_truth.immutable and not ground_truth.immutable_reason:
        emit("SST-PRS015", field=f"{field}.immutable", other="immutable_reason")
    if ground_truth.output and _NUMBER.search(ground_truth.output):
        if not ground_truth.immutable or not ground_truth.immutable_reason:
            emit("SST-PRS015", field=f"{field}.ground_truth_output", other="immutable: true plus immutable_reason")
    return emit.diagnostics


def _expected_tools(
    resolved: ResolvedEval,
    index: int,
    ground_truth: EvalGroundTruth | None,
    tool_names: frozenset[str] | None,
) -> tuple[Diagnostic, ...]:
    """Check each expected invocation against the agent's exact tool names; skipped without them."""
    if ground_truth is None or ground_truth.invocations is None or tool_names is None:
        return ()
    emit = Emitter(subject=resolved.key, artifact=resolved.dataset.source_file, index=index)
    has_web_search = any(name.casefold() == "web_search" for name in tool_names)
    for invocation in ground_truth.invocations:
        name = invocation.tool_name
        if name is None:
            continue
        if name not in tool_names:
            emit("SST-VAL708", name=name, value=resolved.agent.name, origin=invocation.origin)
        if has_web_search and "web" in name.casefold() and name != "web_search":
            emit("SST-VAL709", name=name, origin=invocation.origin)
    return emit.diagnostics


def _relative_date(resolved: ResolvedEval, index: int, row: EvalQuestion) -> tuple[Diagnostic, ...]:
    # Only the first relative date in a row is reported, searching the question first.
    texts = [row.question or ""]
    if row.ground_truth is not None:
        texts.extend(_ground_truth_text(row.ground_truth))
    for text in texts:
        match = _RELATIVE_DATE.search(text)
        if match is not None:
            return (
                D(
                    "SST-VAL706",
                    artifact=resolved.dataset.source_file,
                    index=index,
                    value=match.group(0),
                    origin=row.origin,
                    subject=resolved.key,
                ),
            )
    return ()


def _ground_truth_text(value: EvalGroundTruth) -> tuple[str, ...]:
    parts = [value.output or "", *value.required_filters]
    if value.invocations is not None:
        for invocation in value.invocations:
            parts.extend((invocation.tool_input or "", invocation.tool_output or ""))
    parts.extend(str(extra) for extra in value.extra.values())
    return tuple(parts)


def _row_floor(resolved: ResolvedEval, defaults: EvalDefaults) -> tuple[Diagnostic, ...]:
    dataset = resolved.dataset
    if defaults.min_dataset_rows is None or len(dataset.questions) >= defaults.min_dataset_rows:
        return ()
    return (
        D(
            "SST-VAL710",
            artifact=dataset.source_file,
            count=len(dataset.questions),
            expected=defaults.min_dataset_rows,
            origin=dataset.origin,
            subject=resolved.key,
        ),
    )


def _tool_coverage(resolved: ResolvedEval, tool_names: frozenset[str] | None) -> tuple[Diagnostic, ...]:
    if tool_names is None:
        return ()
    dataset = resolved.dataset
    uncovered = tool_names - _expected_tool_names(dataset.questions)
    return (
        D(
            "SST-VAL711",
            artifact=dataset.source_file,
            value=len(uncovered),
            origin=dataset.origin,
            subject=resolved.key,
        ),
    )


def _expected_tool_names(rows: tuple[EvalQuestion, ...]) -> set[str]:
    names: set[str] = set()
    for row in rows:
        if row.ground_truth is None or row.ground_truth.invocations is None:
            continue
        names.update(
            invocation.tool_name for invocation in row.ground_truth.invocations if invocation.tool_name is not None
        )
    return names


def _dataset_name(resolved: ResolvedEval, dataset_names: dict[str, str]) -> tuple[Diagnostic, ...]:
    config = resolved.config.dataset
    if config is None or config.name_template is None:
        return ()
    emit = Emitter(subject=resolved.key, origin=resolved.config.origin)
    if not has_agent_token(config.name_template):
        emit("SST-PRS002", artifact=resolved.config.source_file, field="dataset.name_template agent token")
    rendered = probe_name(config.name_template, resolved.agent.name)
    if rendered is None:
        return emit.diagnostics
    folded = rendered.casefold()
    other = dataset_names.get(folded)
    if other is not None:
        emit("SST-VAL701", artifact=rendered, value=other)
    else:
        dataset_names[folded] = resolved.name
    if len(rendered) > NAME_LIMIT:
        emit("SST-VAL702", artifact=rendered, size=len(rendered))
    return emit.diagnostics
