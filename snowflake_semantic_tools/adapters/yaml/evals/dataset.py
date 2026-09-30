"""Parse an agent's eval dataset file: its questions and what each one expects.

A question, ground truth, or expected invocation of the wrong shape is reported and kept in
its place as an empty value, so every later row keeps its index.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from ....domain.model.diagnostic import D, Diagnostic
from ....domain.model.eval import EvalDataset, EvalGroundTruth, EvalInvocation, EvalQuestion
from ..documents import ParsedYaml
from .readers import optional_bool_field, optional_string_field, origin_at, required_string, string_tuple

_GROUND_TRUTH_KEYS = frozenset(
    (
        "ground_truth_invocations",
        "ground_truth_output",
        "required_filters",
        "immutable",
        "immutable_reason",
    )
)


def parse_dataset(loaded: tuple[str, ParsedYaml], diagnostics: list[Diagnostic]) -> EvalDataset:
    """Parse one eval dataset file, given with its project-relative path.

    `database`, `schema` and `enabled` are recorded as the location keys the file sets, for
    the validator to refuse.

    Diagnostics:
        SST-PRS002: `agent`, `questions`, or a question's `ground_truth` is absent.
        SST-PRS003: a field has the wrong type, such as a `questions` that is not a list.
        SST-PRS018: a question or an expected invocation is not a mapping.
        SST-PRS019: a ground truth's `immutable` is not a boolean.
    """
    source_file, parsed = loaded
    tree = parsed.tree
    origin = origin_at(parsed, (), source_file)
    agent = required_string(tree, "agent", source_file, parsed, diagnostics)
    description = optional_string_field(tree, "description", source_file, parsed, diagnostics)
    questions_value = tree.get("questions")
    if questions_value is None:
        diagnostics.append(D("SST-PRS002", artifact=source_file, field="questions", origin=origin))
        questions: tuple[EvalQuestion, ...] = ()
    elif not isinstance(questions_value, list):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="questions",
                expected="list",
                found=type(questions_value).__name__,
                origin=origin_at(parsed, ("questions",), source_file),
            )
        )
        questions = ()
    else:
        questions = tuple(
            _parse_question(source_file, parsed, index, value, diagnostics)
            for index, value in enumerate(questions_value)
        )
    return EvalDataset(
        origin,
        source_file,
        agent,
        description,
        questions,
        tuple(key for key in ("database", "schema", "enabled") if key in tree),
    )


def _parse_question(
    source_file: str,
    parsed: ParsedYaml,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalQuestion:
    """Parse one question row; a row that is not a mapping is kept as an empty question.

    Diagnostics:
        SST-PRS018: the row, or one of its expected invocations, is not a mapping.
        SST-PRS002: `ground_truth` is absent.
        SST-PRS003: `ground_truth` is not a mapping, or a field of the row has the wrong type.
        SST-PRS019: `ground_truth.immutable` is not a boolean.
    """
    origin = origin_at(parsed, ("questions", index), source_file)
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                artifact=source_file,
                field="questions",
                index=index,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return EvalQuestion(origin, None, None)
    question = optional_string_field(value, "question", source_file, parsed, diagnostics, ("questions", index))
    ground_truth_value = value.get("ground_truth")
    if not isinstance(ground_truth_value, dict):
        if ground_truth_value is None:
            diagnostics.append(
                D("SST-PRS002", artifact=source_file, field=f"questions[{index}].ground_truth", origin=origin)
            )
        else:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"questions[{index}].ground_truth",
                    expected="mapping",
                    found=type(ground_truth_value).__name__,
                    origin=origin,
                )
            )
        ground_truth = None
    else:
        ground_truth = _parse_ground_truth(source_file, parsed, index, ground_truth_value, diagnostics)
    return EvalQuestion(origin, question, ground_truth)


def _parse_ground_truth(
    source_file: str,
    parsed: ParsedYaml,
    question_index: int,
    value: Mapping[str, object],
    diagnostics: list[Diagnostic],
) -> EvalGroundTruth:
    """Parse a question's ground truth, keeping each key SST does not read in `extra`.

    An absent `ground_truth_invocations` reads as None, unlike an empty list, which expects
    that no tool is called.

    Diagnostics:
        SST-PRS003: `ground_truth_invocations` is not a list, `required_filters` is not a list
            of strings, or a string field is not a non-empty string.
        SST-PRS018: an expected invocation is not a mapping.
        SST-PRS019: `immutable` is not a boolean.
    """
    path = ("questions", question_index, "ground_truth")
    origin = origin_at(parsed, path, source_file)
    invocations: tuple[EvalInvocation, ...] | None = None
    if "ground_truth_invocations" in value:
        raw_invocations = value["ground_truth_invocations"]
        if not isinstance(raw_invocations, list):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"questions[{question_index}].ground_truth.ground_truth_invocations",
                    expected="list",
                    found=type(raw_invocations).__name__,
                    origin=origin,
                )
            )
        else:
            invocations = tuple(
                _parse_invocation(source_file, parsed, question_index, index, item, diagnostics)
                for index, item in enumerate(raw_invocations)
            )
    required_filters = string_tuple(
        value.get("required_filters"),
        f"questions[{question_index}].ground_truth.required_filters",
        origin,
        diagnostics,
    )
    return EvalGroundTruth(
        origin=origin,
        invocations=invocations,
        output=optional_string_field(value, "ground_truth_output", source_file, parsed, diagnostics, path),
        required_filters=required_filters,
        immutable=optional_bool_field(value, "immutable", source_file, origin, diagnostics),
        immutable_reason=optional_string_field(value, "immutable_reason", source_file, parsed, diagnostics, path),
        extra=MappingProxyType({str(key): item for key, item in value.items() if key not in _GROUND_TRUTH_KEYS}),
    )


def _parse_invocation(
    source_file: str,
    parsed: ParsedYaml,
    question_index: int,
    invocation_index: int,
    value: object,
    diagnostics: list[Diagnostic],
) -> EvalInvocation:
    """Parse one expected tool invocation; one that is not a mapping is kept as an empty one.

    Diagnostics:
        SST-PRS018: the invocation is not a mapping.
        SST-PRS003: `tool_name`, `tool_input` or `tool_output` is not a non-empty string.
    """
    path = ("questions", question_index, "ground_truth", "ground_truth_invocations", invocation_index)
    origin = origin_at(parsed, path, source_file)
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                artifact=source_file,
                field=f"questions[{question_index}].ground_truth.ground_truth_invocations",
                index=invocation_index,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return EvalInvocation(origin)
    return EvalInvocation(
        origin,
        optional_string_field(value, "tool_name", source_file, parsed, diagnostics, path),
        optional_string_field(value, "tool_input", source_file, parsed, diagnostics, path),
        optional_string_field(value, "tool_output", source_file, parsed, diagnostics, path),
    )
