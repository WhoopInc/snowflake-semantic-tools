"""Parse an agent's or CoWork profile's eval dataset file: its questions and what each expects.

A question, ground truth, or expected invocation of the wrong shape is reported and kept in
its place as an empty value, so every later row keeps its index. A row of a CoWork profile's
eval names the subagents it expects with `ground_truth_subagents`; CoWork profiles are a
Snowflake private preview (PrPr) feature:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.evals.readers import (
    optional_bool_field,
    optional_string_field,
    origin_at,
    removed_agent_key,
    required_string,
    string_tuple,
)
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.eval import (
    EvalDataset,
    EvalGroundTruth,
    EvalInvocation,
    EvalQuestion,
    EvalSubagentRef,
)
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions

_SUBAGENTS = "ground_truth_subagents"
_GROUND_TRUTH_KEYS = frozenset(
    (
        "ground_truth_invocations",
        "ground_truth_output",
        "required_filters",
        "immutable",
        "immutable_reason",
        _SUBAGENTS,
    )
)


def parse_dataset(loaded: tuple[str, ParsedYaml], diagnostics: list[Diagnostic]) -> EvalDataset:
    """Parse one eval dataset file, given with its project-relative path.

    `database`, `schema` and `enabled` are recorded as the location keys the file sets, for
    the validator to refuse. The file names no agent: its folder says which it belongs to.

    Diagnostics:
        SST-PRS126: the file writes `agent:`.
        SST-PRS002: `name` or `questions` is absent.
        SST-PRS117: a question row has no `question` or no `ground_truth`.
        SST-PRS003: a field has the wrong type, such as a `questions` that is not a list.
        SST-PRS018: a question or an expected invocation is not a mapping, or an expected
            subagent is not a string.
        SST-PRS019: a ground truth's `immutable` is not a boolean.
        Every code `required_call` lists, for an expected subagent that is not one `agent()` or
            `tool()` call.
    """
    source_file, parsed = loaded
    tree = parsed.tree
    origin = origin_at(parsed, (), source_file)
    diagnostics.extend(removed_agent_key(source_file, parsed))
    name = required_string(tree, "name", source_file, parsed, diagnostics)
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
        description,
        questions,
        tuple(key for key in ("database", "schema", "enabled") if key in tree),
        name,
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
        SST-PRS117: the row has no `question`, or no `ground_truth`; once for each.
        SST-PRS003: `ground_truth` is not a mapping, or a field of the row has the wrong type.
        SST-PRS019: `ground_truth.immutable` is not a boolean.
        Every code `_parse_subagents` lists.
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
    if "question" not in value:
        diagnostics.append(D("SST-PRS117", artifact=source_file, index=index, detail="no question", origin=origin))
    ground_truth_value = value.get("ground_truth")
    if not isinstance(ground_truth_value, dict):
        if ground_truth_value is None:
            diagnostics.append(
                D("SST-PRS117", artifact=source_file, index=index, detail="no ground_truth", origin=origin)
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
        Every code `_parse_subagents` lists.
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
        subagents=_parse_subagents(source_file, parsed, question_index, value, diagnostics),
    )


def _parse_subagents(
    source_file: str,
    parsed: ParsedYaml,
    question_index: int,
    value: Mapping[str, object],
    diagnostics: list[Diagnostic],
) -> tuple[EvalSubagentRef, ...] | None:
    """Parse `ground_truth_subagents`: the `agent()` and `tool()` calls a profile may route to.

    An absent key, or one that is not a list, reads as None, unlike an empty list, which expects
    the profile to decline. An entry that is not one call is reported and left out.

    Diagnostics:
        SST-PRS003: the field is not a list.
        SST-PRS018: an entry is not a string.
        Every code `required_call` lists, for an entry that is not one `agent()` or `tool()`
            call.
    """
    if _SUBAGENTS not in value:
        return None
    field = f"questions[{question_index}].ground_truth.{_SUBAGENTS}"
    raw = value[_SUBAGENTS]
    path = ("questions", question_index, "ground_truth", _SUBAGENTS)
    if not isinstance(raw, list):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="list",
                found=type(raw).__name__,
                origin=origin_at(parsed, path, source_file),
            )
        )
        return None
    functions = reference_functions(f"eval_dataset.{_SUBAGENTS}")
    found: list[EvalSubagentRef] = []
    for index, item in enumerate(raw):
        origin = origin_at(parsed, (*path, index), source_file)
        if not isinstance(item, str):
            diagnostics.append(
                D(
                    "SST-PRS018",
                    artifact=source_file,
                    field=field,
                    index=index,
                    expected="agent() or tool() string",
                    found=type(item).__name__,
                    origin=origin,
                )
            )
            continue
        call = required_call(item, functions, origin, field=field, artifact=source_file)
        if isinstance(call, Diagnostic):
            diagnostics.append(call)
            continue
        found.append(EvalSubagentRef(origin, call.function, str(call.args[0])))
    return tuple(found)


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
