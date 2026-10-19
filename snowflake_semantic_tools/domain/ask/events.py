"""The events a streamed Cortex Agents run sends, decoded from server-sent events into types.

`SseDecoder` reads a stream line by line, as the protocol frames it: `event:` names the event,
each `data:` line adds to its payload, a blank line ends it, and a line that starts with `:`
is a comment. `decode` turns one event into the type its name says, as the Run API page lists
them; an event of a type it does not know is `Unknown`, which a caller ignores, as that page
asks. Measured on live runs, and not in that page: each payload carries its `sequence_number`,
which `starting_after` resumes from, rather than an SSE `id:` field; a run sends
`response.suggested_queries` before its final `response`, and ends with `done`, `[DONE]`.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

DONE = "done"


@dataclass(frozen=True, slots=True)
class SseEvent:
    """One server-sent event: its name and its payload, parsed as JSON when it is."""

    name: str
    data: object


class SseDecoder:
    """Decode a server-sent event stream fed one line at a time, the line ending stripped."""

    def __init__(self) -> None:
        self._name = "message"
        self._data: list[str] = []

    def feed(self, line: str) -> SseEvent | None:
        """Read one line; return the event a blank line ends, else None."""
        if line == "":
            if not self._data:
                self._name = "message"
                return None
            text = "\n".join(self._data)
            event = SseEvent(self._name, _parsed(text))
            self._name, self._data = "message", []
            return event
        if line.startswith(":"):
            return None
        name, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if name == "event":
            self._name = value
        elif name == "data":
            self._data.append(value)
        return None


def _parsed(text: str) -> object:
    try:
        return json.loads(text)
    except ValueError:
        return text


@dataclass(frozen=True, slots=True)
class TextDelta:
    """More of the answer's text."""

    text: str
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class ThinkingDelta:
    """More of the agent's reasoning."""

    text: str
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class Status:
    """What the run is doing now, such as `planning` or `executing_tools`, and how it says it."""

    status: str
    message: str
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class ToolUse:
    """A tool call the agent makes: the tool, its type, and its input."""

    tool_use_id: str
    name: str
    type: str
    input: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class ToolProgress:
    """What a tool call is doing now, such as `executing_tool`."""

    tool_use_id: str
    status: str
    message: str
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class ResultTable:
    """A result set: its column names and its rows, each value as Snowflake sent it."""

    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class ToolResult:
    """What a tool call returned.

    Attributes:
        sql: The SQL an Analyst or SQL tool ran; empty for any other.
        table: The rows that SQL returned; None when the result holds none.
        semantic_model: The semantic view an Analyst tool queried, as the result names it.
    """

    tool_use_id: str
    name: str
    type: str
    status: str
    sql: str = ""
    table: ResultTable | None = None
    semantic_model: str = ""
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class Chart:
    """A chart the agent drew, as its Vega-Lite specification."""

    spec: str
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class Warning:
    """A notice the run sends without failing, such as a tool it could not reach."""

    message: str
    code: str = ""
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class RunError:
    """The run failed: Snowflake's code, message and request id."""

    message: str
    code: str = ""
    request_id: str = ""


@dataclass(frozen=True, slots=True)
class MessageIds:
    """A `metadata` event: the id of a message the run stored, whose it is, and the run's id.

    Attributes:
        role: `user` for the question, `assistant` for the reply.
        run_id: The run's id, which cancel and resume take; sent only on a background run's
            user message, so empty otherwise.
    """

    role: str
    message_id: int
    run_id: str = ""
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class Finished:
    """The run's final `response`: how it ended, and the ids the next turn and cancel need.

    Attributes:
        status: `completed`, `cancelled` or `timed_out`.
        tokens: Total input and output tokens the run used, from its usage.
    """

    status: str
    run_id: str = ""
    assistant_message_id: int | None = None
    tokens: int = 0
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class Suggestions:
    """Follow-up questions the agent suggests."""

    queries: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Done:
    """The stream's end."""


@dataclass(frozen=True, slots=True)
class Unknown:
    """An event of a type SST does not know, which a caller ignores."""

    name: str


AgentEvent = (
    TextDelta
    | ThinkingDelta
    | Status
    | ToolUse
    | ToolProgress
    | ToolResult
    | Chart
    | Warning
    | RunError
    | MessageIds
    | Finished
    | Suggestions
    | Done
    | Unknown
)


def decode(event: SseEvent) -> AgentEvent:
    """Return the typed event `event` is, by its name; `Unknown` for a name not listed here.

    A payload that is not an object reads as an empty one, so a field it lacks is empty.
    """
    data: Mapping[str, object] = event.data if isinstance(event.data, dict) else {}
    sequence = _number(data.get("sequence_number"))
    match event.name:
        case "response.text.delta":
            return TextDelta(_text(data.get("text")), sequence)
        case "response.thinking.delta":
            return ThinkingDelta(_text(data.get("text")), sequence)
        case "response.status":
            return Status(_text(data.get("status")), _text(data.get("message")), sequence)
        case "response.tool_use":
            given = data.get("input")
            inputs = MappingProxyType(dict(given)) if isinstance(given, dict) else MappingProxyType({})
            return ToolUse(
                _text(data.get("tool_use_id")), _text(data.get("name")), _text(data.get("type")), inputs, sequence
            )
        case "response.tool_result.status":
            return ToolProgress(
                _text(data.get("tool_use_id")), _text(data.get("status")), _text(data.get("message")), sequence
            )
        case "response.tool_result":
            return _tool_result(data, sequence)
        case "response.chart":
            return Chart(_text(data.get("chart_spec")), sequence)
        case "response.warning":
            return Warning(_text(data.get("message")), _text(data.get("code")), sequence)
        case "error":
            return RunError(_text(data.get("message")), _text(data.get("code")), _text(data.get("request_id")))
        case "metadata":
            return _message_ids(data, sequence)
        case "response":
            return _finished(data, sequence)
        case "response.suggested_queries":
            listed = data.get("suggested_queries")
            queries = listed if isinstance(listed, list) else []
            return Suggestions(tuple(_text(item.get("query")) for item in queries if isinstance(item, dict)))
        case "done":
            return Done()
    return Unknown(event.name)


def _tool_result(data: Mapping[str, object], sequence: int | None) -> ToolResult:
    """Read a tool result's SQL, result set and semantic view from its first JSON content item."""
    content = data.get("content")
    first = content[0] if isinstance(content, list) and content and isinstance(content[0], dict) else {}
    body = first.get("json") if isinstance(first.get("json"), dict) else {}
    assert isinstance(body, dict)
    return ToolResult(
        _text(data.get("tool_use_id")),
        _text(data.get("name")),
        _text(data.get("type")),
        _text(data.get("status")),
        _text(body.get("sql")),
        result_table(body.get("result_set")),
        _text(body.get("semantic_model_path")),
        sequence,
    )


def result_table(value: object) -> ResultTable | None:
    """Read a SQL API result set: its `resultSetMetaData.rowType` names, then its `data` rows."""
    if not isinstance(value, dict):
        return None
    meta = value.get("resultSetMetaData")
    types = meta.get("rowType") if isinstance(meta, dict) else None
    rows = value.get("data")
    if not isinstance(types, list) or not isinstance(rows, list):
        return None
    columns = tuple(_text(item.get("name")) if isinstance(item, dict) else "" for item in types)
    return ResultTable(columns, tuple(tuple(row) if isinstance(row, list) else (row,) for row in rows))


def _message_ids(data: Mapping[str, object], sequence: int | None) -> MessageIds:
    meta = data.get("metadata")
    body: Mapping[str, object] = meta if isinstance(meta, dict) else {}
    return MessageIds(
        _text(body.get("role")), _number(body.get("message_id")) or 0, _text(body.get("run_id")), sequence
    )


def _finished(data: Mapping[str, object], sequence: int | None) -> Finished:
    meta = data.get("metadata")
    body: Mapping[str, object] = meta if isinstance(meta, dict) else {}
    return Finished(
        _text(data.get("status")),
        _text(body.get("run_id")),
        _number(body.get("assistant_message_id")),
        _tokens(body.get("usage")),
        sequence,
    )


def usage_tokens(usage: object) -> tuple[int, int] | None:
    """Read a run's `usage`: the input and output tokens every model it used reports, summed.

    The same `tokens_consumed` list ends a streamed run and a run answered whole (measured on
    a live account), one entry per model, each with `input_tokens` and `output_tokens` totals.

    Returns:
        `(input, output)`; None when `usage` holds no `tokens_consumed` list.
    """
    consumed = usage.get("tokens_consumed") if isinstance(usage, dict) else None
    if not isinstance(consumed, list):
        return None
    sides = [0, 0]
    for item in consumed:
        if not isinstance(item, dict):
            continue
        for position, side in enumerate(("input_tokens", "output_tokens")):
            counted = item.get(side)
            sides[position] += _number(counted.get("total")) or 0 if isinstance(counted, dict) else 0
    return sides[0], sides[1]


def _tokens(usage: object) -> int:
    """Sum the input and output tokens each model the run used reports."""
    return sum(usage_tokens(usage) or ())


def _text(value: object) -> str:
    return value if isinstance(value, str) else "" if value is None else str(value)


def _number(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None
