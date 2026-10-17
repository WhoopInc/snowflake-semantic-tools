"""Fill an LLM judge's prompt from one agent reply, and read the judge's score back.

An unpublished eval run judges `answer_correctness` and each custom metric with a completion
at temperature 0 that answers JSON, `{"score": <number>, "reason": <text>}`. A custom
metric's prompt is its author's, with its placeholders filled from the reply; the prompt for
`answer_correctness` is SST's own, written here, so its scores only approximate the evaluation
service's. Every judged score is approximate.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

_JUDGE_PLACEHOLDER = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}")
# What one reply fills a judge's placeholder with. The evaluation service fills the others --
# `start_timestamp`, the `span_*` ones and `llm_model` -- from the run's trace, which an
# unpublished run does not have.
REPLY_PLACEHOLDERS = frozenset(("input", "output", "ground_truth", "tool_info", "duration", "error", "status"))
# The response a judge is asked for, as AI_COMPLETE's structured output takes it.
JUDGE_SCHEMA: Mapping[str, object] = MappingProxyType(
    {
        "type": "object",
        "properties": {"score": {"type": "number"}, "reason": {"type": "string"}},
        "required": ["score", "reason"],
        "additionalProperties": False,
    }
)
# The scale SST's own `answer_correctness` judge scores on, as the service's thresholds read it.
ANSWER_CORRECTNESS_SCALE = (0.0, 1.0)


@dataclass(frozen=True, slots=True)
class JudgedReply:
    """One question and what the agent did with it, as a judge's placeholders read it.

    Attributes:
        ground_truth: The question's ground truth, as the compiled dataset holds it.
        tool_info: The reply's tool calls and results, as JSON text.
        error: Snowflake's message when the run answered with an error; None when it replied.
    """

    input: str
    output: str
    ground_truth: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tool_info: str = "[]"
    duration_ms: int = 0
    error: str | None = None

    def values(self) -> dict[str, str]:
        """Return each placeholder of `REPLY_PLACEHOLDERS` as the text it is filled with."""
        return {
            "input": self.input,
            "output": self.output,
            "ground_truth": json.dumps(dict(self.ground_truth), sort_keys=True, ensure_ascii=False),
            "tool_info": self.tool_info,
            "duration": str(self.duration_ms),
            "error": self.error or "",
            "status": "error" if self.error is not None else "success",
        }


def unfillable_placeholders(prompt: str) -> tuple[str, ...]:
    """Return, sorted and each once, the placeholders in `prompt` a reply cannot fill."""
    return tuple(sorted({name for name in _JUDGE_PLACEHOLDER.findall(prompt) if name not in REPLY_PLACEHOLDERS}))


def fill_judge_prompt(prompt: str, reply: JudgedReply) -> str:
    """Replace each `{{ placeholder }}` a reply fills with its value; any other is left as written."""
    values = reply.values()
    return _JUDGE_PLACEHOLDER.sub(lambda match: values.get(match.group(1), match.group(0)), prompt)


def custom_judge_prompt(prompt: str, reply: JudgedReply, low: float, high: float) -> str:
    """Return a custom metric's filled prompt, followed by how to answer on the metric's own scale."""
    return f"{fill_judge_prompt(prompt, reply)}\n\n{_answer_format(low, high)}"


def answer_correctness_prompt(reply: JudgedReply) -> str | None:
    """Return SST's prompt judging whether the answer meets the question's `ground_truth_output`.

    Returns:
        The prompt; None when the question has no `ground_truth_output`, which leaves nothing
        to judge the answer against.
    """
    reference = reply.ground_truth.get("ground_truth_output")
    if not isinstance(reference, str) or not reference.strip():
        return None
    low, high = ANSWER_CORRECTNESS_SCALE
    return (
        "You are grading an AI agent's answer to a question against a reference that says what a "
        "correct answer must contain.\n\n"
        f"Question:\n{reply.input}\n\n"
        f"Reference:\n{reference}\n\n"
        f"Answer:\n{reply.output}\n\n"
        f"Score {high:g} when the answer meets every requirement of the reference, {low:g} when it "
        "contradicts the reference or misses its point, and a value between for an answer that "
        "meets some requirements. Judge only what the reference requires, not style or length. "
        "If evidence is insufficient or two scores are tied, choose the lower score.\n\n"
        f"{_answer_format(low, high)}"
    )


def judged_score(answer: object, low: float, high: float) -> float | None:
    """Read the score a judge answered, when it is a finite number within `low` to `high`.

    Args:
        answer: The completion, decoded or as JSON text.

    Returns:
        The score; None for an answer that is not an object with such a `score`.
    """
    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except json.JSONDecodeError:
            return None
    score = answer.get("score") if isinstance(answer, Mapping) else None
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        return None
    value = float(score)
    return value if math.isfinite(value) and low <= value <= high else None


def _answer_format(low: float, high: float) -> str:
    return (
        f'Reply with a JSON object: "score", a number from {low:g} to {high:g}, and "reason", one sentence saying why.'
    )
