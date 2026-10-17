"""Score `tool_selection_accuracy` exactly, from the tools a reply called and its ground truth.

The rule, which the Snowflake evaluation service is to be calibrated against:

1. Only a call of a tool the agent's specification declares counts. Snowflake reports calls
   of its own system tools too, such as `server_skill` and `system_execute_sql`, which the
   specification never declares (measured on a live account); they are ignored.
2. A `data_to_chart` tool is ignored on both sides, called or expected: the service scored an
   agent that drew an extra chart as fully accurate (measured on a live account).
3. What remains is compared with the expected tools as multisets of names, matched without
   case: the evaluation service reports both lowercased. The score is the matched calls over
   matched, missing and extra calls together, so the same tools score 1.0, none in common
   0.0, and a tool called twice where it was expected once 0.5, as the service scored it
   (measured on a live account). The order of the calls does not matter.
4. An empty `ground_truth_invocations` expects no tool, so a reply that called none scores
   1.0. A question with no `ground_truth_invocations` states no expectation, and is unscored.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping

CHART_TOOL_TYPES = frozenset(("data_to_chart",))


def declared_tools(spec: object) -> dict[str, str]:
    """Map each tool a rendered agent specification declares, by name, to its type.

    The specification is the parsed JSON `sst compile --emit-agent-spec` writes: `tools` is a
    list of entries, each with a `tool_spec` holding `name` and `type`. An entry without a
    name is left out; one without a type maps to "".
    """
    tools = spec.get("tools") if isinstance(spec, Mapping) else None
    declared: dict[str, str] = {}
    for entry in tools if isinstance(tools, list) else ():
        tool_spec = entry.get("tool_spec") if isinstance(entry, Mapping) else None
        if not isinstance(tool_spec, Mapping):
            continue
        name = tool_spec.get("name")
        if isinstance(name, str) and name:
            declared[name] = str(tool_spec.get("type") or "")
    return declared


def expected_tools(ground_truth: object) -> tuple[str, ...] | None:
    """Return the tool names a question's `ground_truth_invocations` expects, in order.

    Returns:
        The names; `()` for an empty list, which expects no tool; None when the ground truth
        has no `ground_truth_invocations` list, which states no expectation. An invocation
        without a `tool_name` names nothing.
    """
    invocations = ground_truth.get("ground_truth_invocations") if isinstance(ground_truth, Mapping) else None
    if not isinstance(invocations, list):
        return None
    return tuple(
        name
        for invocation in invocations
        if isinstance(invocation, Mapping) and isinstance(name := invocation.get("tool_name"), str)
    )


def selected_tools(called: Iterable[str], declared: Mapping[str, str]) -> Counter[str]:
    """Count the calls of declared, non-chart tools among `called`, by name without case."""
    types = {name.casefold(): kind for name, kind in declared.items()}
    return Counter(
        name.casefold()
        for name in called
        if name.casefold() in types and types[name.casefold()] not in CHART_TOOL_TYPES
    )


def tool_selection_score(
    called: Iterable[str], declared: Mapping[str, str], expected: Iterable[str] | None
) -> float | None:
    """Score one reply's tool calls against the expected tools, by the module's rule.

    Args:
        called: The name of every tool call the reply reports, system tools included.
        declared: The specification's tools, as `declared_tools` maps them.
        expected: The names `expected_tools` returns; None states no expectation.

    Returns:
        Matched calls over matched, missing and extra calls, by the module's rule; 1.0 when
        none is expected and none that counts was made; None when nothing is expected.
    """
    if expected is None:
        return None
    types = {name.casefold(): kind for name, kind in declared.items()}
    wanted = Counter(name.casefold() for name in expected if types.get(name.casefold()) not in CHART_TOOL_TYPES)
    made = selected_tools(called, declared)
    matched = sum((made & wanted).values())
    compared = matched + sum((wanted - made).values()) + sum((made - wanted).values())
    return matched / compared if compared else 1.0
