"""How a member diff reads in the terminal and in an agent document: its caps, `--full`, and long lines."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.cli.render.agent import agent_document
from snowflake_semantic_tools.cli.render.report import (
    DIFF_CHANGE_LINES,
    DIFF_LINE_CHARS,
    DIFF_PROPERTY_LINES,
    member_diff_lines,
)
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import Terminal
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.plan.member_diff import ADDED, CHANGED, REMOVED, MemberDiff, PropertyDiff

_STYLE = Style(Terminal(width=100), stdout=True)


def _synonyms(count: int) -> MemberDiff:
    return MemberDiff("metric", "m", CHANGED, (PropertyDiff("synonyms", (), tuple(f"s{n}" for n in range(count))),))


def test_a_long_property_and_a_long_diff_are_cut_short_and_counted() -> None:
    lines = member_diff_lines((_synonyms(DIFF_PROPERTY_LINES + 3),), _STYLE, full=False, more="the file")
    assert len(lines) == 2 + DIFF_PROPERTY_LINES + 1
    assert lines[-1] == "      … 3 more lines (the file)"
    many = tuple(MemberDiff("metric", f"m{n}", CHANGED, (PropertyDiff("comment", "a", "b"),)) for n in range(15))
    lines = member_diff_lines(many, _STYLE, full=False, more="the file")
    assert len(lines) == DIFF_CHANGE_LINES + 1
    assert lines[-1] == f"      … {15 * 4 - DIFF_CHANGE_LINES} more lines (the file)"
    single = member_diff_lines((_synonyms(DIFF_PROPERTY_LINES + 1),), _STYLE, full=False, more="x")
    assert single[-1] == "      … 1 more line (x)"
    assert len(member_diff_lines(many, _STYLE, full=True, more="x")) == 15 * 4


def test_an_added_or_removed_member_lists_its_properties_only_in_full() -> None:
    diffs = (
        MemberDiff("dimension", "d", ADDED, (PropertyDiff("expression", None, "T.D"),)),
        MemberDiff("filter", "f", REMOVED, (PropertyDiff("expression", "T.F = 1", None),)),
    )
    assert member_diff_lines(diffs, _STYLE, full=False, more="x") == ["      + dimension d", "      - filter f"]
    assert member_diff_lines(diffs, _STYLE, full=True, more="x") == [
        "      + dimension d",
        "          expression:",
        "            + T.D",
        "      - filter f",
        "          expression:",
        "            - T.F = 1",
    ]


def test_a_long_line_is_clipped_unless_full() -> None:
    long = MemberDiff("tool", "t", CHANGED, (PropertyDiff("body", "a\n" + "x" * 300, "a\n" + "y" * 300),))
    clipped = member_diff_lines((long,), _STYLE, full=False, more="x")
    assert max(len(line) for line in clipped) == 12 + 2 + DIFF_LINE_CHARS
    assert clipped[3].endswith("...")
    assert member_diff_lines((long,), _STYLE, full=True, more="x")[3].endswith("x" * 10)


def test_an_agent_document_names_at_most_six_members_and_counts_the_rest() -> None:
    members = [{"kind": "metric", "member": f"orders.m {n}", "op": "changed"} for n in range(8)]
    change = {
        "action": "update",
        "artifact_key": "semantic_view:sales",
        "target": "DB.S.SALES",
        "reason": "fingerprint_differs",
        "properties": [{"property": "fingerprint"}],
        "diff_status": "compared",
        "diff": [*members, {"kind": "dimension", "member": "d", "op": "added"}],
    }
    out = agent_document(
        "plan",
        2,
        DiagnosticBag(),
        baselined=frozenset(),
        fingerprint=stable_fingerprint,
        project_dir=Path("."),
        data={"changes": [change, {**change, "diff": [], "artifact_key": "semantic_view:menu"}]},
    )
    [sales, menu] = [line for line in out.splitlines() if line.startswith("~ ")]
    assert sales.endswith("changes=" + ",".join(f"metric:orders.m_{n}~" for n in range(6)) + ",3_more")
    # A compared update whose members all agree still says its definition changed.
    assert menu.endswith("changes=definition")
