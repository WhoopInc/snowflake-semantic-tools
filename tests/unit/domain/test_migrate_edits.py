"""The migrations' text edits, outlines, template-call text and pass merging."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.migrate.calls import bare, call, one_arg_tool, quoted_call, relative_to, tool_arity
from snowflake_semantic_tools.domain.migrate.changes import Changes, merged
from snowflake_semantic_tools.domain.migrate.edits import (
    Edit,
    apply_edits,
    comments_above,
    dash_line,
    double_quoted,
    entry_span,
    lines_edit,
    node_edit,
    shift,
    value_end,
)
from snowflake_semantic_tools.domain.migrate.outline import SCALAR, Node
from snowflake_semantic_tools.domain.migrate.refs import MigrationResult, Rewrite
from tests.helpers.migrate_outline import lines, outline

DOC = """a:
  b:
    c: 1
    d: |-
      text

  # belongs to e
  e: [x, y]
  f:
    - 1
    -
      g: 2
h: ~
i: "{{ ref('m') }}"
"""


def test_outline_locates_every_node_and_restores_templates() -> None:
    root = outline(DOC)
    assert root.is_map and not root.is_seq
    assert root.find("a", "b", "c") is not None and root.find("a", "b", "c").text == "1"  # type: ignore[union-attr]
    assert root.find("a", "missing", "c") is None
    assert root.get("h") is not None and root.get("h").null  # type: ignore[union-attr]
    assert root.get("i").text == "{{ ref('m') }}"  # type: ignore[union-attr]
    flow = root.find("a", "e")
    assert flow is not None and flow.flow and flow.is_seq and [item.text for item in flow.items] == ["x", "y"]
    assert root.find("a", "f").maps()[0].get("g").text == "2"  # type: ignore[union-attr]
    assert root.maps() == () and root.text is None and root.entry("zz") is None


def test_value_end_drops_trailing_blanks_and_shallow_comments() -> None:
    text = lines(DOC)
    b = outline(DOC).find("a").entry("b")  # type: ignore[union-attr]
    assert b is not None and entry_span(text, b) == (1, 5)
    d = outline(DOC).find("a", "b").entry("d")  # type: ignore[union-attr]
    assert d is not None and value_end(text, d.value, d.col) == 5
    e = outline(DOC).find("a").entry("e")  # type: ignore[union-attr]
    assert e is not None and comments_above(text, e.line, e.col) == 6
    f = outline(DOC).find("a", "f")
    assert f is not None and dash_line(text, f.items[1], f.col) == 10
    scalar = Node(SCALAR, 0, 0, 1, 0, value="x")
    assert value_end(["x\n", "y\n"], scalar, 0) == 1


def test_apply_edits_applies_from_the_end_and_refuses_overlaps() -> None:
    text = "one\ntwo\nthree"
    edits = [lines_edit(1, 2, "TWO\n"), Edit(0, 0, 0, 0, ">"), Edit(9, 0, 9, 0, "!"), Edit(2, 0, 2, 5, "3")]
    assert apply_edits(text, edits) == ">one\nTWO\n3!"
    with pytest.raises(ValueError, match="overlapping"):
        apply_edits(text, [Edit(0, 0, 1, 1, ""), Edit(0, 2, 0, 3, "")])
    node = Node(SCALAR, 0, 1, 0, 3, value="ne")
    assert apply_edits(text, [node_edit(node, "X")]) == "oX\ntwo\nthree"


def test_shift_and_quoting() -> None:
    assert shift("  a\n\n    b\n", 2) == "    a\n\n      b\n"
    assert shift("  a\n b\n", -2) == "a\nb\n"
    assert double_quoted('a "b" \\c') == '"a \\"b\\" \\\\c"'


def test_call_text() -> None:
    assert call("ref", "m", "c") == "{{ ref('m', 'c') }}"
    assert quoted_call("tag", "t") == "\"{{ tag('t') }}\""
    assert bare(None) is None and bare("  ") is None and bare("{{ x() }}") is None and bare(" x ") == "x"
    assert relative_to("tools/sql/a.sql", "tools/sales") == "../sql/a.sql"
    assert relative_to("a.sql", "") == "a.sql" and relative_to("tools", "tools") == "."


def test_one_arg_tool_drops_only_a_group_argument() -> None:
    text = "x: \"{{ tool('g', 'n') }}/p\" {{ tool(\"n\") }} {{ ref('a', 'b') }} {{ broken( }} {{ tool('g','m') }}"
    rewritten, count = one_arg_tool(text)
    assert count == 2
    assert rewritten == "x: \"{{ tool('n') }}/p\" {{ tool(\"n\") }} {{ ref('a', 'b') }} {{ broken( }} {{ tool('m') }}"
    assert one_arg_tool("plain") == ("plain", 0)
    result = tool_arity("a: 1\nb: \"{{ tool('g', 'n') }}\"\n")
    assert result.text == "a: 1\nb: \"{{ tool('n') }}\"\n" and [r.line for r in result.rewrites] == [2]
    assert tool_arity("a: 1\n") == MigrationResult("a: 1\n")


def test_merged_keeps_earlier_reports_and_applies_edits() -> None:
    previous = MigrationResult("a\nb\n", (Rewrite(2, 1, "x", "y", "ref"),))
    changes = Changes()
    changes.add(lines_edit(0, 1, "A\n"), "a", "A", "reference")
    changes.leave(1, 0, "b", "why")
    result = merged(previous, changes)
    assert result.text == "A\nb\n" and [r.line for r in result.rewrites] == [1, 2] and result.untouched[0].line == 2
    assert merged(previous, Changes()) is previous
    replaced = Changes()
    replaced.replace(Node(SCALAR, 0, 0, 0, 1, value="a"), "Z", "reference")
    assert merged(MigrationResult("a\n"), replaced).text == "Z\n"


def test_value_end_keeps_the_last_line_of_a_text_with_no_trailing_newline() -> None:
    doc = "a:\n  - name: b\n    syn:\n      - x\n      - last"
    item = outline(doc).find("a").items[0]  # type: ignore[union-attr]
    syn = item.entry("syn")
    assert syn is not None and value_end(lines(doc), syn.value, syn.col) == 5
    assert value_end(lines(doc), item, 2) == 5
    # A block that a sibling key ends still stops on the line before that key.
    sibling = "a:\n  - name: b\n    syn:\n      - x\n    k: v"
    syn = outline(sibling).find("a").items[0].entry("syn")  # type: ignore[union-attr]
    assert syn is not None and value_end(lines(sibling), syn.value, syn.col) == 4
