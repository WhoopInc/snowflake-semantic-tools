"""The member diff: what two definitions of one artifact disagree on, and how a person reads it."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.plan.member_diff import (
    ADDED,
    CHANGED,
    REMOVED,
    Definition,
    Member,
    MemberDiff,
    PropertyDiff,
    diff_definitions,
    normalize,
    printable,
    property_lines,
    summary,
    unified_text,
)

_TEXT = st.text(max_size=12)
_VALUES = st.one_of(_TEXT, st.tuples(_TEXT, _TEXT))
_MEMBERS = st.lists(
    st.builds(
        Member,
        st.sampled_from(("metric", "dimension", "file")),
        st.text(alphabet="abcdef", min_size=1, max_size=3),
        st.lists(st.tuples(st.sampled_from(("comment", "expression", "synonyms")), _VALUES), max_size=3).map(
            lambda pairs: tuple(dict(pairs).items())
        ),
    ),
    max_size=6,
    unique_by=lambda member: member.key,
).map(lambda members: Definition(tuple(members)))


def _view(*members: Member) -> Definition:
    return Definition(members)


def test_a_definition_compared_with_itself_differs_nowhere() -> None:
    view = _view(Member("metric", "orders.total_revenue", (("comment", "Revenue."), ("synonyms", ("sales",)))))
    assert diff_definitions(view, view) == ()


@given(_MEMBERS)
def test_any_definition_compared_with_itself_differs_nowhere(definition: Definition) -> None:
    assert diff_definitions(definition, definition) == ()


@given(_MEMBERS, _MEMBERS)
def test_swapping_the_sides_swaps_added_and_removed_and_before_and_after(left: Definition, right: Definition) -> None:
    forward = {(item.kind, item.name.casefold()): item for item in diff_definitions(left, right)}
    backward = {(item.kind, item.name.casefold()): item for item in diff_definitions(right, left)}
    assert forward.keys() == backward.keys()
    swapped = {ADDED: REMOVED, REMOVED: ADDED, CHANGED: CHANGED}
    for key, item in forward.items():
        other = backward[key]
        assert other.op == swapped[item.op]
        if item.op == CHANGED:
            assert {(p.name, p.after, p.before) for p in other.properties} == {
                (p.name, p.before, p.after) for p in item.properties
            }


def test_a_changed_member_names_only_the_properties_that_differ() -> None:
    before = _view(Member("metric", "orders.total_revenue", (("comment", "Revenue."), ("expression", "SUM(X)"))))
    after = _view(
        Member("metric", "ORDERS.TOTAL_REVENUE", (("comment", "Revenue, in dollars."), ("expression", "SUM(X)")))
    )
    assert diff_definitions(before, after) == (
        MemberDiff(
            "metric", "ORDERS.TOTAL_REVENUE", CHANGED, (PropertyDiff("comment", "Revenue.", "Revenue, in dollars."),)
        ),
    )


def test_members_are_listed_in_the_after_order_then_the_removed_ones() -> None:
    before = _view(Member("dimension", "a"), Member("filter", "gone", (("expression", "X = 1"),)))
    after = _view(Member("dimension", "new", (("comment", "New."), ("synonyms", ()))), Member("dimension", "a"))
    diffs = diff_definitions(before, after)
    assert [(item.op, item.label) for item in diffs] == [(ADDED, "dimension new"), (REMOVED, "filter gone")]
    # An added or removed member carries what it states, less its empty properties.
    assert diffs[0].properties == (PropertyDiff("comment", None, "New."),)
    assert diffs[1].properties == (PropertyDiff("expression", "X = 1", None),)
    assert summary(diffs) == ("+dimension new", "-filter gone")


def test_a_property_only_one_side_states_is_never_compared() -> None:
    # The live side could not read `window`, so it says nothing about it.
    before = _view(Member("metric", "m", (("expression", "SUM(X)"),)))
    after = _view(Member("metric", "m", (("expression", "SUM(X)"), ("window", "OVER (ORDER BY D)"))))
    assert diff_definitions(before, after) == ()


def test_a_definition_refuses_two_members_of_one_name() -> None:
    with pytest.raises(ValueError, match="names metric 'a' twice"):
        _view(Member("metric", "a"), Member("metric", "A"))


def test_a_scalar_shows_its_old_and_new_value() -> None:
    assert property_lines(PropertyDiff("comment", "Old.", "")) == (("-", "Old."), ("+", "(none)"))
    assert property_lines(PropertyDiff("comment", None, "New.")) == (("+", "New."),)
    assert property_lines(PropertyDiff("comment", "Gone.", None)) == (("-", "Gone."),)
    assert property_lines(PropertyDiff("synonyms", None, ("new",))) == (("+", "new"),)


def test_sample_values_are_counted_not_listed() -> None:
    lines = property_lines(PropertyDiff("sample_values", ("a", "b"), ("a", "b", "c")))
    assert lines == (("~", "2 -> 3 values"),)


def test_a_list_shows_the_items_it_loses_and_gains_or_that_only_its_order_changed() -> None:
    assert property_lines(PropertyDiff("synonyms", ("aov", "order size"), ("aov", "basket"))) == (
        ("-", "order size"),
        ("+", "basket"),
    )
    assert property_lines(PropertyDiff("synonyms", ("a", "b"), ("b", "a"))) == (("~", "reordered"),)


def test_text_is_a_line_diff_with_one_line_of_context() -> None:
    before = "SELECT\n    a\n  , b\nFROM t\nWHERE x\nGROUP BY 1\nORDER BY 1\nLIMIT 10"
    after = "SELECT\n    a\n  , c\nFROM t\nWHERE x\nGROUP BY 1\nORDER BY 1\nLIMIT 20"
    assert property_lines(PropertyDiff("sql", before, after)) == (
        (" ", "    a"),
        ("-", "  , b"),
        ("+", "  , c"),
        (" ", "FROM t"),
        (" ", "..."),
        (" ", "ORDER BY 1"),
        ("-", "LIMIT 10"),
        ("+", "LIMIT 20"),
    )


def test_control_characters_never_reach_a_reader() -> None:
    assert printable("ok\x1b[31mred\x07\x9b\ttab\nline") == "ok[31mred\ttab\nline"
    assert property_lines(PropertyDiff("comment", "\x1b]8;;x\x1b\\", "fine")) == (("-", "]8;;x\\"), ("+", "fine"))


def test_the_unified_text_has_a_section_per_artifact_and_a_hunk_per_member() -> None:
    diffs = (
        MemberDiff("metric", "orders.total_revenue", CHANGED, (PropertyDiff("comment", "Revenue.", "Revenue, $."),)),
        MemberDiff("dimension", "customers.signup_channel", ADDED, (PropertyDiff("expression", None, "C.S"),)),
    )
    text = unified_text((("semantic_view:sales", "DB.SV.SALES", diffs), ("semantic_view:menu", "DB.SV.MENU", ())))
    assert text == (
        "--- live/semantic_view:sales  DB.SV.SALES\n"
        "+++ desired/semantic_view:sales\n"
        "@@ changed metric orders.total_revenue @@\n"
        "   comment:\n"
        "-  Revenue.\n"
        "+  Revenue, $.\n"
        "@@ added dimension customers.signup_channel @@\n"
        "   expression:\n"
        "+  C.S\n"
    )
    assert unified_text(()) == ""
    assert unified_text((("agent:a", "", diffs[:1]),), before_side="dev", after_side="prod").startswith(
        "--- dev/agent:a\n+++ prod/agent:a\n"
    )


def test_normalizing_replaces_each_mapped_name_whole_or_within_text() -> None:
    names = {("DEV_DB", "MARTS", "ORDERS"): "ref('orders')", ("DEV_DB", "SV", "SALES"): "semantic_view:sales"}
    definition = _view(
        Member("table", "orders", (("base_table", "DEV_DB.MARTS.ORDERS"), ("comment", "From dev_db.marts.orders."))),
        Member(
            "tool", "analyst", (("semantic_view", "dev_db.sv.sales"), ("query", "SELECT * FROM DEV_DB.MARTS.ORDERS_X"))
        ),
        Member("tool", "other", (("names", ("DEV_DB.SV.SALES", "PROD_DB.SV.SALES")),)),
    )
    assert normalize(definition, names) == _view(
        Member("table", "orders", (("base_table", "ref('orders')"), ("comment", "From ref('orders')."))),
        # A longer name that only starts like a mapped one is kept.
        Member(
            "tool",
            "analyst",
            (("semantic_view", "semantic_view:sales"), ("query", "SELECT * FROM DEV_DB.MARTS.ORDERS_X")),
        ),
        # A name another target uses is not this target's, so it stays physical.
        Member("tool", "other", (("names", ("semantic_view:sales", "PROD_DB.SV.SALES")),)),
    )
    assert normalize(definition, {}) is definition


def test_two_targets_that_differ_only_in_names_compare_equal_once_normalized() -> None:
    dev = _view(Member("table", "orders", (("base_table", "DEV_DB.MARTS.ORDERS"),)))
    prod = _view(Member("table", "orders", (("base_table", "SALES_DB.MARTS.ORDERS"),)))
    assert diff_definitions(dev, prod)
    left = normalize(dev, {("DEV_DB", "MARTS", "ORDERS"): "ref('orders')"})
    right = normalize(prod, {("SALES_DB", "MARTS", "ORDERS"): "ref('orders')"})
    assert diff_definitions(left, right) == ()


def test_a_long_one_line_value_is_shown_from_shortly_before_its_first_difference() -> None:
    before = "Searches menu items: " + "x" * 150 + " and which items suit a taste."
    after = "Searches menu items: " + "x" * 150 + " and Edited. which items suit a taste."
    [(old_sign, old), (new_sign, new)] = property_lines(PropertyDiff("comment", before, after))
    assert (old_sign, new_sign) == ("-", "+")
    assert old.startswith("...") and new.startswith("...")
    assert "and which" in old and "and Edited." in new
    # Differing from the start, nothing is cut.
    assert property_lines(PropertyDiff("comment", "a" * 130, "b" * 130)) == (("-", "a" * 130), ("+", "b" * 130))


def test_a_blank_context_line_carries_no_trailing_space_and_a_changed_one_keeps_its_own() -> None:
    lines = property_lines(PropertyDiff("body", "a\n  \nb\nc \n", "a\n  \nB\nc\n"))
    assert lines == ((" ", ""), ("-", "b"), ("-", "c "), ("+", "B"), ("+", "c"))
    # In the file, a blank context line is a lone space, as a unified diff writes one.
    diff = MemberDiff("tool", "t", CHANGED, (PropertyDiff("body", "a\n\nb", "a\n\nB"),))
    assert unified_text((("tool:t", "", (diff,)),)) == (
        "--- live/tool:t\n+++ desired/tool:t\n@@ changed tool t @@\n   body:\n \n-  b\n+  B\n"
    )


def test_a_kind_one_side_did_not_read_is_not_compared() -> None:
    config_only = Definition((Member("config", "yaml", (("text", "a"),)),), frozenset(("config",)))
    everything = Definition(
        (Member("config", "yaml", (("text", "b"),)), Member("question", "Q?", (("ground_truth", "1"),)))
    )
    assert [item.label for item in diff_definitions(config_only, everything)] == ["config yaml"]
    assert [item.label for item in diff_definitions(everything, config_only)] == ["config yaml"]
    assert normalize(config_only, {("A", "B", "C"): "x"}).kinds == frozenset(("config",))
