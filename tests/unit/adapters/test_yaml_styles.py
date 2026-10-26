"""String styles in the canonical form: long lines fold exactly, soft wraps join, code is left alone."""

from __future__ import annotations

from typing import Any, cast

from hypothesis import given, settings
from hypothesis import strategies as st
from ruamel.yaml import YAML

from snowflake_semantic_tools.adapters.yaml.format import canonical_yaml, keep_canonical
from snowflake_semantic_tools.adapters.yaml.styles import YamlStyle, fold_positions, foldable, unwrap_prose

STYLE: dict[str, Any] = {"wrap_width": 60, "unwrap_prose": True}
_WORDS = st.lists(st.text(alphabet="abcdefghij'.,:-()", min_size=1, max_size=14), min_size=1, max_size=30)


def _value(text: str) -> object:
    return YAML(typ="safe", pure=True).load(text)


@settings(max_examples=200)
@given(_WORDS, st.sampled_from(("description", "label", "sql")), st.booleans())
def test_a_long_line_folds_to_the_same_value_and_is_a_fixed_point(words: list[str], key: str, dash: bool) -> None:
    line = " ".join(words)
    text = f"models:\n- name: m\n  {key}: '{line.replace(chr(39), chr(39) * 2)}'\n"
    if not dash:
        text = text.replace("\n- name: m\n  ", "\n  - name: m\n    ")
    once = canonical_yaml(text, "f.yml", **STYLE)
    assert _value(once) == _value(text)
    assert canonical_yaml(once, "f.yml", **STYLE) == once
    if key == "sql":
        assert ">-" not in once
        return
    for row in once.splitlines():
        assert len(row) <= 60 or " " not in row.strip()


def test_fold_positions_fill_lines_greedily_and_never_split_a_word() -> None:
    text = "aaaa bbbb cccc dddd"
    assert [text[:positions] for positions in fold_positions(text, 11, 2)] == ["aaaa bbbb"]
    assert fold_positions("x" * 30, 10, 0) == []


def test_only_text_folding_gives_back_exactly_is_foldable() -> None:
    assert foldable("one two three")
    for text in ("two  spaces", " lead", "tail ", "a\tb", "{{ ref('x') }} y", "a\rb"):
        assert not foldable(text)


def test_soft_wraps_join_and_lists_headings_and_indented_lines_keep_their_breaks() -> None:
    assert unwrap_prose("One row per\nmember per day.\n\n\nNOTE: exposed\nas a source.") == (
        ["One row per member per day.", "NOTE: exposed as a source."],
        False,
    )
    for hard in ("Columns:\n- a\n- b", "Intro\n# Heading", "Steps:\n1. one", "a\n  indented", "a\n| t |"):
        assert unwrap_prose(hard)[1]


def test_a_soft_wrapped_description_publishes_as_one_line_per_paragraph() -> None:
    text = (
        "models:\n- name: m\n  description: |-\n    One row per\n    member per day.\n\n"
        "    NOTE: exposed\n    as a source.\n"
    )
    once = canonical_yaml(text, "f.yml", **STYLE)
    assert once == (
        "models:\n- name: m\n  description: >-\n    One row per member per day.\n\n    NOTE: exposed as a source.\n"
    )
    assert (
        cast(dict[str, Any], _value(once))["models"][0]["description"]
        == "One row per member per day.\nNOTE: exposed as a source."
    )
    assert canonical_yaml(once, "f.yml", **STYLE) == once


def test_structural_values_and_lists_keep_their_line_breaks() -> None:
    text = (
        "metrics:\n  - name: m\n    expr: |-\n      SUM(x)\n      -- net of refunds\n"
        "    description: |-\n      Revenue:\n      - gross\n      - net\n"
    )
    assert canonical_yaml(text, "f.yml", **STYLE) == text


def test_without_the_settings_nothing_is_folded_or_joined() -> None:
    text = "a:\n  description: |-\n    one\n    two\n  label: " + "word " * 30 + "end\n"
    assert canonical_yaml(text, "f.yml") == text


def test_an_edited_file_stays_canonical_only_when_it_was() -> None:
    style = YamlStyle(spaced=True, wrap_width=60, unwrap_prose=True)
    canonical = canonical_yaml("models:\n- name: a\n- name: b\n", "f.yml", spaced=True)
    edited = canonical + "- name: c\n  description: " + "word " * 20 + "end\n"
    kept = keep_canonical(canonical, edited, "f.yml", style)
    assert "description: >-" in kept
    assert "\n\n- name: c" in kept
    messy = "models:\n- name: a\n- name: b\n"
    assert keep_canonical(messy, messy + "- name: c\n", "f.yml", style) == messy + "- name: c\n"
    assert keep_canonical(messy, messy, "f.yml", YamlStyle()) == messy


def test_nothing_in_a_flow_collection_is_folded() -> None:
    items = ", ".join(f"'Item number {index}'" for index in range(12))
    text = f"columns:\n- name: c\n  data_tests:\n  - accepted_values:\n      values: [{items}]\n"
    once = canonical_yaml(text, "f.yml", **STYLE)
    assert once == text
