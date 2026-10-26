"""The canonical YAML form: idempotent, value-preserving, and the cure for every SST-VAL009 finding."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from ruamel.yaml import YAML

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.format import _trimmed_tree, canonical_yaml
from snowflake_semantic_tools.domain.validate.semantic.files import formatting_problem

_KEYS = st.sampled_from(("name", "description", "expr", "synonyms", "tables", "sample_values", "x"))
_TEXT = st.text(alphabet=st.characters(codec="utf-8", exclude_categories=("Cs", "Cc")), max_size=20)
_LINES = st.lists(_TEXT, min_size=1, max_size=4).map("\n".join)
_SCALARS = st.one_of(st.none(), st.booleans(), st.integers(-5, 5), _TEXT, _LINES)
_TREES = st.recursive(
    _SCALARS,
    lambda children: st.one_of(st.lists(children, max_size=3), st.dictionaries(_KEYS, children, max_size=4)),
    max_leaves=12,
)


def _value(text: str) -> object:
    return YAML(typ="safe", pure=True).load(text)


def _formatted_value(text: str) -> object:
    """The value format keeps: the loaded value, with the one whitespace repair it always makes."""
    return _trimmed_tree(_value(text))


def _dumped(tree: object, *, style: str | None) -> str:
    import io

    yaml = YAML(typ="safe", pure=True)
    yaml.default_style = style  # type: ignore[assignment]
    yaml.default_flow_style = False
    stream = io.StringIO()
    yaml.dump(tree, stream)
    return stream.getvalue()


@settings(max_examples=150)
@given(_TREES, st.sampled_from((None, '"', "'")), st.sampled_from(("", "  ", "\t")), st.booleans())
def test_format_is_idempotent_and_never_changes_a_value(tree: object, style: str | None, pad: str, crlf: bool) -> None:
    text = _dumped({"root": tree}, style=style)
    # Whitespace after each line, and Windows line endings, are what VAL009 most often finds.
    text = "\n".join(line + (pad if line.strip() else "") for line in text.split("\n"))
    if crlf:
        text = text.replace("\n", "\r\n")
    try:
        expected = _formatted_value(text)
    except Exception:  # whitespace that made the text unparseable is not this test's input
        return
    once = canonical_yaml(text, "f.yml")
    assert canonical_yaml(once, "f.yml") == once
    assert _value(once) == expected
    assert formatting_problem(once) is None


def test_multi_line_strings_become_literal_blocks_and_block_scalars_keep_their_style() -> None:
    text = 'a: "one\\ntwo"\nb: >\n  folded\n  text\n\n  para\nc: |\n  ends\n  in newline\n'
    assert canonical_yaml(text, "f.yml") == "a: |-\n  one\n  two\n" + text[text.index("b:") :]


_BLOCK_LINES = st.lists(
    st.text(alphabet=st.characters(codec="utf-8", exclude_categories=("Cs", "Cc", "Zs")), min_size=1, max_size=12),
    min_size=1,
    max_size=4,
)


@settings(max_examples=200)
@given(
    st.sampled_from(("|", ">")),
    st.sampled_from(("", "-", "+")),
    _BLOCK_LINES,
    st.lists(st.booleans(), min_size=4, max_size=4),
    st.booleans(),
)
def test_format_keeps_every_block_scalar_style_and_value(
    indicator: str, chomping: str, lines: list[str], indents: list[bool], followed: bool
) -> None:
    # Blank and more-indented lines are where a folded scalar's value differs from a literal's.
    body = [
        ("  " if indent and index else "") + line
        for index, (line, indent) in enumerate(zip(lines, indents[: len(lines)], strict=True))
    ]
    if len(body) > 2:
        body[1] = ""
    text = f"sources:\n  - name: s\n    freshness: {indicator}{chomping}\n"
    text += "".join(f"      {line}\n" if line else "\n" for line in body)
    text += "    loader: x\n" if followed else ""
    try:
        expected = _formatted_value(text)
    except Exception:  # a line the YAML scanner reads as structure is not this test's input
        return
    once = canonical_yaml(text, "f.yml")
    assert _value(once) == expected
    assert canonical_yaml(once, "f.yml") == once
    [header] = [line for line in once.split("\n") if "freshness:" in line]
    assert header.split(": ", 1)[1][0] == indicator


def test_layout_name_order_comments_quotes_and_nulls_survive() -> None:
    text = "# head\nitems:\n- expr: x\n  name: n\n- name: m  # kept\n  expr: y\nq: 'quoted'\nz: null\nw: ~\n"
    # The file writes its dashes at the parent's column, as dbt does; that layout is kept.
    assert canonical_yaml(text, "f.yml") == (
        "# head\nitems:\n- name: n\n  expr: x\n- name: m  # kept\n  expr: y\nq: 'quoted'\nz: null\nw: null\n"
    )


def test_a_commented_mapping_keeps_its_key_order() -> None:
    text = "items:\n  - expr: x\n    # about the name\n    name: n\n"
    assert canonical_yaml(text, "f.yml") == text


def test_a_single_item_file_is_laid_out_as_its_list_item_would_be() -> None:
    text = '---\n# One metric.\nmetric:\n    expr: x\n    description: "one\\ntwo"\n    name: m\n'
    formatted = canonical_yaml(text, "m.yml")
    assert formatted == "---\n# One metric.\nmetric:\n  name: m\n  expr: x\n  description: |-\n    one\n    two\n"
    assert canonical_yaml(formatted, "m.yml") == formatted
    listed = canonical_yaml('snowflake_metrics:\n  - expr: x\n    description: "one\\ntwo"\n    name: m\n', "l.yml")
    assert listed.replace("snowflake_metrics:\n  - ", "metric:\n  ").replace(
        "\n    ", "\n  "
    ) == formatted.removeprefix("---\n# One metric.\n")
    # Only a singular root moves `name`, and only when no comment sits between its keys.
    assert canonical_yaml("other:\n  expr: x\n  name: m\n", "o.yml") == "other:\n  expr: x\n  name: m\n"
    commented = "filter:\n  expr: x\n  # about the name\n  name: m\n"
    assert canonical_yaml(commented, "f.yml") == commented
    assert canonical_yaml("metric: 1\n", "f.yml") == "metric: 1\n"
    # A root list keeps its dashes at column 0, the only column a root list has.
    assert canonical_yaml("- metric\n", "f.yml") == "- metric\n"


def test_whitespace_a_block_scalar_holds_becomes_a_quoted_scalar() -> None:
    assert canonical_yaml("a: |\n  x  \n  y\n", "f.yml") == 'a: "x  \\ny\\n"\n'
    assert canonical_yaml("c: |-\n  \tindented\n  ok\n", "f.yml") == 'c: "\\tindented\\nok"\n'


def test_document_markers_and_empty_files() -> None:
    assert canonical_yaml("---\na: 1\n---\nb: 2\n", "f.yml") == "---\na: 1\n---\nb: 2\n"
    assert canonical_yaml("  \n\n", "f.yml") == ""
    assert canonical_yaml("a: 1", "f.yml") == "a: 1\n"


def test_sanitize_repairs_synonym_apostrophes_and_jinja_and_nothing_else() -> None:
    text = (
        "t:\n  synonyms: [\"bob's\", 'it''s', plain]\n  sample_values:\n    - \u2019q\u2019\n"
        "  description: 'see {{ x }} {% if %} {# c #}'\n  expr: \"it's\"\n"
    )
    repaired = canonical_yaml(text, "f.yml", sanitize=True)
    assert _value(repaired) == {
        "t": {
            "synonyms": ["bobs", "its", "plain"],
            "sample_values": ["\u2019q\u2019"],
            "description": "see { { x } } { % if % } { # c # }",
            "expr": "it's",
        }
    }
    assert canonical_yaml(text, "f.yml") == text.replace("\n  synonyms", "\n  synonyms")


def test_text_that_is_not_yaml_is_refused_with_its_position() -> None:
    with pytest.raises(ProjectError) as raised:
        canonical_yaml("a: [1,\n", "f.yml")
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.context["line"]) == ("SST-LOD001", 2)


def test_a_canonical_form_that_would_change_the_value_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.adapters.yaml import format as module

    monkeypatch.setattr(module, "_literal", lambda value: "changed" if value == "kept" else value)
    with pytest.raises(ProjectError) as raised:
        canonical_yaml("a: kept\n", "f.yml")
    assert raised.value.diagnostics[0].code == "SST-INT003"


def test_trailing_space_that_is_content_is_not_stripped() -> None:
    from snowflake_semantic_tools.adapters.yaml.format import _without_trailing_space

    assert _without_trailing_space("a: |\n  x \n") == "a: |\n  x \n"
    assert _without_trailing_space("a: 1  \n") == "a: 1\n"
    assert _without_trailing_space("a: [  \n") == "a: [  \n"


def test_a_canonical_form_a_second_pass_would_change_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.adapters.yaml import format as module

    passes = iter(("a: 1\n", "a:  1\n"))
    monkeypatch.setattr(module, "_canonical", lambda text, path, sanitize, spaced=False: next(passes))
    with pytest.raises(ProjectError) as raised:
        canonical_yaml("a: 1\n", "f.yml")
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.code == "SST-INT003"
    assert "a second format would change" in diagnostic.message


_DBT = """version: 2
models:
  - name: orders
    description: |-
      One row per order.
      Cancelled orders included.
    columns:
      - name: order_id
        data_tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: id
      # the customer who placed it
      - name: customer_id
  - name: customers
    columns:
      - name: id
"""


def test_spaced_separates_named_items_above_their_comments_and_nothing_else() -> None:
    assert canonical_yaml(_DBT, "f.yml", spaced=True) == (
        "version: 2\nmodels:\n  - name: orders\n    description: |-\n      One row per order.\n"
        "      Cancelled orders included.\n    columns:\n      - name: order_id\n        data_tests:\n"
        "          - not_null\n          - relationships:\n              to: ref('customers')\n"
        "              field: id\n\n      # the customer who placed it\n      - name: customer_id\n\n"
        "  - name: customers\n    columns:\n      - name: id\n"
    )


def test_spaced_separates_eval_questions_but_not_their_invocations() -> None:
    text = (
        "name: d\nquestions:\n  - question: a?\n    ground_truth:\n      ground_truth_invocations:\n"
        "        - tool_name: X\n        - tool_name: Y\n  - question: b?\n"
    )
    assert canonical_yaml(text, "f.yml", spaced=True) == text.replace("  - question: b?", "\n  - question: b?")


def test_spaced_is_a_fixed_point_and_unspaced_keeps_its_blank_lines() -> None:
    once = canonical_yaml(_DBT, "f.yml", spaced=True)
    assert canonical_yaml(once, "f.yml", spaced=True) == once
    assert canonical_yaml(once, "f.yml") == once
    assert canonical_yaml(_DBT, "f.yml") == _DBT
    assert _value(once) == _value(_DBT)


def test_spaced_never_adds_a_blank_line_a_keep_chomped_block_would_hold() -> None:
    text = "items:\n  - name: a\n    v: |+\n      kept\n\n\n  - name: b\n"
    once = canonical_yaml(text, "f.yml", spaced=True)
    assert _value(once) == _formatted_value(text)
    assert once.count("\n\n") == text.count("\n\n")


def test_spaced_items_returns_text_whose_value_a_blank_line_would_change() -> None:
    from snowflake_semantic_tools.adapters.yaml.format import spaced_items

    text = "items:\n  - name: a\n    v: |+\n      kept\n  - name: b\n"
    assert spaced_items(text) == text


@settings(max_examples=150)
@given(st.lists(st.dictionaries(_KEYS, _SCALARS, max_size=3), min_size=1, max_size=4))
def test_spaced_never_changes_a_value(items: list[dict[str, object]]) -> None:
    text = _dumped({"root": [{"name": "n", **item} for item in items]}, style=None)
    once = canonical_yaml(text, "f.yml", spaced=True)
    assert canonical_yaml(once, "f.yml", spaced=True) == once
    assert _value(once) == _formatted_value(text)
    assert formatting_problem(once) is None


def test_a_carriage_return_stays_escaped_in_a_double_quoted_string() -> None:
    text = 'samples:\n  - "<a/href=\\\\j\\n\\ravascript:x>"\n'
    once = canonical_yaml(text, "f.yml")
    assert _value(once) == _formatted_value(text)
    assert canonical_yaml(once, "f.yml") == once


def test_a_folded_description_ending_lines_in_spaces_becomes_a_literal_block_not_a_quoted_line() -> None:
    folded = (
        "models:\n"
        "  - name: m\n"
        "    description: >\n"
        "      For each turn (a, b),  \n"
        "      this table records it. \n"
        "\n"
        "      Sourced from events. \n"
    )
    formatted = canonical_yaml(folded, "m.yml")
    assert formatted == (
        "models:\n"
        "  - name: m\n"
        "    description: |-\n"
        "      For each turn (a, b),   this table records it.\n"
        "      Sourced from events.\n"
    )
    assert canonical_yaml(formatted, "m.yml") == formatted
    # A value that is not prose keeps its trailing whitespace, double-quoted, as before.
    sql = "models:\n  - name: m\n    meta:\n      sql: |\n        select 1 \n        from t\n"
    assert '"select 1 \\nfrom t\\n"' in canonical_yaml(sql, "m.yml")


def test_unquoted_templates_are_read_as_validate_reads_them_and_restored_as_written() -> None:
    text = (
        "metrics:\n"
        "  - name: avg_runs\n"
        "    expr: {{ metric('runs') }} / NULLIF({{ metric('tests') }}, 0)\n"
        "semantic_views:\n"
        "  - name: sales\n"
        "    tables:\n"
        "    - {{ ref('accessory_sales') }}\n"
        "filters:\n"
        "  - name: f\n"
        "    meta: {expr: {{ ref('x') }}.flag = TRUE}\n"
    )
    formatted = canonical_yaml(text, "t.yml")
    # Laid out canonically around the templates, each kept exactly as written.
    assert formatted == text.replace("    - {{ ref('accessory_sales') }}", "      - {{ ref('accessory_sales') }}")
    assert canonical_yaml(formatted, "t.yml") == formatted
    with pytest.raises(ProjectError) as raised:
        canonical_yaml("a: {{ ref('x')\n", "t.yml")
    assert [item.code for item in raised.value.diagnostics] == ["SST-LOD004"]


def test_a_result_whose_templates_would_not_read_back_is_refused_for_its_file(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.adapters.yaml import format as formatting

    monkeypatch.setattr(formatting, "_restored_templates", lambda text, templates: text)
    with pytest.raises(ProjectError) as raised:
        canonical_yaml("a: {{ ref('x') }}\n", "t.yml")
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.code == "SST-INT003" and "t.yml" in diagnostic.message


@pytest.mark.parametrize(
    "value", ["a\\n\\rb", "\\tx\\ty", "<a/href=\\\\j\\n\\ravascript:{v}\\\\", "mixed \\r\\n\\t\\x07 end"]
)
def test_a_double_quoted_value_with_control_escapes_round_trips(value: str) -> None:
    text = f'models:\n  - name: m\n    meta:\n      sample_values:\n        - "{value}"\n'
    expected = YAML(typ="safe", pure=True).load(text)
    formatted = canonical_yaml(text, "m.yml")
    assert YAML(typ="safe", pure=True).load(formatted) == expected
    assert canonical_yaml(formatted, "m.yml") == formatted
