"""The canonical YAML form: idempotent, value-preserving, and the cure for every SST-VAL009 finding."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from ruamel.yaml import YAML

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.format import canonical_yaml
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
        expected = _value(text)
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
        expected = _value(text)
    except Exception:  # a line the YAML scanner reads as structure is not this test's input
        return
    once = canonical_yaml(text, "f.yml")
    assert _value(once) == expected
    assert canonical_yaml(once, "f.yml") == once
    [header] = [line for line in once.split("\n") if "freshness:" in line]
    assert header.split(": ", 1)[1][0] == indicator


def test_layout_name_order_comments_quotes_and_nulls_survive() -> None:
    text = "# head\nitems:\n- expr: x\n  name: n\n- name: m  # kept\n  expr: y\nq: 'quoted'\nz: null\nw: ~\n"
    assert canonical_yaml(text, "f.yml") == (
        "# head\nitems:\n  - name: n\n    expr: x\n  - name: m # kept\n    expr: y\nq: 'quoted'\nz: null\nw: null\n"
    )


def test_a_commented_mapping_keeps_its_key_order() -> None:
    text = "items:\n  - expr: x\n    # about the name\n    name: n\n"
    assert canonical_yaml(text, "f.yml") == text


def test_whitespace_a_block_scalar_holds_becomes_a_quoted_scalar() -> None:
    assert canonical_yaml("a: |\n  x  \n  y\n", "f.yml") == 'a: "x  \\ny\\n"\n'
    assert canonical_yaml("c: |-\n  \tindented\n  ok\n", "f.yml") == 'c: "\\tindented\\nok"\n'


def test_document_markers_and_empty_files() -> None:
    assert canonical_yaml("---\na: 1\n---\nb: 2\n", "f.yml") == "---\na: 1\n---\nb: 2\n"
    assert canonical_yaml("  \n\n", "f.yml") == ""
    assert canonical_yaml("a: 1", "f.yml") == "a: 1\n"


def test_sanitize_repairs_apostrophes_and_jinja_and_nothing_else() -> None:
    text = (
        "t:\n  synonyms: [\"bob's\", 'it''s', plain]\n  sample_values:\n    - \u2019q\u2019\n"
        "  description: 'see {{ x }} {% if %} {# c #}'\n  expr: \"it's\"\n"
    )
    repaired = canonical_yaml(text, "f.yml", sanitize=True)
    assert _value(repaired) == {
        "t": {
            "synonyms": ["bobs", "its", "plain"],
            "sample_values": ["q"],
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
