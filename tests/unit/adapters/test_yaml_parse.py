"""The YAML parse module: `parse_yaml_bytes` and the two readers built on it."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml, SourcePosition, TemplateSource
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes, read_yaml_file, read_yaml_mapping
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, Origin


def _raised(raw: bytes, path: str = "f.yml") -> ProjectError:
    with pytest.raises(ProjectError) as exc_info:
        parse_yaml_bytes(raw, path)
    return exc_info.value


def test_templates_are_restored_verbatim_and_located_where_written() -> None:
    parsed = parse_yaml_bytes(b"expr: SUM({{ ref('o', 'x') }})\ntables:\n  - {{ ref('o') }}\n", "m.yml")
    assert dict(parsed.tree) == {"expr": "SUM({{ ref('o', 'x') }})", "tables": ["{{ ref('o') }}"]}
    assert sorted((source.raw, source.line, source.col) for source in parsed.templates.values()) == [
        ("{{ ref('o') }}", 3, 5),
        ("{{ ref('o', 'x') }}", 1, 11),
    ]
    assert parsed.line_index[("tables", 0)] == SourcePosition(3, 5)


def test_a_block_scalar_is_read_as_text_with_no_template_recorded() -> None:
    parsed = parse_yaml_bytes(b"note: |\n  Write {{ ref( to name a model.\n", "m.yml")
    assert parsed.tree == {"note": "Write {{ ref( to name a model.\n"}
    assert parsed.templates == {}


def test_top_level_keys_become_strings_and_nested_values_keep_their_yaml_types() -> None:
    parsed = parse_yaml_bytes(b"2: two\ntrue: yes\nnested:\n  3: three\n", "m.yml")
    assert dict(parsed.tree) == {"2": "two", "True": True, "nested": {3: "three"}}
    assert isinstance(parsed.tree, MappingProxyType)


@pytest.mark.parametrize("raw", (b"---\n", b"~\n", b"null\n"), ids=("bare-marker", "tilde", "null"))
def test_a_null_document_is_an_empty_tree(raw: bytes) -> None:
    assert parse_yaml_bytes(raw, "m.yml") == ParsedYaml(
        MappingProxyType({}), MappingProxyType({}), MappingProxyType({})
    )


@pytest.mark.parametrize("raw", (b"", b"\n\n", b"# only a comment\n"), ids=("empty", "blank", "comment"))
def test_a_file_without_a_document_is_lod003(raw: bytes) -> None:
    assert _raised(raw, "empty.yml").diagnostics == (D("SST-LOD003", file="empty.yml"),)


def test_bytes_that_are_not_utf8_are_prs122_at_their_offset() -> None:
    error = _raised(b"a: \xff\n", "bad.yml")
    assert error.diagnostics == (D("SST-PRS122", origin=Origin("bad.yml"), file="bad.yml", offset=3),)
    assert isinstance(error.__cause__, UnicodeDecodeError)


def test_every_malformed_template_is_reported_at_once() -> None:
    error = _raised(b"a: {{ ref(\nb: {{ x {{ y }} }}\n", "t.yml")
    assert error.diagnostics == (
        D("SST-LOD004", file="t.yml", line=1, col=4, reason="unterminated template expression"),
        D("SST-LOD004", file="t.yml", line=2, col=9, reason="nested template expression"),
    )
    assert str(error) == "; ".join(diagnostic.message for diagnostic in error.diagnostics)


@pytest.mark.parametrize(
    ("raw", "diagnostic"),
    (
        (b"- 1\n- 2\n", D("SST-LOD002", file="f.yml", found="list")),
        (b"a: 1\n---\nb: 2\n---\nc: 3\n", D("SST-LOD008", file="f.yml", count=3)),
        (b"a: 1\nb:\n  c: 1\n  c: 2\n", D("SST-LOD005", file="f.yml", line=4, key="c")),
    ),
    ids=("list-root", "stream", "nested-duplicate"),
)
def test_documents_of_the_wrong_shape_carry_one_diagnostic(raw: bytes, diagnostic: object) -> None:
    assert _raised(raw).diagnostics == (diagnostic,)


def test_a_syntax_error_is_lod001_at_the_mark_and_keeps_the_yaml_error_as_cause() -> None:
    error = _raised(b"a: 1\nb: [\n", "s.yml")
    (diagnostic,) = error.diagnostics
    assert (diagnostic.code, diagnostic.context["file"], diagnostic.context["line"]) == ("SST-LOD001", "s.yml", 3)
    assert error.__cause__ is not None


def test_an_earlier_merged_mapping_wins_over_a_later_one() -> None:
    parsed = parse_yaml_bytes(b"a: &a {k: 1}\nb: &b {k: 2, j: 2}\nuse:\n  <<: [*a, *b]\n", "m.yml")
    assert parsed.tree["use"] == {"k": 1, "j": 2}


def test_read_yaml_mapping_returns_a_plain_dict_and_names_the_file_by_its_path(tmp_path: Path) -> None:
    path = tmp_path / "sst_config.yml"
    path.write_text("project:\n  name: demo\n", encoding="utf-8")
    loaded = read_yaml_mapping(path)
    assert loaded == {"project": {"name": "demo"}}
    assert type(loaded) is dict
    path.write_text("a: 1\na: 2\n", encoding="utf-8")
    with pytest.raises(ProjectError) as exc_info:
        read_yaml_mapping(path)
    assert exc_info.value.diagnostics == (D("SST-LOD005", file=str(path), line=2, key="a"),)


def test_read_yaml_mapping_raises_an_unreadable_file_without_diagnostics(tmp_path: Path) -> None:
    path = tmp_path / "missing.yml"
    with pytest.raises(ProjectError) as exc_info:
        read_yaml_mapping(path)
    assert str(exc_info.value).startswith(f"cannot read {path}: ")
    assert exc_info.value.diagnostics == ()
    assert isinstance(exc_info.value.__cause__, OSError)


def test_read_yaml_file_returns_the_parse_and_leaves_the_sink_alone(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    path.write_text("agent: a\n", encoding="utf-8")
    sink = [D("SST-LOD003", file="earlier.yml")]
    parsed = read_yaml_file(path, "agents/a/config.yml", sink)
    assert parsed == parse_yaml_bytes(b"agent: a\n", "agents/a/config.yml")
    assert sink == [D("SST-LOD003", file="earlier.yml")]


def test_read_yaml_file_collects_parse_diagnostics_after_what_the_sink_holds(tmp_path: Path) -> None:
    path = tmp_path / "bad.yml"
    path.write_text("a: {{ ref(\n", encoding="utf-8")
    sink = [D("SST-LOD003", file="earlier.yml")]
    assert read_yaml_file(path, "evals/bad.yml", sink) is None
    assert sink == [
        D("SST-LOD003", file="earlier.yml"),
        D("SST-LOD004", file="evals/bad.yml", line=1, col=4, reason="unterminated template expression"),
    ]


def test_an_unreadable_file_is_lod018_at_the_file_itself(tmp_path: Path) -> None:
    sink: list[Diagnostic] = []
    assert read_yaml_file(tmp_path / "gone.yml", "eval_metrics/gone.yml", sink) is None
    assert sink == [
        D(
            "SST-LOD018",
            file="eval_metrics/gone.yml",
            path="eval_metrics/gone.yml",
            origin=Origin("eval_metrics/gone.yml"),
        )
    ]


def test_an_unreadable_pointed_to_file_is_lod018_at_the_pointer(tmp_path: Path) -> None:
    sink: list[Diagnostic] = []
    pointer = Origin("agents/a/agent.yml", 1, 1)
    assert read_yaml_file(tmp_path / "gone.yml", "agents/a/evals/dataset.yml", sink, pointer_origin=pointer) is None
    assert sink == [D("SST-LOD018", file="agents/a/agent.yml", path="agents/a/evals/dataset.yml", origin=pointer)]
    assert sink[0].message == "agents/a/agent.yml references agents/a/evals/dataset.yml, which does not exist"


def test_template_sources_record_the_one_based_column_after_the_last_newline() -> None:
    parsed = parse_yaml_bytes(b"a: 1\nb: '{{ var(\"x\") }}'\n", "m.yml")
    assert list(parsed.templates.values()) == [TemplateSource('{{ var("x") }}', 2, 5)]
