"""JSON is parsed in one place, `adapters.json_files`, which bounds its size and nesting.

Any other `json.load` or `json.loads` in the package is refused, except the in-memory parses
listed in `EXEMPT`, each with why it needs no bound. A file is never among them: every JSON file
SST reads goes through `read_json_file`. Domain and app code cannot import an adapter, so text
SST rendered itself, or a value a Snowflake row carries, is parsed there directly.
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[3] / "snowflake_semantic_tools"
READER = "adapters/json_files.py"
_PARSERS = frozenset({"load", "loads", "JSONDecoder"})

# (module, enclosing function) -> why that parse needs no bound.
EXEMPT: dict[tuple[str, str], str] = {
    ("app/desktop_contract.py", "desktop_value"): (
        "a column value of a row a Snowflake query returned, parsed as Desktop parses it"
    ),
    ("app/evals/retrieve.py", "_expected_question_map"): (
        "the dataset payload SST rendered with json.dumps from a loaded YAML dataset"
    ),
    ("app/evals/retrieve.py", "_variant"): "a VARIANT column of a row a Snowflake query returned",
    ("app/lifecycle/evals.py", "_with_provenance"): (
        "the dataset version METADATA SST rendered with json.dumps itself"
    ),
    ("domain/render/eval.py", "render_source_table_statements"): (
        "the payload render_dataset_payload rendered with json.dumps"
    ),
    ("domain/validate/agent_live.py", "live_spec"): "the specification text DESCRIBE AGENT returned",
    ("domain/validate/publication.py", "_unknown_metadata"): (
        "dataset version METADATA SST rendered, or Snowflake reported for a version"
    ),
}


def json_parses() -> set[tuple[str, str]]:
    """Every `(module, enclosing function)` that calls a `json` parser or imports one by name."""
    found: set[tuple[str, str]] = set()
    for path in sorted(PACKAGE.rglob("*.py")):
        if " " in path.name:
            continue
        module = path.relative_to(PACKAGE).as_posix()
        _visit(ast.parse(path.read_text(encoding="utf-8")), module, "<module>", found)
    return found


def _visit(node: ast.AST, module: str, function: str, found: set[tuple[str, str]]) -> None:
    for child in ast.iter_child_nodes(node):
        inner = child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else function
        if _is_parse(child):
            found.add((module, function))
        _visit(child, module, inner, found)


def _is_parse(node: ast.AST) -> bool:
    if isinstance(node, ast.ImportFrom) and node.module == "json":
        return any(alias.name in _PARSERS for alias in node.names)
    if isinstance(node, ast.Import):
        return any(alias.name == "json" and alias.asname is not None for alias in node.names)
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "json"
        and node.attr in _PARSERS
    )


def test_json_is_parsed_only_by_the_bounded_reader_or_an_exempt_in_memory_parse() -> None:
    unexpected = sorted(site for site in json_parses() if site[0] != READER and site not in EXEMPT)
    assert unexpected == [], "parse JSON through adapters.json_files, or exempt an in-memory parse here"


def test_no_exemption_outlives_its_parse() -> None:
    assert sorted(set(EXEMPT) - json_parses()) == []


def test_the_bounded_reader_is_where_json_is_parsed() -> None:
    assert (READER, "parse_json") in json_parses()


def test_the_scan_sees_each_way_of_reaching_a_parser() -> None:
    found: set[tuple[str, str]] = set()
    source = (
        "import json as j\n"
        "from json import loads\n"
        "def read(handle):\n"
        "    return json.load(handle)\n"
        "def decode(text):\n"
        "    return json.JSONDecoder().raw_decode(text)\n"
    )
    _visit(ast.parse(source), "probe.py", "<module>", found)
    assert found == {("probe.py", "<module>"), ("probe.py", "read"), ("probe.py", "decode")}
