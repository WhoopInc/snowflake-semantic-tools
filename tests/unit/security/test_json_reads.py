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
    ("app/evals/inline.py", "_plan"): "the agent specification SST rendered with json.dumps",
    ("app/lifecycle/eval_rules.py", "provenance"): (
        "a dataset version's METADATA, which SST rendered with json.dumps or SHOW VERSIONS returned"
    ),
    ("app/lifecycle/eval_rules.py", "with_provenance"): (
        "the dataset version METADATA SST rendered with json.dumps itself"
    ),
    ("app/prior.py", "_specification"): "the specification text SHOW VERSIONS IN AGENT returned",
    ("app/importing.py", "_read_agent"): "the specification text SHOW VERSIONS IN AGENT returned",
    ("app/importing_text.py", "parse_profile"): (
        "the profile column SHOW AGENTS returned, which Snowflake bounds to 1,000 bytes"
    ),
    ("app/importing_text.py", "parse_manifest"): (
        "a plugin version's .cortex-plugin/plugin.json, within Snowflake's 2 MB scan limit per file"
    ),
    ("adapters/snowflake/connector/agent_stream.py", "_answer"): (
        "a Cortex REST call's answer, which Snowflake sends as one small JSON object"
    ),
    ("adapters/snowflake/connector/agent_stream.py", "_failure"): (
        "a Cortex REST error body, cut to its first 2000 characters before it is parsed"
    ),
    ("app/ask_conversation.py", "_spec_fields"): "the agent specification SST rendered with json.dumps",
    ("domain/ask/spec.py", "rewrite_spec"): "the agent specification SST rendered with json.dumps",
    ("domain/ask/spec.py", "referenced_names"): "the agent specification SST rendered with json.dumps",
    ("domain/ask/spec.py", "_spec_object"): "the agent specification SST rendered with json.dumps",
    ("domain/ask/transcript.py", "_revision"): "the agent specification SST rendered and sent, as rewrite_spec made it",
    ("domain/ask/events.py", "_parsed"): "one server-sent event's payload, whose lines the stream adapter bounds",
    ("domain/model/lifecycle/version.py", "spec_fingerprint"): (
        "an agent specification SST rendered, or the text SHOW VERSIONS IN AGENT returned"
    ),
    ("domain/evaluate/judge.py", "judged_score"): (
        "a judge's completion, which the connector decoded from its VARIANT column unless it was JSON text"
    ),
    ("domain/evaluate/judge.py", "judged_reason"): (
        "a judge's completion, which the connector decoded from its VARIANT column unless it was JSON text"
    ),
    ("domain/render/compare.py", "with_orchestration_model"): (
        "the agent specification SST rendered with json.dumps, as the compiled manifest holds it"
    ),
    ("domain/render/eval.py", "render_source_table_statements"): (
        "the payload render_dataset_payload rendered with json.dumps"
    ),
    ("domain/validate/agent_live.py", "live_spec"): "the specification text DESCRIBE AGENT returned",
    ("domain/validate/publication.py", "_unknown_metadata"): (
        "dataset version METADATA SST rendered, or Snowflake reported for a version"
    ),
    ("domain/plan/definitions/agent.py", "spec_definition"): (
        "an agent specification SST rendered, or the version an agent serves returned"
    ),
    ("domain/plan/definitions/agent.py", "_profile"): (
        "an agent profile SST rendered, or the profile column SHOW AGENTS returned"
    ),
    ("domain/plan/definitions/composite.py", "_json"): (
        "a profile registry column or eval ground truth, as SST rendered it or a VARIANT returned it"
    ),
    (
        "domain/plan/definitions/composite.py",
        "payload_questions",
    ): "the dataset payload render_dataset_payload rendered",
    ("domain/plan/definitions/semantic_view.py", "_parse"): (
        "one property value DESCRIBE SEMANTIC VIEW returned, such as a member's synonyms"
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
