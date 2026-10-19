"""Whitespace never blocks a command and never changes what publishes."""

from __future__ import annotations

import re
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import QUERIES, project_copy

_BLOCK_HEADER = re.compile(r"(?::|^\s*-)\s*\|[-+]?\s*$")


def _padded(project: Path) -> int:
    """Add trailing spaces to every line inside a `|` block scalar, where YAML keeps them, and use CRLF.

    Returns how many lines were padded.
    """
    padded = 0
    for path in sorted(project.rglob("*.yml")):
        lines, opened_at = [], None
        for line in path.read_text(encoding="utf-8").split("\n"):
            indent = len(line) - len(line.lstrip(" "))
            if opened_at is not None and line.strip() and indent <= opened_at:
                opened_at = None
            if opened_at is not None and line.strip():
                line, padded = line + "  \t", padded + 1
            lines.append(line)
            if _BLOCK_HEADER.search(line):
                opened_at = indent
        path.write_bytes("\r\n".join(lines).encode("utf-8"))
    return padded


def _emitted(project: Path) -> dict[str, bytes]:
    args = ["compile", *common(project), "--emit-ddl", str(project / "ddl"), "--emit-agent-spec", str(project / "spec")]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    return {
        str(path.relative_to(project)): path.read_bytes()
        for folder in ("ddl", "spec")
        for path in (project / folder).rglob("*")
        if path.is_file()
    }


def test_strict_promotes_no_whitespace_finding(tmp_path: Path) -> None:
    clean, spaced = project_copy(tmp_path / "clean"), project_copy(tmp_path / "spaced")
    path = spaced / QUERIES
    path.write_bytes(path.read_text(encoding="utf-8").replace("\n", "  \r\n").encode("utf-8"))
    before = invoke_json(["validate", *common(clean), "--strict", "--no-snowflake-syntax-check"])
    after = invoke_json(["validate", *common(spaced), "--strict", "--no-snowflake-syntax-check"])
    assert after[0] == before[0]
    found = {(item["code"], item["severity"]) for item in after[1] if item["code"] in ("SST-LOD012", "SST-VAL009")}
    assert found == {("SST-LOD012", "info")}
    errors = [item["code"] for item in after[1] if item["severity"] == "error"]
    assert errors == [item["code"] for item in before[1] if item["severity"] == "error"]


def test_trailing_whitespace_and_crlf_render_the_same_payloads(tmp_path: Path) -> None:
    clean, spaced = project_copy(tmp_path / "clean"), project_copy(tmp_path / "spaced")
    assert _padded(spaced) > 100
    expected = _emitted(clean)
    assert expected
    assert _emitted(spaced) == expected
