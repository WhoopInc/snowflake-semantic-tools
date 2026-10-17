"""`sst migrate split` and `sst migrate refs` carry the reference project from its earlier forms to now.

The pre-split copy writes members as lists, tools in groups, settings outside `config:`, bare
references, and the old `skills:` blocks. Migrating it, in either order, must leave a project
that validates with no errors and renders every view, the defined tool, and every agent exactly
as the goldens hold them; running either command again finds nothing left to do.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_json import invoke_json
from tests.helpers.reference_project import DBT_MANIFEST, REPO_ROOT, pre_split_copy

GOLDENS = REPO_ROOT / "tests" / "golden" / "expected"
VIEWS = ("jaffle_menu", "jaffle_minimal", "jaffle_sales")
AGENTS = ("jaffle_analytics_agent", "jaffle_delivery_agent", "jaffle_minimal_agent")
# The warnings the reference project carries on purpose (sst_config.yml says why).
EXPECTED_WARNINGS = Counter({"SST-RND013": 1, "SST-VAL528": 1, "SST-RND010": 1, "SST-CFG018": 2})


def sst(*args: str) -> int:
    return CliRunner().invoke(cli, list(args)).exit_code


def without_header(text: str) -> str:
    """Return a golden or rendered DDL file's statement, its provenance comments and `;` dropped."""
    lines = text.splitlines()
    start = next(index for index, line in enumerate(lines) if line.strip() and not line.startswith("--"))
    return "\n".join(lines[start:]).rstrip().rstrip(";")


@pytest.mark.parametrize("order", [("split", "refs"), ("refs", "split")])
def test_the_pre_split_reference_project_migrates_to_the_goldens(tmp_path: Path, order: tuple[str, str]) -> None:
    project = pre_split_copy(tmp_path)
    where = ("--project-dir", str(project))
    for command in order:
        assert sst("migrate", command, *where) == 2
        assert sst("migrate", command, *where, "--write") == 0
    for command in order:
        assert sst("migrate", command, *where) == 0
    assert not (project / "tools" / "jaffle_platform.yml").exists()
    assert (project / "tools" / "jaffle_partner" / "partner_delivery_agent.yml").is_file()
    assert "  jaffle_partner:\n    +immutable: true\n" in (project / "sst_config.yml").read_text(encoding="utf-8")

    exit_code, diagnostics = invoke_json(["validate", *where, "--manifest", str(DBT_MANIFEST), "--no-strict"])
    assert exit_code == 0, [item for item in diagnostics if item["severity"] == "error"]
    warnings = Counter(item["code"] for item in diagnostics if item["severity"] == "warning")
    assert warnings == EXPECTED_WARNINGS

    ddl, specs = tmp_path / "ddl", tmp_path / "specs"
    compiled = (
        "compile",
        *where,
        "--manifest",
        str(DBT_MANIFEST),
        "--emit-ddl",
        str(ddl),
        "--emit-agent-spec",
        str(specs),
    )
    assert sst(*compiled) == 0
    for view in VIEWS:
        rendered = (ddl / "semantic_view" / f"{view}.sql").read_text(encoding="utf-8")
        assert without_header(rendered) == without_header((GOLDENS / "ddl" / f"{view}.sql").read_text(encoding="utf-8"))
    tool = (ddl / "tool" / "menu_docs_search.sql").read_text(encoding="utf-8")
    assert without_header(tool) == without_header(
        (GOLDENS / "tool" / "menu_docs_search.sql").read_text(encoding="utf-8")
    )
    for agent in AGENTS:
        assert (specs / f"{agent}.json").read_bytes() == (GOLDENS / "agent" / f"{agent}.json").read_bytes()
