"""SST-MAN033: `sst lineage` reads a compiled manifest older than the project's files."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy

FILTER = Path("semantic_models") / "filters" / "is_large_order.yml"


def _lineage(project: Path, *flags: str) -> tuple[int, list[str]]:
    result = CliRunner().invoke(cli, ["-o", "agent", "lineage", *common(project), *flags, "filter:is_large_order"])
    return result.exit_code, result.stdout.splitlines()


def _compiled(root: Path) -> Path:
    project = project_copy(root)
    assert CliRunner().invoke(cli, ["compile", *common(project)]).exit_code == 0
    return project


def test_sst_man033_fires(tmp_path: Path) -> None:
    # GitHub #268: lineage traced a stale manifest and said nothing.
    project = _compiled(tmp_path)
    path = project / FILTER
    path.write_text(path.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
    exit_code, lines = _lineage(project)
    assert exit_code == 0
    assert (
        "W SST-MAN033 | the compiled manifest is stale (semantic_models/filters/is_large_order.yml changed); "
        "lineage shows the last compile"
    ) in lines


def test_sst_man033_silent(tmp_path: Path) -> None:
    exit_code, lines = _lineage(_compiled(tmp_path))
    assert exit_code == 0 and not any("SST-MAN033" in line for line in lines)


def test_a_node_that_fails_to_compile_reports_its_errors_not_an_unknown_node(tmp_path: Path) -> None:
    # GitHub #268: `--no-manifest` said the node did not exist (SST-PRT112, exit 3).
    project = project_copy(tmp_path)
    path = project / FILTER
    path.write_text(path.read_text(encoding="utf-8").replace("'order_total'", "'order_totl'"), encoding="utf-8")
    exit_code, lines = _lineage(project, "--no-manifest")
    assert exit_code == 1
    assert lines[1].startswith("E SST-REF002 semantic_models/filters/is_large_order.yml:")
    assert not any("SST-PRT112" in line for line in lines)
