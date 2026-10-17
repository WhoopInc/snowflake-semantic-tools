"""SST-DIS012: `sst test`, naming no suite, finds no suite that applies.

The smoke suite applies to a semantic view and the eval suite to an eval. A run with no
`--suite` whose selection compiles neither would test nothing, so it is refused, exit 4,
rather than passing with every suite skipped.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import applied_port, common, compile_project, invoke_with_port
from tests.helpers.reference_project import project_copy


def test_sst_dis012_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    exit_code, diagnostics = invoke_json(["test", *common(project), "--select", "type:tool"])
    assert exit_code == 4
    [diagnostic] = [item for item in diagnostics if item["code"] == "SST-DIS012"]
    assert diagnostic["severity"] == "error"
    assert diagnostic["message"].startswith("no test suite applies")


def test_sst_dis012_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = invoke_with_port(
        monkeypatch,
        applied_port(project),
        ["test", *common(project), "--target", "dev", "--select", "type:semantic_view", "--output", "json"],
    )
    envelope = json.loads(result.output)
    assert result.exit_code == 0, result.output
    assert envelope["data"]["suites"] == ["smoke"]
    assert "SST-DIS012" not in [item["code"] for item in envelope["diagnostics"]]


def test_a_bare_run_before_sst_compile_asks_for_it(tmp_path: Path) -> None:
    exit_code, diagnostics = invoke_json(["test", *common(project_copy(tmp_path))])
    assert exit_code == 4
    assert [item["code"] for item in diagnostics if item["severity"] == "error"] == ["SST-MAN001"]
