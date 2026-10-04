"""SST-PRT102: a selector's prefix is not a registered kind, or it globs where only a name may."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt102_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for selector in ("view:orders", "type:*", "tool:*"):
        exit_code, [diagnostic] = invoke_json(["validate", *common(project), "--select", selector])
        assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT102", "error")
        assert diagnostic["message"] == f"selector '{selector}' names an unknown kind"


def test_sst_prt102_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for selector in ("type:agent", "jaffle_*", "agent:jaffle_minimal_agent", "path:semantic_models/*"):
        exit_code, diagnostics = invoke_json(["validate", *common(project), "--select", selector])
        assert exit_code == 0, (selector, diagnostics)
