"""SST-PRT113: a selector globs after a kind that takes an exact value."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt113_fires(tmp_path: Path) -> None:
    # GitHub #272: `agent:my*` was reported as naming an unknown kind, though `agent` is one.
    project = project_copy(tmp_path)
    suggestions = {
        "agent:jaffle*": (
            "use an exact value after 'agent:', or the bare glob 'jaffle*', which matches a name of every type"
        ),
        "type:*": "use an exact value after 'type:', since only a bare name or a path: selector takes a glob",
    }
    for selector, suggestion in suggestions.items():
        exit_code, [diagnostic] = invoke_json(["validate", *common(project), "--select", selector])
        assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT113", "error")
        kind = selector.partition(":")[0]
        assert diagnostic["message"] == f"selector '{selector}' has a glob after '{kind}:', which takes an exact value"
        assert diagnostic["suggestion"] == suggestion


def test_sst_prt113_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    for selector in ("jaffle*", "agent:jaffle_minimal_agent", "path:semantic_models/*"):
        exit_code, diagnostics = invoke_json(["validate", *common(project), "--select", selector])
        assert exit_code == 0, (selector, diagnostics)
