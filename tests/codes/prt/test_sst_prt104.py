"""SST-PRT104: two flags that exclude each other were both given."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt104_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = invoke_json(["validate", *common(project_copy(tmp_path)), "--verbose", "--quiet"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT104", "error")
    assert diagnostic["message"] == "--verbose and --quiet cannot be combined"
    _, [partial] = invoke_json(["plan", *common(project_copy(tmp_path / "p")), "--partial", "--prune"])
    assert partial["message"] == "--partial and --prune cannot be combined"


def test_sst_prt104_silent(tmp_path: Path) -> None:
    exit_code, _ = invoke_json(["validate", *common(project_copy(tmp_path)), "--quiet"])
    assert exit_code == 0
