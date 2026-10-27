"""SST-DIS011: an `--exclude` selector matched no artifact, so it left nothing out."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.cli.wiring.compile import compiled_universe, selector_report
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.compiled_things import Thing
from tests.helpers.reference_project import project_copy

COMPILED = compiled_universe((Thing("orders"), Thing("lookup", "tool")))


def test_sst_dis011_fires() -> None:
    [diagnostic] = [item for item in selector_report((), ("lookup", "lokup"), COMPILED) if item.code != "SST-DIS201"]
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS011", Severity.WARNING)
    assert diagnostic.message == "--exclude lokup matched no artifact"


def test_sst_dis011_silent() -> None:
    assert [item.code for item in selector_report((), ("lookup", "type:tool"), COMPILED)] == ["SST-DIS201"]


def test_validate_reports_an_exclusion_that_matched_nothing(tmp_path: Path) -> None:
    exit_code, diagnostics = invoke_json(
        ["validate", *common(project_copy(tmp_path)), "--no-strict", "--exclude", "nope_xyz"]
    )
    assert exit_code == 0
    assert [item["message"] for item in diagnostics if item["code"] == "SST-DIS011"] == [
        "--exclude nope_xyz matched no artifact"
    ]
