"""SST-DIS010: a `--select` selector matched no artifact.

Each value is reported on its own, by every command that takes `--select`: one that misses among
others that match warns, and the run goes on with what matched; only when nothing matches at all
is the run refused, exit 4.
"""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compiled_universe, selector_report
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common, compile_project
from tests.helpers.compiled_things import Thing
from tests.helpers.reference_project import project_copy

COMPILED = compiled_universe((Thing("orders"), Thing("lookup", "tool")))
PARTIAL = ("--select", "jaffle_minimal", "--select", "nope_xyz")


def test_sst_dis010_fires() -> None:
    [diagnostic] = selector_report(("orders", "ordrs"), (), COMPILED)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS010", Severity.WARNING)
    assert diagnostic.message == "--select ordrs matched no artifact"


def test_sst_dis010_silent() -> None:
    assert selector_report(("orders", "type:tool"), (), COMPILED) == ()


def _missed(diagnostics: list[dict[str, object]]) -> list[object]:
    return [item["message"] for item in diagnostics if item["code"] == "SST-DIS010"]


def test_a_selector_that_misses_beside_one_that_matches_warns_on_validate_compile_and_list(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    missed = ["--select nope_xyz matched no artifact"]
    exit_code, diagnostics = invoke_json(["validate", *common(project), "--no-strict", *PARTIAL])
    assert (exit_code, _missed(diagnostics)) == (0, missed)
    exit_code, diagnostics = invoke_json(["compile", *common(project), "--select", "jaffle_minimal nope_xyz"])
    assert (exit_code, _missed(diagnostics)) == (0, missed)
    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), *PARTIAL, "-o", "json"])
    envelope = json.loads(listed.output)
    assert (listed.exit_code, _missed(envelope["diagnostics"])) == (0, missed)
    assert [item["key"] for item in envelope["data"]["items"]] == ["semantic_view:jaffle_minimal"]


def test_strict_validate_promotes_a_selector_that_misses_to_an_error(tmp_path: Path) -> None:
    exit_code, diagnostics = invoke_json(["validate", *common(project_copy(tmp_path)), "--strict", *PARTIAL])
    assert exit_code == 1
    assert [item["severity"] for item in diagnostics if item["code"] == "SST-DIS010"] == ["error"]


def test_list_refuses_selectors_that_all_miss(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--select", "nope_xyz", "-o", "json"])
    assert listed.exit_code == 4
    assert _missed(json.loads(listed.output)["diagnostics"]) == ["--select nope_xyz matched no artifact"]
