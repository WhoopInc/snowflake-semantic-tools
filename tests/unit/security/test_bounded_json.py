"""Every JSON file a user can hand SST is read within a size and nesting bound, and refused cleanly.

Each file kind is given three crafted files: one nested past the depth bound but within Python's
recursion limit, one nested far past that limit, and one larger than the size bound (lowered for
the test). Each is refused with the code that kind already gives a malformed file, or read the
way that kind reads an unusable file, and never escapes as `RecursionError` or `MemoryError`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters import json_files
from snowflake_semantic_tools.adapters.code_coverage import prechecks
from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.baseline import read_baseline
from snowflake_semantic_tools.adapters.fs.local import (
    JsonStore,
    ManifestFileStore,
    ObservationFileStore,
    PlanFileStore,
    StateFileStore,
)
from snowflake_semantic_tools.adapters.json_files import JsonFileError, parse_json, read_json_file
from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.cli.commands.debug import _manifest
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.run_log import RUN_LOG, signature_report
from tests.helpers.cli_projects import project_copy

CRAFTED = ("nested-past-the-bound", "nested-past-recursion", "oversized")
_LIMIT = 256


def nested(depth: int) -> str:
    """An object whose one key holds arrays nested `depth` deep."""
    return '{"k": ' + "[" * depth + "]" * depth + "}"


@pytest.fixture
def crafted(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    """The crafted text named by the test's parameter; for `oversized`, the size bound is lowered."""
    kind = str(request.param)
    if kind == "oversized":
        monkeypatch.setattr(json_files, "MAX_JSON_BYTES", _LIMIT)
        return json.dumps({"k": "x" * (_LIMIT * 2)})
    return nested(json_files.MAX_JSON_DEPTH + 50 if kind == "nested-past-the-bound" else 50_000)


def refused_codes(read: Callable[[], object]) -> list[str]:
    with pytest.raises(ProjectError) as raised:
        read()
    return [item.code for item in raised.value.diagnostics]


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
@pytest.mark.parametrize(
    ("store", "code"),
    [(ManifestFileStore, "SST-MAN002"), (StateFileStore, "SST-MAN022"), (ObservationFileStore, "SST-PRT009")],
)
def test_a_store_with_a_code_refuses_a_crafted_file_with_it(
    tmp_path: Path, crafted: str, store: Callable[[Path], JsonStore[object]], code: str
) -> None:
    path = tmp_path / "document.json"
    path.write_text(crafted, encoding="utf-8")
    assert refused_codes(store(path).read) == [code]


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_a_saved_plan_crafted_file_is_a_value_error_the_runner_reports(tmp_path: Path, crafted: str) -> None:
    """No code names an unusable plan; `cli.runner.guarded` reports a `ValueError` as exit 4."""
    path = tmp_path / "plan.json"
    path.write_text(crafted, encoding="utf-8")
    with pytest.raises(ValueError, match=r"plan\.json: it (nests|is larger)"):
        PlanFileStore(path).read()


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_diff_from_a_crafted_saved_plan_is_refused_as_unreadable(tmp_path: Path, crafted: str) -> None:
    project = project_copy(tmp_path)
    (project / "plan.json").write_text(crafted, encoding="utf-8")
    result = CliRunner().invoke(cli, ["diff", "--project-dir", str(project), "--from", "plan.json", "--output", "json"])
    envelope = json.loads(result.output)
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "SST-PRT009" in [item["code"] for item in envelope["diagnostics"]]


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_a_crafted_baseline_is_refused_as_unreadable(tmp_path: Path, crafted: str) -> None:
    path = tmp_path / "baseline.json"
    path.write_text(crafted, encoding="utf-8")
    assert refused_codes(lambda: read_baseline(path, "baseline.json")) == ["SST-PRT009"]


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_a_crafted_dbt_manifest_is_refused_as_unreadable(tmp_path: Path, crafted: str) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(crafted, encoding="utf-8")
    assert refused_codes(lambda: load_manifest_catalog(path)) == ["SST-PRT009"]


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_a_crafted_mcp_config_is_refused_as_not_valid(tmp_path: Path, crafted: str) -> None:
    path = tmp_path / "mcp-servers" / "dbt" / "mcp.json"
    path.parent.mkdir(parents=True)
    path.write_text(crafted, encoding="utf-8")
    catalog = load_profile_catalog(
        tmp_path, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers", commands_dir="commands"
    )
    [diagnostic] = [item for item in catalog.diagnostics if item.code == "SST-VAL853"]
    assert diagnostic.message.startswith("MCP config 'dbt': mcp.json cannot be used: ")
    assert catalog.mcp_configs == ()


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_debug_reports_no_schema_for_a_crafted_dbt_manifest(tmp_path: Path, crafted: str) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(crafted, encoding="utf-8")
    reported = _manifest(locate_project(project_copy(tmp_path)), path)
    assert (reported["schema_version"], reported["supported"]) == (None, None)


@pytest.mark.parametrize("crafted", CRAFTED, indirect=True)
def test_the_coverage_catalog_reads_as_empty_when_crafted(tmp_path: Path, crafted: str) -> None:
    path = tmp_path / "catalog.json"
    path.write_text(crafted, encoding="utf-8")
    assert prechecks(path) == {}


@pytest.mark.parametrize("depth", [json_files.MAX_JSON_DEPTH + 50, 50_000])
def test_a_crafted_run_log_line_is_skipped(tmp_path: Path, depth: int) -> None:
    (tmp_path / RUN_LOG).write_text(nested(depth) + '\n{"code": "SST-SNO001"}\n', encoding="utf-8")
    assert signature_report(tmp_path)["unmatched"] == 1


def test_an_oversized_run_log_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(json_files, "MAX_JSON_BYTES", _LIMIT)
    (tmp_path / RUN_LOG).write_text('{"code": "SST-SNO001"}\n' * 20, encoding="utf-8")
    with pytest.raises(JsonFileError, match="larger than"):
        signature_report(tmp_path)


@pytest.mark.parametrize(
    "lock",
    [
        nested(json_files.MAX_JSON_DEPTH + 50),
        nested(50_000),
        json.dumps({"run_id": "old", "created_at": "2000-01-01T00:00:00+00:00", "pad": "x" * 8192}),
    ],
    ids=["nested-past-the-bound", "nested-past-recursion", "oversized"],
)
def test_a_crafted_lock_is_held_by_nobody_and_never_stale(tmp_path: Path, lock: str) -> None:
    store = StateFileStore(tmp_path / "state.json", now=lambda: datetime(2026, 1, 1, tzinfo=UTC))
    (tmp_path / "state.json.lock").write_text(lock, encoding="utf-8")
    assert store.acquire_lock("new", break_stale=True) == (False, None, False)
    store.release_lock("old")
    assert (tmp_path / "state.json.lock").read_text(encoding="utf-8") == lock


def test_the_reader_keeps_what_is_within_its_bounds(tmp_path: Path) -> None:
    path = tmp_path / "document.json"
    path.write_text(nested(json_files.MAX_JSON_DEPTH - 1), encoding="utf-8")
    assert isinstance(read_json_file(path), dict)
    assert parse_json(b'{"a": 1}', max_depth=1) == {"a": 1}
    with pytest.raises(JsonFileError, match="deeper than 1 levels"):
        parse_json("[[]]", max_depth=1)
    with pytest.raises(JsonFileError, match="can't decode"):
        parse_json(b"\xff")
    with pytest.raises(FileNotFoundError):
        read_json_file(tmp_path / "absent.json")
