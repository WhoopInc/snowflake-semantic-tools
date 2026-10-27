"""SST-CFG062: a baseline entry records a code SST does not report, so it holds nothing."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry, match_baseline
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy

ENTRIES = (
    BaselineEntry("0" * 16, "SST-VAL003"),
    BaselineEntry("1" * 16, "SST-NOPE999"),
    BaselineEntry("2" * 16, "SST-MEM003"),
)


def _notices(*entries: BaselineEntry) -> list[tuple[str, str]]:
    baseline = Baseline(".sst/baseline.json", "2099-01-01", entries)
    found = match_baseline((), baseline, today="2026-10-02", warn_from="2026-11-01").notices
    return [(item.code, item.message) for item in found]


def test_sst_cfg062_fires() -> None:
    baseline = Baseline(".sst/baseline.json", "2099-01-01", ENTRIES)
    first, second = match_baseline((), baseline, today="2026-10-02", warn_from="2026-11-01").notices
    assert (first.code, first.severity) == ("SST-CFG062", Severity.WARNING)
    assert first.message == "baseline entry 2 records SST-NOPE999, which SST does not report; no code replaced it"
    assert (first.subject, first.origin) == ("config:.sst/baseline.json", Origin(".sst/baseline.json"))
    # A retired code names the code that took its condition over.
    assert second.message == (
        "baseline entry 3 records SST-MEM003, which SST does not report; it was replaced by SST-REF001"
    )


def test_sst_cfg062_silent() -> None:
    assert _notices(ENTRIES[0]) == []


def _project(root: Path, *entries: dict[str, object]) -> Path:
    project = project_copy(root)
    (project / ".sst").mkdir()
    expires_on = (datetime.now(UTC).date() + timedelta(days=90)).isoformat()
    document = {"version": 1, "expires_on": expires_on, "entries": list(entries)}
    (project / ".sst" / "baseline.json").write_text(json.dumps(document), encoding="utf-8")
    return project


def test_validate_and_show_warn_and_prune_removes_it(tmp_path: Path) -> None:
    # GitHub #273: such an entry was ignored silently, and prune could not remove a connected one.
    unknown = {"fingerprint": "f" * 16, "code": "SST-NOPE999", "connected": True, "target": "dev"}
    project = _project(tmp_path, unknown)
    for command in (["validate"], ["baseline", "show"]):
        result = CliRunner().invoke(cli, ["-o", "json", *command, *common(project)])
        codes = [item["code"] for item in json.loads(result.output)["diagnostics"]]
        assert "SST-CFG062" in codes, command
    pruned = CliRunner().invoke(cli, ["-o", "json", "baseline", "prune", *common(project)])
    assert pruned.exit_code == 0, pruned.output
    assert [entry["code"] for entry in json.loads(pruned.output)["data"]["pruned"]] == ["SST-NOPE999"]
    assert json.loads((project / ".sst" / "baseline.json").read_text(encoding="utf-8"))["entries"] == []
