"""SST-CFG035: the baseline expires within 30 days."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, BaselineEntry, match_baseline
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy

BASELINE = Baseline(".sst/baseline.json", "2026-10-20", (BaselineEntry("0" * 16, "SST-VAL003"),))


def test_sst_cfg035_fires() -> None:
    [diagnostic] = match_baseline((), BASELINE, today="2026-10-02", warn_from="2026-11-01").notices
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG035", Severity.WARNING)
    assert diagnostic.message == "baseline expires on 2026-10-20; 1 entries remain"
    assert (diagnostic.subject, diagnostic.origin) == ("config:.sst/baseline.json", Origin(".sst/baseline.json"))


def test_sst_cfg035_silent() -> None:
    assert match_baseline((), BASELINE, today="2026-09-01", warn_from="2026-10-01").notices == ()
    assert match_baseline((), None, today="2026-10-02", warn_from="2026-11-01").notices == ()


def _strict_project_baselining_every_warning(root: Path) -> Path:
    """Copy the fixture with `validation.strict: true`, and baseline each warning it reports, for 7 days."""
    project = project_copy(root)
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8").replace("strict: false", "strict: true"), encoding="utf-8")
    report = json.loads(
        CliRunner().invoke(cli, ["validate", *common(project), "--no-strict", "--output", "json"]).output
    )
    entries = [
        {"fingerprint": item["fingerprint"], "code": item["code"]}
        for item in report["diagnostics"]
        if item["severity"] == "warning" and item["code"] != "SST-CFG034"
    ]
    expires_on = (datetime.now(UTC).date() + timedelta(days=7)).isoformat()
    (project / ".sst").mkdir()
    document = {"version": 1, "expires_on": expires_on, "entries": entries}
    (project / ".sst" / "baseline.json").write_text(json.dumps(document), encoding="utf-8")
    return project


def _validated(project: Path, *flags: str) -> tuple[int, dict[str, Any]]:
    result = CliRunner().invoke(cli, ["validate", *common(project), *flags, "--output", "json"])
    envelope: dict[str, Any] = json.loads(result.output)
    return result.exit_code, envelope


def test_sst_cfg035_fires_as_an_error_under_strict(tmp_path: Path) -> None:
    project = _strict_project_baselining_every_warning(tmp_path)

    exit_code, envelope = _validated(project)

    [diagnostic] = [item for item in envelope["diagnostics"] if item["code"] == "SST-CFG035"]
    assert (diagnostic["severity"], diagnostic["promoted_from"]) == ("error", "warning")
    assert exit_code == 1
    blocking = [
        item["code"] for item in envelope["diagnostics"] if item["severity"] == "error" and not item["baselined"]
    ]
    assert blocking == ["SST-CFG035"]
    assert envelope["summary"]["promoted"] == envelope["summary"]["baselined"] + 1
    human = CliRunner().invoke(cli, ["validate", *common(project)])
    assert "error[SST-CFG035] (promoted from warning)" in human.output


def test_sst_cfg035_stays_a_warning_without_strict(tmp_path: Path) -> None:
    exit_code, envelope = _validated(_strict_project_baselining_every_warning(tmp_path), "--no-strict")

    [diagnostic] = [item for item in envelope["diagnostics"] if item["code"] == "SST-CFG035"]
    assert (diagnostic["severity"], diagnostic["promoted_from"], exit_code) == ("warning", None, 0)
