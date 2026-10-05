"""The CI tools that run outside pytest, offline: the coverage ratchet, the scratch sweep, the recorder."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.domain.model.lifecycle import (
    ExecResult,
    ExecutionError,
    OwnershipMarker,
    QueryResult,
    ShowRow,
)
from tests.helpers import coverage_ratchet, session_recorder, sweep_scratch
from tests.helpers.live_project import live_view, orders_table
from tests.helpers.live_snowflake import SCRATCH_MARKER, LiveAccount, scratch_schema_name, scratch_scope
from tests.helpers.snowflake_fake import FakeSnowflake

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
ACCOUNT = LiveAccount("acct", "ci_user", "CI_ROLE", "CI_WH", "SCRATCH_DB", private_key_path="/keys/ci.p8")


def coverage_report(**layers: tuple[int, int, int, int]) -> dict[str, Any]:
    """A `coverage json` report with one file per layer: covered and total lines, then branches."""
    return {
        "files": {
            f"snowflake_semantic_tools/{layer}/module.py": {
                "summary": {
                    "covered_lines": lines,
                    "num_statements": statements,
                    "covered_branches": covered,
                    "num_branches": branches,
                }
            }
            for layer, (lines, statements, covered, branches) in layers.items()
        }
        | {"tests/unit/test_x.py": {"summary": {"covered_lines": 0, "num_statements": 9}}}
    }


FULL = {"domain": (10, 10, 4, 4), "app": (995, 1000, 0, 0), "cli": (976, 1000, 913, 1000), "adapters": (1, 3, 1, 3)}


def test_measure_floors_each_layer_to_one_decimal_and_counts_no_branches_as_full() -> None:
    measured = coverage_ratchet.measure(coverage_report(**FULL))
    assert measured == {
        "domain": {"line": 100.0, "branch": 100.0},
        "app": {"line": 99.5, "branch": 100.0},
        "cli": {"line": 97.6, "branch": 91.3},
        "adapters": {"line": 33.3, "branch": 33.3},
    }
    with pytest.raises(ValueError, match="no file of: adapters"):
        coverage_ratchet.measure(coverage_report(**{key: FULL[key] for key in ("domain", "app", "cli")}))


def test_check_fails_on_any_decrease_and_raise_never_lowers_a_number(tmp_path: Path) -> None:
    report = tmp_path / "coverage.json"
    report.write_text(json.dumps(coverage_report(**FULL)), encoding="utf-8")
    baseline = tmp_path / "baseline.json"
    numbers = coverage_ratchet.measure(coverage_report(**FULL))
    baseline.write_text(json.dumps(numbers), encoding="utf-8")
    args = ["--coverage-json", str(report), "--baseline", str(baseline)]
    assert coverage_ratchet.main(["check", *args]) == 0

    higher = {layer: dict(values) for layer, values in numbers.items()}
    higher["cli"]["branch"] = 91.4
    higher["app"]["line"] = 90.0
    baseline.write_text(json.dumps(higher), encoding="utf-8")
    assert coverage_ratchet.main(["check", *args]) == 1
    assert coverage_ratchet.main(["raise", *args]) == 0
    written = json.loads(baseline.read_text(encoding="utf-8"))
    assert written["cli"]["branch"] == 91.4 and written["app"]["line"] == 99.5

    baseline.write_text(json.dumps({"domain": numbers["domain"]}), encoding="utf-8")
    assert coverage_ratchet.main(["check", *args]) == 1


def test_the_committed_baseline_names_every_layer_and_keeps_domain_whole() -> None:
    baseline = json.loads(coverage_ratchet.BASELINE.read_text(encoding="utf-8"))
    assert sorted(baseline) == sorted(coverage_ratchet.LAYERS)
    assert all(sorted(values) == ["branch", "line"] for values in baseline.values())
    assert baseline["domain"] == {"line": 100.0, "branch": 100.0}


def test_the_sweep_drops_what_it_chose_and_reports_a_drop_that_failed() -> None:
    old = scratch_schema_name("R1", "MAIN", NOW - timedelta(hours=9))
    older = scratch_schema_name("R2", "MAIN", NOW - timedelta(hours=8))
    shown = QueryResult(
        ("created_on", "name", "comment"),
        (("x", old, SCRATCH_MARKER), ("x", older, SCRATCH_MARKER), ("x", "ANALYTICS", ""), ("x", "SST_IT_OTHER", None)),
    )
    port = FakeSnowflake(
        execute_results=(ExecResult(True), ExecResult(False, error=ExecutionError("no"))), query_results=(shown,)
    )
    dropped, failed = sweep_scratch.sweep(port, ACCOUNT, NOW, older_than=timedelta(hours=6), run=None, dry_run=False)
    assert (dropped, failed) == ([old], [older])
    assert port.queries[0][0] == "SHOW SCHEMAS LIKE 'SST_IT_%' IN DATABASE SCRATCH_DB"
    assert port.scripts == [
        (f"DROP SCHEMA IF EXISTS SCRATCH_DB.{old} CASCADE",),
        (f"DROP SCHEMA IF EXISTS SCRATCH_DB.{older} CASCADE",),
    ]
    dry = FakeSnowflake(query_results=(shown,))
    assert sweep_scratch.sweep(dry, ACCOUNT, NOW, older_than=timedelta(hours=6), run="R2", dry_run=True) == (
        [older],
        [],
    )
    assert dry.scripts == []


def test_the_tools_refuse_to_run_without_an_account(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SST_TEST_SNOWFLAKE_ACCOUNT", raising=False)
    assert sweep_scratch.main(["--older-than-hours", "6"]) == 1
    assert session_recorder.main(["--out", str(tmp_path / "capture.json")]) == 1


def test_a_recording_names_no_account_and_no_run_and_compares_byte_for_byte(tmp_path: Path) -> None:
    schema = scratch_scope(ACCOUNT, session_recorder.RECORDING_SCHEMA)
    view = live_view(schema)
    marker = OwnershipMarker("a" * 64, "b" * 64)
    port = FakeSnowflake(
        objects={
            ("SEMANTIC VIEW", schema.sql): (
                ShowRow(view.name.value, "SCRATCH_DB", schema.schema.value, "CI_ROLE", "2026-01-01", marker.text),
            )
        },
        markers={view.sql: marker},
        existing=(orders_table(schema).sql,),
    )
    captured = session_recorder.normalise(session_recorder.observe(port, schema), schema)
    for leak in ("SCRATCH_DB", session_recorder.RECORDING_SCHEMA, "CI_ROLE", "2026-01-01", "a" * 64):
        assert leak not in captured
    assert json.loads(captured)["existing"] == ["<DATABASE>.<SCHEMA>.ORDERS"]

    recording = tmp_path / "recording.json"
    assert session_recorder.compare(captured, recording) == (
        False,
        f"no recording at {recording}: review the capture and commit it\n",
    )
    recording.write_text(captured, encoding="utf-8")
    assert session_recorder.compare(captured, recording) == (True, "")
    same, diff = session_recorder.compare(captured.replace("<volatile>", "moved", 1), recording)
    assert not same and diff.startswith("--- committed\n+++ captured\n")
