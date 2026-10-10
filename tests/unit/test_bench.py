"""The benchmark script: reading envelopes, summarizing samples, and the tables it prints."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import bench


def _envelope(*, wall_s: float = 2.0, load: float = 0.5, busy_s: float = 1.0, queries: int = 4) -> dict[str, Any]:
    return {
        "tool": "sst",
        "sst_version": "1.0.0",
        "status": "ok",
        "invocation": {
            "duration_s": wall_s,
            "timings": {
                "wall_s": wall_s,
                "phases": {"load": load, "observe": 0.75},
                "snowflake": {
                    "queries": queries,
                    "client_s": 1.5,
                    "max_s": 0.5,
                    "busy_s": busy_s,
                    "connects": 1,
                    "connect_s": 0.25,
                },
                "dbt_runs": 1,
                "dbt_s": 0.4,
            },
        },
    }


def _sample(wall_s: float, **envelope: Any) -> dict[str, Any]:
    return {
        "ok": True,
        "exit_code": 0,
        "wall_s": wall_s,
        "metrics": bench.sample_metrics(wall_s, _envelope(**envelope)),
    }


def _results(*walls: float, failed: int = 0, **envelope: Any) -> dict[str, Any]:
    samples = [_sample(wall, **envelope) for wall in walls]
    samples += [{"ok": False, "exit_code": 4, "error": "exit 4", "wall_s": 9.0}] * failed
    return {
        "results_version": bench.RESULTS_VERSION,
        "commands": {"plan": {"args": ["plan"], "samples": samples, "summary": bench.summarize(samples)}},
    }


def test_the_envelope_is_the_last_line_that_holds_one() -> None:
    printed = "a dbt warning\n" + json.dumps(_envelope()) + "\n{not json\n" + json.dumps({"tool": "dbt"}) + "\n"
    assert bench.parse_envelope(printed) == _envelope()
    assert bench.parse_envelope("no envelope here\n") is None


def test_a_sample_splits_the_wall_time_into_startup_snowflake_and_the_rest() -> None:
    metrics = bench.sample_metrics(3.0, _envelope())
    # The second the envelope cannot see, plus the load phase.
    assert metrics["startup_s"] == pytest.approx(1.5)
    assert metrics["busy_s"] == 1.0 and metrics["queries"] == 4 and metrics["connects"] == 1
    assert metrics["other_s"] == pytest.approx(0.5)
    assert metrics["phase.observe"] == 0.75
    assert bench.sample_metrics(1.0, {})["queries"] == 0


def test_the_summary_is_over_the_successful_samples_only() -> None:
    summary = _results(1.0, 3.0, 2.0, failed=1)["commands"]["plan"]["summary"]
    assert summary["wall_s"] == {"median": 2.0, "min": 1.0, "max": 3.0}
    assert bench.summarize([{"ok": False, "wall_s": 1.0}]) == {}


def test_the_table_has_a_row_per_command_and_the_median_of_each_phase() -> None:
    rendered = bench.table(_results(1.0, 3.0, 2.0, failed=1))
    lines = rendered.splitlines()
    assert lines[0].split() == [
        "command",
        "ok",
        "wall",
        "min",
        "max",
        *(m.removesuffix("_s") for m in bench.METRICS[1:]),
    ]
    assert lines[1].split()[:5] == ["plan", "3/4", "2.00", "1.00", "3.00"]
    assert lines[2] == ""
    assert lines[3].split() == ["phases", "(median", "s)", "load", "observe"]
    assert lines[4].split() == ["plan", "0.50", "0.75"]


def test_a_command_with_no_successful_run_shows_dashes() -> None:
    rendered = bench.table(_results(failed=2))
    assert rendered.splitlines()[1].split()[:3] == ["plan", "0/2", "-"]


def test_compare_shows_each_median_its_change_and_the_percentage() -> None:
    old, new = _results(2.0, queries=10), _results(1.0, queries=5)
    new["commands"]["smoke"] = new["commands"]["plan"]
    rows = {tuple(line.split()[:2]): line.split()[2:] for line in bench.compare(old, new).splitlines()[1:]}
    assert rows[("plan", "wall_s")] == ["2.00", "1.00", "-1.00", "-50%"]
    assert rows[("plan", "queries")] == ["10", "5", "-5", "-50%"]
    assert rows[("plan", "phase.observe")] == ["0.75", "0.75", "+0.00", "+0%"]
    # A command only one results file holds is not compared.
    assert not any(command == "smoke" for command, _ in rows)


def test_merged_results_replace_a_command_of_the_same_name() -> None:
    old, new = _results(2.0), _results(1.0)
    old["commands"]["compile"] = old["commands"]["plan"]
    combined = bench.merged(old, new)
    assert set(combined["commands"]) == {"compile", "plan"}
    assert combined["commands"]["plan"] is new["commands"]["plan"]


def _fake_sst(tmp_path: Path, exit_code: int, printed: str) -> list[str]:
    script = tmp_path / "fake_sst.py"
    script.write_text(f"import sys\nsys.stdout.write({printed!r})\nsys.exit({exit_code})\n", encoding="utf-8")
    return [sys.executable, str(script)]


def test_a_run_records_its_exit_and_metrics_and_an_exit_it_does_not_accept_as_failed(tmp_path: Path) -> None:
    printed = json.dumps(_envelope()) + "\n"
    planned = bench.run_once(_fake_sst(tmp_path, 2, printed), tmp_path, "verify", "plan")
    assert planned["ok"] and planned["exit_code"] == 2 and planned["metrics"]["queries"] == 4
    refused = bench.run_once(_fake_sst(tmp_path, 4, printed), tmp_path, None, "compile")
    assert not refused["ok"] and refused["status"] == "ok" and not refused["envelope_on_stderr"]


def test_an_envelope_printed_on_stderr_fails_the_run_as_a_broken_contract(tmp_path: Path) -> None:
    script = tmp_path / "stderr_sst.py"
    script.write_text(f"import sys\nsys.stderr.write({json.dumps(_envelope()) + chr(10)!r})\n", encoding="utf-8")
    sample = bench.run_once([sys.executable, str(script)], tmp_path, None, "compile")
    assert not sample["ok"] and sample["envelope_on_stderr"] and sample["metrics"]["queries"] == 4
    assert sample["error"].startswith("CONTRACT:")
    results = {"commands": {"compile": {"samples": [sample], "summary": bench.summarize([sample])}}}
    assert "CONTRACT BROKEN: compile printed its JSON envelope on stderr in 1 of 1 runs" in bench.table(results)


def test_main_exits_one_when_an_envelope_reaches_stderr(tmp_path: Path) -> None:
    script = tmp_path / "stderr_sst.py"
    script.write_text(f"import sys\nsys.stderr.write({json.dumps(_envelope()) + chr(10)!r})\n", encoding="utf-8")
    args = [
        "--project-dir",
        str(tmp_path),
        "--sst",
        f"{sys.executable} {script}",
        "--runs",
        "1",
        "--commands",
        "compile",
    ]
    assert bench.main(args) == 1


def test_a_run_with_no_envelope_or_no_executable_is_a_failure(tmp_path: Path) -> None:
    silent = bench.run_once(_fake_sst(tmp_path, 1, "boom\n"), tmp_path, None, "compile")
    assert not silent["ok"] and "no JSON envelope" in silent["error"] and "boom" in silent["error"]
    missing = bench.run_once([str(tmp_path / "no-such-sst")], tmp_path, None, "compile")
    assert not missing["ok"] and "could not start" in missing["error"]


def test_main_runs_writes_appends_and_prints(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sst = " ".join(_fake_sst(tmp_path, 0, json.dumps(_envelope()) + "\n"))
    out = tmp_path / "results.json"
    common = ["--project-dir", str(tmp_path), "--sst", sst, "--runs", "2", "--out", str(out)]
    assert bench.main([*common, "--commands", "compile"]) == 0
    assert bench.main([*common, "--commands", "validate-offline", "--append"]) == 0
    written = json.loads(out.read_text(encoding="utf-8"))
    assert set(written["commands"]) == {"compile", "validate-offline"}
    assert len(written["commands"]["compile"]["samples"]) == 2
    capsys.readouterr()
    assert bench.main(["--table", str(out)]) == 0
    assert capsys.readouterr().out.startswith("command")
    assert bench.main(["--compare", str(out), str(out)]) == 0
    assert "+0%" in capsys.readouterr().out


def test_main_reports_a_failed_run_and_refuses_bad_arguments(tmp_path: Path) -> None:
    sst = " ".join(_fake_sst(tmp_path, 4, "refused\n"))
    assert bench.main(["--project-dir", str(tmp_path), "--sst", sst, "--runs", "1", "--commands", "compile"]) == 1
    for argv in ([], ["--project-dir", ".", "--commands", "nope"], ["--project-dir", ".", "--runs", "0"]):
        with pytest.raises(SystemExit):
            bench.main(argv)
    stray = tmp_path / "stray.json"
    stray.write_text("{}", encoding="utf-8")
    with pytest.raises(SystemExit, match="not a benchmark results file"):
        bench.main(["--table", str(stray)])
