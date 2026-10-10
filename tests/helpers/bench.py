"""The engine benchmark: run `sst` commands against a project and report where their time goes.

Each command runs as its own process, `--runs` times, with `--output json`; the envelope's
`invocation.timings` says how long the run took and how much of it was loading the project
(dbt included), Snowflake, and each phase the engine names. The wall time is measured here, so
it includes the interpreter's start and imports, which the envelope cannot see.

    python tests/helpers/bench.py --project-dir ../my-project --target verify --runs 3 --out before.json
    python tests/helpers/bench.py --compare before.json after.json
    python tests/helpers/bench.py --table before.json

`--append` adds to an existing results file, so a long benchmark can run one command at a time.
Only the standard library is imported, so the file runs by path from any directory, with whichever
`sst` is on `PATH` (or `--sst`). Each command runs with no stdin and a time limit; one that fails
or times out is recorded as such and the benchmark goes on.

The connected commands write to the target: `apply-noop` runs `sst apply --yes`, which publishes
whatever the project's plan holds, so benchmark a scratch target whose plan is already applied.

Exit status: 0 when every run succeeded (or `--compare`/`--table` printed); 1 when a run failed.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RESULTS_VERSION = 1


@dataclass(frozen=True, slots=True)
class BenchCommand:
    """One benchmarked command: the `sst` arguments, the exits that count as success, its time limit."""

    args: tuple[str, ...]
    ok_exits: frozenset[int]
    timeout_s: int


# The time limits are the overnight brief's: offline 300 s, connected reads 900, apply 1200, smoke 600.
COMMANDS: Mapping[str, BenchCommand] = {
    "compile": BenchCommand(("compile",), frozenset({0}), 300),
    "validate-offline": BenchCommand(("validate", "--no-snowflake-syntax-check"), frozenset({0}), 300),
    "validate": BenchCommand(("validate", "--snowflake-syntax-check"), frozenset({0}), 900),
    "plan": BenchCommand(("plan",), frozenset({0, 2}), 900),
    "apply-noop": BenchCommand(("apply", "--yes"), frozenset({0}), 1200),
    "smoke": BenchCommand(("test", "--suite", "smoke"), frozenset({0}), 600),
}
DEFAULT_COMMANDS = tuple(COMMANDS)

# The columns of the table, each a sample metric; medians unless marked.
METRICS = (
    "wall_s",
    "startup_s",
    "dbt_s",
    "queries",
    "client_s",
    "busy_s",
    "connects",
    "connect_s",
    "other_s",
)


def sample_metrics(wall_s: float, envelope: Mapping[str, Any]) -> dict[str, float]:
    """Return one run's metrics from its measured wall time and its envelope.

    `startup_s` is the interpreter's start and imports (the wall time the envelope does not
    see) plus the `load` phase, which reads the configuration and compiles the project, dbt
    included. `other_s` is what remains once startup and Snowflake's wall-clock time
    (`busy_s`: any statement or login in flight) are taken away; `busy_s` rather than the summed
    `client_s`, which counts statements on parallel sessions twice.
    """
    timings = envelope.get("invocation", {}).get("timings", {})
    snowflake = timings.get("snowflake", {})
    phases = timings.get("phases", {})
    run_s = float(timings.get("wall_s", 0.0))
    startup_s = max(wall_s - run_s, 0.0) + float(phases.get("load", 0.0))
    busy_s = float(snowflake.get("busy_s", 0.0))
    metrics = {
        "wall_s": wall_s,
        "startup_s": startup_s,
        "dbt_s": float(timings.get("dbt_s", 0.0)),
        "queries": float(snowflake.get("queries", 0)),
        "client_s": float(snowflake.get("client_s", 0.0)),
        "max_query_s": float(snowflake.get("max_s", 0.0)),
        "busy_s": busy_s,
        "connects": float(snowflake.get("connects", 0)),
        "connect_s": float(snowflake.get("connect_s", 0.0)),
        "other_s": wall_s - startup_s - busy_s,
    }
    metrics.update({f"phase.{name}": float(seconds) for name, seconds in phases.items()})
    return metrics


def run_once(sst: Sequence[str], project_dir: Path, target: str | None, name: str) -> dict[str, Any]:
    """Run one benchmarked command once and return its sample: exit, status, metrics, or an error."""
    command = COMMANDS[name]
    argv = [*sst, *command.args, "--project-dir", str(project_dir), "--output", "json"]
    if target:
        argv += ["--target", target]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            argv,
            cwd=project_dir,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=command.timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"timed out after {command.timeout_s}s", "wall_s": time.perf_counter() - started}
    except OSError as exc:
        return {"ok": False, "error": f"could not start {argv[0]}: {exc}", "wall_s": 0.0}
    wall_s = time.perf_counter() - started
    envelope = parse_envelope(completed.stdout)
    # An envelope belongs on stdout; one found on stderr still has its timings, and is flagged.
    misplaced = envelope is None and (envelope := parse_envelope(completed.stderr)) is not None
    if envelope is None:
        tail = (completed.stderr or completed.stdout).strip().splitlines()[-3:]
        error = f"exit {completed.returncode} with no JSON envelope: " + " | ".join(tail)
        return {"ok": False, "exit_code": completed.returncode, "error": error, "wall_s": wall_s}
    return {
        "ok": completed.returncode in command.ok_exits,
        "exit_code": completed.returncode,
        "status": envelope.get("status"),
        "sst_version": envelope.get("sst_version"),
        "envelope_on_stderr": misplaced,
        "wall_s": wall_s,
        "metrics": sample_metrics(wall_s, envelope),
    }


def parse_envelope(stdout: str) -> dict[str, Any] | None:
    """Return the one JSON envelope `sst` printed, from its last line that holds one; None when none does."""
    for line in reversed(stdout.strip().splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("tool") == "sst":
            return value
    return None


def summarize(samples: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    """Return the median, minimum, and maximum of every metric over the successful samples."""
    values: dict[str, list[float]] = {}
    for sample in samples:
        if sample.get("ok"):
            for key, value in sample["metrics"].items():
                values.setdefault(key, []).append(float(value))
    return {
        key: {"median": statistics.median(found), "min": min(found), "max": max(found)}
        for key, found in sorted(values.items())
    }


def benchmark(
    sst: Sequence[str], project_dir: Path, target: str | None, commands: Sequence[str], runs: int
) -> dict[str, Any]:
    """Run each command `runs` times, in order, and return the results document."""
    results: dict[str, Any] = {}
    for name in commands:
        samples = []
        for index in range(runs):
            sample = run_once(sst, project_dir, target, name)
            samples.append(sample)
            shown = f"{sample['wall_s']:.1f}s" + ("" if sample["ok"] else f" FAILED: {sample.get('error', 'exit')}")
            print(f"{name} run {index + 1}/{runs}: {shown}", file=sys.stderr)
        results[name] = {"args": list(COMMANDS[name].args), "samples": samples, "summary": summarize(samples)}
    return {
        "results_version": RESULTS_VERSION,
        "project_dir": str(project_dir),
        "target": target,
        "runs": runs,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "commands": results,
    }


def merged(existing: Mapping[str, Any], new: Mapping[str, Any]) -> dict[str, Any]:
    """Return `existing` with `new`'s commands added, each replacing one of the same name."""
    return {**existing, "commands": {**existing.get("commands", {}), **new["commands"]}}


def _median(summary: Mapping[str, Mapping[str, float]], key: str) -> float | None:
    found = summary.get(key)
    return found["median"] if found else None


def _cell(value: float | None, key: str) -> str:
    if value is None:
        return "-"
    return f"{value:.0f}" if key in ("queries", "connects") else f"{value:.2f}"


def table(results: Mapping[str, Any]) -> str:
    """Render the results as text: a row per command, then the median seconds of each phase.

    The first table shows the wall time's median, minimum, and maximum, and the median of each
    other metric, over the successful runs; `ok` counts them.
    """
    header = ["command", "ok", "wall", "min", "max", *(key.removesuffix("_s") for key in METRICS[1:])]
    rows = [header]
    phase_names: set[str] = set()
    for name, entry in results.get("commands", {}).items():
        summary = entry["summary"]
        wall = summary.get("wall_s", {})
        ok = sum(bool(sample.get("ok")) for sample in entry["samples"])
        rows.append(
            [
                name,
                f"{ok}/{len(entry['samples'])}",
                _cell(wall.get("median"), "wall_s"),
                _cell(wall.get("min"), "wall_s"),
                _cell(wall.get("max"), "wall_s"),
                *(_cell(_median(summary, key), key) for key in METRICS[1:]),
            ]
        )
        phase_names.update(key.removeprefix("phase.") for key in summary if key.startswith("phase."))
    text = _aligned(rows)
    if phase_names:
        ordered = sorted(phase_names)
        phase_rows = [["phases (median s)", *ordered]]
        for name, entry in results.get("commands", {}).items():
            phase_rows.append([name, *(_cell(_median(entry["summary"], f"phase.{phase}"), "") for phase in ordered)])
        text += "\n" + _aligned(phase_rows)
    return text


def _aligned(rows: Sequence[Sequence[str]]) -> str:
    """Render rows as columns, the first left-aligned and the rest right-aligned."""
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    lines = []
    for row in rows:
        cells = [row[0].ljust(widths[0]), *(cell.rjust(width) for cell, width in zip(row[1:], widths[1:], strict=True))]
        lines.append("  ".join(cells).rstrip())
    return "\n".join(lines) + "\n"


def compare(old: Mapping[str, Any], new: Mapping[str, Any]) -> str:
    """Render each metric's median in both results, its change, and the change as a percentage.

    Only the commands both results hold are compared; a metric either lacks shows `-`.
    """
    rows = [["command", "metric", "old", "new", "delta", "%"]]
    for name, entry in new.get("commands", {}).items():
        before = old.get("commands", {}).get(name)
        if before is None:
            continue
        keys = [key for key in (*METRICS, "max_query_s") if key in entry["summary"] or key in before["summary"]]
        keys += sorted({key for key in (*entry["summary"], *before["summary"]) if key.startswith("phase.")})
        for key in keys:
            was, now = _median(before["summary"], key), _median(entry["summary"], key)
            delta = now - was if was is not None and now is not None else None
            percent = f"{100 * delta / was:+.0f}%" if delta is not None and was else "-"
            signed = "-" if delta is None else ("+" if delta >= 0 else "") + _cell(delta, key)
            rows.append([name, key, _cell(was, key), _cell(now, key), signed, percent])
    return _aligned(rows)


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("results_version") != RESULTS_VERSION:
        raise SystemExit(f"{path} is not a benchmark results file (results_version {RESULTS_VERSION})")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark, or print or compare results files, as the arguments say."""
    parser = argparse.ArgumentParser(prog="tests/helpers/bench.py", description=__doc__.split("\n\n")[0])
    parser.add_argument("--project-dir", type=Path, help="The project to benchmark.")
    parser.add_argument("--target", help="The profile target every command runs against.")
    parser.add_argument("--runs", type=int, default=3, help="How many times each command runs (default 3).")
    parser.add_argument(
        "--commands",
        default=",".join(DEFAULT_COMMANDS),
        help=f"Comma-separated, in the order run (default: {','.join(DEFAULT_COMMANDS)}).",
    )
    parser.add_argument("--sst", default="sst", help="The sst executable, or a command line to split (default: sst).")
    parser.add_argument("--out", type=Path, help="Write the results here as JSON.")
    parser.add_argument("--append", action="store_true", help="Add to the results already in --out.")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("OLD", "NEW"), help="Compare two results files.")
    parser.add_argument("--table", type=Path, metavar="RESULTS", help="Print a results file as a table.")
    options = parser.parse_args(argv)
    if options.compare:
        print(compare(_read(options.compare[0]), _read(options.compare[1])), end="")
        return 0
    if options.table:
        print(table(_read(options.table)), end="")
        return 0
    if options.project_dir is None:
        parser.error("--project-dir is required to run the benchmark")
    names = [name.strip() for name in options.commands.split(",") if name.strip()]
    unknown = [name for name in names if name not in COMMANDS]
    if unknown or options.runs < 1:
        parser.error(
            f"unknown command(s) {unknown}; choose from {list(COMMANDS)}" if unknown else "--runs must be >= 1"
        )
    results = benchmark(options.sst.split(), options.project_dir.resolve(), options.target, names, options.runs)
    if options.out is not None:
        if options.append and options.out.exists():
            results = merged(_read(options.out), results)
        options.out.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(table(results), end="")
    failed = any(not sample.get("ok") for entry in results["commands"].values() for sample in entry["samples"])
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
