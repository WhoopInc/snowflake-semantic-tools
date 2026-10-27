"""The coverage ratchet: each layer's line and branch coverage may rise, never fall.

`python -m tests.helpers.coverage_ratchet check --coverage-json coverage.json` reads the report
`coverage json` wrote for a whole-suite run, measures each layer (`domain`, `app`, `cli`,
`adapters`), and compares it with `tests/coverage_baseline.json`. A measured percentage below its
baseline fails, however respectable the number. Percentages are floored to one decimal place on
both sides, so churn of a line or two cannot trip it.

`raise` rewrites the baseline as `max(baseline, measured)`. It never lowers a number: lowering
one is an edit to the baseline file in the pull request that needs it, where a reviewer reads it
as a line, not a flag anyone can pass.

Exit status: 0 when no layer fell; 1 when one did, or a layer is missing from either side.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.helpers.reference_project import REPO_ROOT

BASELINE = REPO_ROOT / "tests" / "coverage_baseline.json"
PACKAGE = "snowflake_semantic_tools"
LAYERS = ("domain", "app", "cli", "adapters")
METRICS = ("line", "branch")


@dataclass(frozen=True, slots=True)
class Finding:
    """One layer and metric compared with its baseline."""

    layer: str
    metric: str
    baseline: float
    measured: float

    @property
    def fell(self) -> bool:
        """Whether the measured percentage is below the baseline."""
        return self.measured < self.baseline


def floored(percent: float) -> float:
    """`percent` floored to one decimal place."""
    return math.floor(percent * 10 + 1e-9) / 10


def measure(report: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    """Each layer's floored line and branch percentage from a `coverage json` report.

    A layer with no statements or no branches counts as 100 for that metric.

    Raises:
        ValueError: the report measured no file of a layer, so the run did not cover it at all.
    """
    totals = {layer: [0, 0, 0, 0] for layer in LAYERS}
    for path, data in report["files"].items():
        parts = Path(path).as_posix().split("/")
        if PACKAGE not in parts:
            continue
        layer = parts[parts.index(PACKAGE) + 1] if parts.index(PACKAGE) + 1 < len(parts) else ""
        if layer not in totals:
            continue
        summary = data["summary"]
        counts = totals[layer]
        counts[0] += summary["covered_lines"]
        counts[1] += summary["num_statements"]
        counts[2] += summary.get("covered_branches", 0)
        counts[3] += summary.get("num_branches", 0)
    missing = [layer for layer, counts in totals.items() if counts[1] == 0]
    if missing:
        raise ValueError(f"the coverage report measured no file of: {', '.join(missing)}")
    return {
        layer: {
            "line": floored(100 * covered / statements),
            "branch": floored(100 * branches_covered / branches) if branches else 100.0,
        }
        for layer, (covered, statements, branches_covered, branches) in totals.items()
    }


def compare(baseline: Mapping[str, Mapping[str, float]], measured: Mapping[str, Mapping[str, float]]) -> list[Finding]:
    """Every layer and metric, measured against its baseline, in layer order.

    Raises:
        ValueError: a layer is missing from the baseline.
    """
    missing = [layer for layer in LAYERS if layer not in baseline]
    if missing:
        raise ValueError(f"the baseline has no entry for: {', '.join(missing)}")
    return [
        Finding(layer, metric, float(baseline[layer][metric]), measured[layer][metric])
        for layer in LAYERS
        for metric in METRICS
    ]


def raised(baseline: Mapping[str, Mapping[str, float]], measured: Mapping[str, Mapping[str, float]]) -> dict[str, Any]:
    """The baseline with each number replaced by `max(baseline, measured)`."""
    return {
        layer: {
            metric: max(float(baseline.get(layer, {}).get(metric, 0.0)), measured[layer][metric]) for metric in METRICS
        }
        for layer in LAYERS
    }


def report_lines(findings: Sequence[Finding]) -> list[str]:
    """A table of the comparison, one row per layer and metric."""
    lines = [f"{'layer':<10} {'metric':<7} {'baseline':>9} {'measured':>9}"]
    for item in findings:
        verdict = "FELL" if item.fell else ("rose" if item.measured > item.baseline else "")
        lines.append(
            f"{item.layer:<10} {item.metric:<7} {item.baseline:>9.1f} {item.measured:>9.1f}  {verdict}".rstrip()
        )
    return lines


def main(argv: Sequence[str] | None = None) -> int:
    """Check the measured coverage against the baseline, or raise the baseline to it."""
    parser = argparse.ArgumentParser(prog="python -m tests.helpers.coverage_ratchet", description=__doc__)
    parser.add_argument("mode", choices=("check", "raise"))
    parser.add_argument("--coverage-json", type=Path, required=True, help="The report `coverage json` wrote.")
    parser.add_argument("--baseline", type=Path, default=BASELINE, help="The committed baseline file.")
    options = parser.parse_args(argv)
    try:
        measured = measure(json.loads(options.coverage_json.read_text(encoding="utf-8")))
        baseline = json.loads(options.baseline.read_text(encoding="utf-8"))
        findings = compare(baseline, measured)
    except (OSError, ValueError, KeyError) as error:
        print(f"coverage_ratchet: {error}", file=sys.stderr)
        return 1
    print("\n".join(report_lines(findings)))
    if options.mode == "raise":
        options.baseline.write_text(json.dumps(raised(baseline, measured), indent=2) + "\n", encoding="utf-8")
        return 0
    fell = [f"{item.layer} {item.metric}" for item in findings if item.fell]
    if fell:
        print(f"coverage fell below the baseline: {', '.join(fell)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
