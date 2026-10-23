"""The CLI's own goldens: what `sst` prints for a few runs of the reference project, per terminal.

Each case is one command line, run in-process on a fresh copy of the reference project, and
each variant one terminal: colour on, plain, ASCII only, and 80 columns. The text is
normalised before it is compared or written -- the project's temporary path becomes
`<project>`, a run's seconds `<n>s` -- so a golden changes only when what `sst` prints does.
`tests/unit/cli/test_cli_goldens.py` compares them; `python -m tests.helpers.cli_goldens`
rewrites them (refused in CI, as `update_goldens` is).
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions
from snowflake_semantic_tools.app.evals.run import EvalRunResult, EvalSuiteResult
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.render.terminal import Terminal
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.tool_selection import expected_tools
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalGateVerdict,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolUse
from tests.helpers.cli_projects import applied_port, common, compile_project
from tests.helpers.compare_reports import JsonReportWriter, jaffle_outcome
from tests.helpers.described_objects import described_view
from tests.helpers.published_target import published
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

GOLDEN_DIR = Path(__file__).resolve().parents[1] / "golden" / "expected" / "cli"
VARIANTS: dict[str, Terminal] = {
    "color": Terminal(color=True, color_out=True, width=100),
    "plain": Terminal(width=100),
    "ascii": Terminal(unicode=False, width=100),
    "narrow": Terminal(width=80),
}
_NOT_CI = frozenset({"", "0", "false", "no"})


@dataclasses.dataclass(frozen=True)
class Case:
    """One command line the goldens pin, and the project it runs on.

    Attributes:
        prepare: Makes the project the case runs in, under the directory it is given.
        args: The command line after `sst`; `<project>` stands for the project's directory.
        variants: The terminals it is pinned on; one, `plain`, when the others would not differ.
        snowflake: Runs with a fake Snowflake that holds nothing, so a plan creates everything.
        connect: Makes the fake Snowflake accounts the case runs against, once the project is
            prepared, and returns what each connection opens; None for `snowflake`'s empty one.
        data_only: Pin the `--output json` envelope's `data` alone, indented, which holds no time.
    """

    name: str
    prepare: Callable[[Path], Path]
    args: tuple[str, ...]
    variants: tuple[str, ...] = tuple(VARIANTS)
    snowflake: bool = False
    connect: Callable[[Path, pytest.MonkeyPatch], Callable[[dict[str, object]], FakeSnowflake]] | None = None
    data_only: bool = False


def _clean(root: Path) -> Path:
    return project_copy(root)


def _broken(root: Path) -> Path:
    project = project_copy(root)
    metric = project / "semantic_models" / "metrics" / "total_supply_cost.yml"
    metric.write_text(metric.read_text().replace("'supply_cost'", "'supply_costs'"))
    return project


def _compiled(root: Path) -> Path:
    project = project_copy(root)
    compile_project(project)
    return project


_METRIC = Path("semantic_models") / "metrics" / "line_item_count.yml"


def _edited(project: Path) -> None:
    """Count distinct order items in `line_item_count`, and compile the edit."""
    path = project / _METRIC
    path.write_text(path.read_text(encoding="utf-8").replace("COUNT({{", "COUNT(DISTINCT {{", 1), encoding="utf-8")
    compile_project(project)


def _one_update(project: Path, patch: pytest.MonkeyPatch) -> Callable[[dict[str, object]], FakeSnowflake]:
    """dev, applied and described as published, then one metric edited: a plan updates one view."""
    port = published(project, patch)
    _edited(project)
    return lambda params: port


def _two_targets(project: Path, patch: pytest.MonkeyPatch) -> Callable[[dict[str, object]], FakeSnowflake]:
    """dev and prod each applied to its own account, prod's view then holding the edited metric."""
    dev = published(project, patch, "dev", agents=True)
    prod = published(project, patch, "prod", agents=True)
    edited = project_copy(project.parent / "edited")
    _edited(edited)
    for item in compile_result(locate_project(edited), "prod", DBT_MANIFEST).compiled:
        if isinstance(item, CompiledView):
            prod.view_descriptions[item.rendered_artifact.target.sql] = described_view(item.view)
    by_database = {"SST_REF_DEV": dev, "SST_REF_PROD": prod}
    return lambda params: by_database[str(params["database"]).upper()]


def _unpublished_miss(project: Path, patch: pytest.MonkeyPatch) -> Callable[[dict[str, object]], FakeSnowflake]:
    """Every question answered with the tools it expects, but the first that expects one calls it twice."""
    # One question at a time, and one scratch suffix, so every run prints the same lines.
    patch.setattr("snowflake_semantic_tools.app.evals.inline.suite_concurrency", lambda *args: 1)
    patch.setattr("snowflake_semantic_tools.cli.commands.eval.secrets.token_hex", lambda size: "5f680229")
    port = FakeSnowflake(state={})
    port.judge_answer = {"score": 1, "reason": "The answer says what the expected answer says."}
    [item] = [
        entry
        for entry in compile_result(locate_project(project), "dev", DBT_MANIFEST).compiled
        if isinstance(entry, CompiledEval)
    ]
    replies: dict[str, AgentReply] = {}
    doubled = False
    for index, row in enumerate(json.loads(item.rendered.dataset_payload), start=1):
        tools = [call["tool_name"] for call in row["ground_truth"].get("ground_truth_invocations", [])]
        if tools and not doubled:
            tools, doubled = tools * 2, True
        uses = tuple(ToolUse(name, "") for name in tools)
        replies[row["input_query"]] = AgentReply(f"Answer {index}.", uses, duration_ms=1000 * index, tokens=(900, 40))
    port.agent_replies = replies  # type: ignore[assignment]
    port.close = lambda: None  # type: ignore[attr-defined]
    return lambda params: port


def _smoke_pass(project: Path, patch: pytest.MonkeyPatch) -> Callable[[dict[str, object]], FakeSnowflake]:
    """Every compiled artifact applied as compiled, so each smoke probe runs and answers."""
    port = applied_port(project)
    port.close = lambda: None  # type: ignore[attr-defined]
    return lambda params: port


def _published_pass(project: Path, patch: pytest.MonkeyPatch) -> Callable[[dict[str, object]], FakeSnowflake]:
    """One published attempt in which every question passed, read back with its answers and costs."""
    [item] = [
        entry
        for entry in compile_result(locate_project(project), "dev", DBT_MANIFEST).compiled
        if isinstance(entry, CompiledEval)
    ]
    rows = []
    for index, (key, query, truth) in enumerate(compiled_questions(item), start=1):
        expected = expected_tools(truth)
        metrics = (
            EvalMetricResult(key, "answer_correctness", 1.0, True, "Matches the expected answer."),
            EvalMetricResult(key, "tool_selection_accuracy", 1.0, True, "**Score:** 1.00", ThresholdRange(0.8)),
        )
        cost = EvalCostSummary(2000 + index, 30, 10, 40, 38862, 715, 2)
        rows.append(EvalResultRow(key, query, metrics, index, f"Answer {index}.", None, expected, expected, cost))
    total = EvalCostSummary(8010, 120, 40, 160, 155448, 2860, 8)
    attempt = EvalRunAttempt("EVAL_JAFFLE_ANALYTICS_AGENT_RUN", 1, "COMPLETED", tuple(rows), total)
    suite = EvalSuiteResult((EvalRunResult(item.artifact_key, (attempt,), DiagnosticBag(), True),), DiagnosticBag())
    patch.setattr("snowflake_semantic_tools.app.evals.run.RunEvalSuite.run", lambda *args, **kwargs: suite)
    patch.setattr("snowflake_semantic_tools.app.evals.suite.validate_eval_publication", lambda *args: DiagnosticBag())
    passed = EvalGateVerdict("blocking", (), True)
    patch.setattr(
        "snowflake_semantic_tools.app.evals.suite.evaluate_gate", lambda *args, **kwargs: (passed, DiagnosticBag())
    )
    port = FakeSnowflake(state={})
    port.close = lambda: None  # type: ignore[attr-defined]
    return lambda params: port


def _compared(project: Path, patch: pytest.MonkeyPatch) -> Callable[[dict[str, object]], FakeSnowflake]:
    """A comparison of the reference project's agent eval, its report written as compare.json alone."""
    outcome = jaffle_outcome()
    patch.setattr("snowflake_semantic_tools.app.evals.compare.RunEvalComparison.run", lambda *args, **kwargs: outcome)
    patch.setattr("snowflake_semantic_tools.cli.compare.compare_report_writer", JsonReportWriter)
    port = FakeSnowflake(state={})
    port.close = lambda: None  # type: ignore[attr-defined]
    return lambda params: port


_COMPARED = (
    "eval",
    "--compare-models",
    "claude-opus-4-8,openai-gpt-5,claude-haiku-4-5",
    "--compare-report",
    "<project>/sst_target/compare/run",
    "--target",
    "dev",
)
_PROJECT = ("--project-dir", "<project>")
_OFFLINE = (*_PROJECT, "--manifest", "<manifest>")
CASES = (
    Case("validate-errors", _broken, (*_OFFLINE, "validate", "--no-snowflake-syntax-check")),
    Case("validate-clean", _clean, (*_OFFLINE, "validate", "--no-snowflake-syntax-check")),
    Case("list", _compiled, (*_OFFLINE, "list")),
    Case("lineage", _compiled, (*_OFFLINE, "lineage", "tool:order_status"), variants=("color", "plain", "narrow")),
    Case(
        "lineage-upstream",
        _compiled,
        (*_OFFLINE, "lineage", "metric:gross_margin_rate", "--upstream"),
        variants=("color", "plain"),
    ),
    Case(
        "lineage-wide",
        _compiled,
        (*_OFFLINE, "lineage", "skill:jaffle-semantics", "--downstream", "--wide"),
        variants=("plain", "ascii"),
    ),
    Case("list-graph", _compiled, (*_OFFLINE, "list", "--select", "model:orders+"), variants=("plain",)),
    Case(
        "plan-graph",
        _compiled,
        (*_OFFLINE, "plan", "--target", "dev", "--no-plan-out", "--select", "+agent:jaffle_analytics_agent"),
        variants=("plain",),
        snowflake=True,
    ),
    Case("usage-error", _clean, ("validate", "--bogus")),
    Case("compile-help", _clean, ("compile", "--help"), variants=("plain",)),
    Case("compile-help-full", _clean, ("compile", "--help", "--full"), variants=("plain",)),
    Case("plan-changes", _compiled, (*_OFFLINE, "plan", "--target", "dev", "--no-plan-out"), snowflake=True),
    Case("agent-validate-errors", _broken, ("-o", "agent", *_OFFLINE, "validate"), variants=("plain",)),
    Case("plan-updates", _compiled, (*_OFFLINE, "plan", "--target", "dev", "--no-plan-out"), connect=_one_update),
    Case(
        "diff-targets",
        _compiled,
        (*_OFFLINE, "diff", "--from", "dev", "--to", "prod"),
        variants=("color", "plain"),
        connect=_two_targets,
    ),
    Case(
        "test-smoke",
        _compiled,
        (*_OFFLINE, "test", "--target", "dev"),
        variants=("color", "plain", "narrow"),
        connect=_smoke_pass,
    ),
    Case(
        "eval-unpublished",
        _clean,
        (*_OFFLINE, "eval", "--unpublished", "--target", "dev"),
        connect=_unpublished_miss,
    ),
    Case(
        "eval-unpublished-verbose",
        _clean,
        (*_OFFLINE, "-v", "eval", "--unpublished", "--target", "dev"),
        variants=("color", "plain", "narrow"),
        connect=_unpublished_miss,
    ),
    Case("eval-published", _compiled, (*_OFFLINE, "eval", "--target", "dev"), connect=_published_pass),
    Case("eval-compare", _compiled, (*_OFFLINE, *_COMPARED), connect=_compared),
    Case(
        "eval-compare-json",
        _compiled,
        ("-o", "json", *_OFFLINE, *_COMPARED),
        variants=("plain",),
        connect=_compared,
        data_only=True,
    ),
)


def render_case(case: Case, variant: str, root: Path) -> str:
    """Run `case` on the `variant` terminal in a project under `root`; return the normalised transcript."""
    project = case.prepare(root)
    manifest = common(project)[3]
    args = [manifest if arg == "<manifest>" else arg.replace("<project>", project.as_posix()) for arg in case.args]
    with pytest.MonkeyPatch.context() as patch:
        terminal = VARIANTS[variant]
        patch.setattr("snowflake_semantic_tools.cli.output.system_terminal", lambda **_: terminal)
        patch.setenv("COLUMNS", str(terminal.width))
        if case.snowflake:
            port = FakeSnowflake(state={})
            port.close = lambda: None  # type: ignore[attr-defined]
            patch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
        if case.connect is not None:
            opened = case.connect(project, patch)
            patch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", opened)
        result = CliRunner().invoke(cli, args, color=terminal.color, prog_name="sst")
    head = f"$ sst {' '.join(case.args)}\n--- exit {result.exit_code}\n"
    if case.data_only:
        data = json.dumps(json.loads(result.stdout)["data"], indent=2, ensure_ascii=False)
        return normalised(f"{head}--- data\n{data}\n", project)
    text = f"{head}--- stdout\n{result.stdout}--- stderr\n{result.stderr}"
    return normalised(text, project)


def normalised(text: str, project: Path) -> str:
    """Replace what varies between runs: the project's path, a temporary directory, and seconds."""
    for path in {project.as_posix(), project.resolve().as_posix()}:
        text = text.replace(path, "<project>")
    text = re.sub(r"\b\d+\.\ds\b", "<n>s", text)
    text = re.sub(r"time=\d+\.\ds", "time=<n>s", text)
    return text


def golden_path(case: Case, variant: str) -> Path:
    """Return where the golden of `case` on `variant` is kept."""
    return GOLDEN_DIR / f"{case.name}.{variant}.txt"


def main() -> int:
    """Rewrite every CLI golden that differs; exit 1 in CI."""
    if os.environ.get("CI", "").strip().casefold() not in _NOT_CI:
        print("refusing to rewrite goldens in CI; run this locally and commit the result", file=sys.stderr)
        return 1
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    for case in CASES:
        for variant in case.variants:
            with tempfile.TemporaryDirectory() as root:
                text = render_case(case, variant, Path(root))
            path = golden_path(case, variant)
            if not path.is_file() or path.read_text(encoding="utf-8") != text:
                path.write_text(text, encoding="utf-8")
                written += 1
                print(f"wrote {path.relative_to(GOLDEN_DIR.parents[2])}")
    print(f"{written} CLI golden file(s) written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
