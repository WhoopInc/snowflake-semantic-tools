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
import os
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.render.terminal import Terminal
from tests.helpers.cli_projects import common, compile_project
from tests.helpers.reference_project import project_copy
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
    """

    name: str
    prepare: Callable[[Path], Path]
    args: tuple[str, ...]
    variants: tuple[str, ...] = tuple(VARIANTS)
    snowflake: bool = False


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


_PROJECT = ("--project-dir", "<project>")
_OFFLINE = (*_PROJECT, "--manifest", "<manifest>")
CASES = (
    Case("validate-errors", _broken, (*_OFFLINE, "validate", "--no-snowflake-syntax-check")),
    Case("validate-clean", _clean, (*_OFFLINE, "validate", "--no-snowflake-syntax-check")),
    Case("list", _compiled, (*_OFFLINE, "list")),
    Case("usage-error", _clean, ("validate", "--bogus")),
    Case("compile-help", _clean, ("compile", "--help"), variants=("plain",)),
    Case("plan-changes", _compiled, (*_OFFLINE, "plan", "--target", "dev", "--no-plan-out"), snowflake=True),
    Case("agent-validate-errors", _broken, ("-o", "agent", *_OFFLINE, "validate"), variants=("plain",)),
)


def render_case(case: Case, variant: str, root: Path) -> str:
    """Run `case` on the `variant` terminal in a project under `root`; return the normalised transcript."""
    project = case.prepare(root)
    manifest = common(project)[3]
    args = [project.as_posix() if arg == "<project>" else manifest if arg == "<manifest>" else arg for arg in case.args]
    with pytest.MonkeyPatch.context() as patch:
        terminal = VARIANTS[variant]
        patch.setattr("snowflake_semantic_tools.cli.output.system_terminal", lambda **_: terminal)
        patch.setenv("COLUMNS", str(terminal.width))
        if case.snowflake:
            port = FakeSnowflake(state={})
            port.close = lambda: None  # type: ignore[attr-defined]
            patch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
        result = CliRunner().invoke(cli, args, color=terminal.color, prog_name="sst")
    head = f"$ sst {' '.join(case.args)}\n--- exit {result.exit_code}\n"
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
