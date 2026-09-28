"""Proof that the ring contracts in `pyproject.toml` actually FAIL when crossed.

WHY THIS FILE EXISTS. `lint-imports` reports "5 kept" against empty ring packages,
which proves nothing at all -- a contract that has never rejected anything is
indistinguishable from a contract that cannot reject anything. 0.3's layering
decayed with an architecture documented in its README the whole time. So each
contract here is shown to bite: the test writes a module that deliberately crosses
one boundary, asserts `lint-imports` reports that specific contract BROKEN, and
removes the module again.

A test asserting the linter passes would be the weaker test. These assert it fails.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PKG = REPO_ROOT / "snowflake_semantic_tools"

# (ring-relative probe path, violating module body, contract name that must break)
CROSSINGS = [
    pytest.param(
        "domain/_boundary_probe.py",
        "import yaml\n",
        "domain is pure",
        id="domain-may-not-import-yaml",
    ),
    pytest.param(
        "domain/_boundary_probe.py",
        "import datetime\n",
        "domain is pure",
        id="domain-may-not-import-a-clock",
    ),
    pytest.param(
        "domain/_boundary_probe.py",
        "import os\n",
        "domain is pure",
        id="domain-may-not-read-the-environment",
    ),
    pytest.param(
        "domain/_boundary_probe.py",
        "from snowflake_semantic_tools import cli\n",
        "Rings are layered",
        id="domain-may-not-import-upward-to-cli",
    ),
    pytest.param(
        "app/_boundary_probe.py",
        "import click\n",
        "app imports domain only",
        id="app-may-not-touch-the-terminal",
    ),
    pytest.param(
        "app/_boundary_probe.py",
        "from snowflake_semantic_tools.adapters import yaml\n",
        "Rings are layered",
        id="app-may-not-construct-an-adapter",
    ),
    pytest.param(
        "adapters/_boundary_probe.py",
        "from snowflake_semantic_tools import app\n",
        "Rings are layered",
        id="adapter-may-not-call-back-into-a-use-case",
    ),
    pytest.param(
        "adapters/_boundary_probe.py",
        "from snowflake_semantic_tools.domain import render\n",
        "adapters use domain ports and models",
        id="adapter-may-not-use-domain-renderers",
    ),
    pytest.param(
        "cli/_boundary_probe.py",
        "from snowflake_semantic_tools import services\n",
        "1.0 rings do not import 0.3 internals",
        id="ring-may-not-lean-on-0.3-internals",
    ),
]


def _lint_imports() -> subprocess.CompletedProcess[str]:
    """Invoke the `lint-imports` console script from the running interpreter's venv.

    NOT `python -m importlinter.cli` -- that package ships no `__main__.py`, so `-m`
    exits 0 with empty output and every crossing test below would pass for the wrong
    reason. `test_contracts_hold_on_the_real_tree` is what caught that.
    """
    script = Path(sys.executable).parent / "lint-imports"
    assert script.exists(), f"lint-imports is not installed beside {sys.executable}"
    return subprocess.run(
        [str(script)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def test_repo_root_resolves() -> None:
    """Guard the `parents[2]` above: a moved file silently breaks every test here.

    Without this, relocating the file would make `cwd` wrong, `lint-imports` would
    find no config, and the crossing tests would pass for the wrong reason again.
    """
    assert (REPO_ROOT / "pyproject.toml").is_file(), f"REPO_ROOT is wrong: {REPO_ROOT}"
    assert PKG.is_dir(), f"package not found under {REPO_ROOT}"


def test_contracts_hold_on_the_real_tree() -> None:
    """Baseline: with nothing crossed, every contract is kept.

    This is the control. Without it a broken linter invocation -- a bad path, a
    missing config -- would make every crossing test below pass for the wrong
    reason, since they only look for failure.
    """
    result = _lint_imports()
    assert result.returncode == 0, f"contracts already broken before probing:\n{result.stdout}"
    assert "Contracts: 5 kept, 0 broken." in result.stdout, result.stdout


@pytest.mark.parametrize(("probe_path", "body", "contract"), CROSSINGS)
def test_crossing_a_boundary_breaks_its_contract(probe_path: str, body: str, contract: str) -> None:
    probe = PKG / probe_path
    assert not probe.exists(), f"probe path is not clean: {probe}"

    probe.write_text(f"# Temporary boundary probe written by {Path(__file__).name}.\n{body}")
    try:
        result = _lint_imports()
    finally:
        probe.unlink()
        # .pyc would keep the violating module visible to a later run.
        for cached in (probe.parent / "__pycache__").glob(f"{probe.stem}.*"):
            cached.unlink()

    assert result.returncode != 0, (
        f"crossing a boundary was NOT caught -- {probe_path} imported "
        f"{body.strip()!r} and the linter still passed:\n{result.stdout}"
    )
    broken = [ln for ln in result.stdout.splitlines() if ln.endswith("BROKEN")]
    assert any(
        contract in ln for ln in broken
    ), f"expected the {contract!r} contract to break; broken contracts were {broken}\n{result.stdout}"


def test_probes_left_no_residue() -> None:
    """Every probe path is gone once the suite has run."""
    leftover = sorted(p.relative_to(REPO_ROOT) for p in PKG.rglob("_boundary_probe.py"))
    assert not leftover, f"boundary probes were left behind: {leftover}"
