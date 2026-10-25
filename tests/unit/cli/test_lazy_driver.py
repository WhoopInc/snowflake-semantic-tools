"""A command that never connects never imports the Snowflake driver, which costs most of a second."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.helpers.network_guard import guarded_environment

# Each starts a process of its own; deselected locally, run in CI (`-m "not live"`).
pytestmark = pytest.mark.slow

REPO_ROOT = Path(__file__).resolve().parents[3]

# Runs `sst <args>` in-process, then reports whether the driver was ever imported.
_PROBE = """
import sys
from snowflake_semantic_tools.cli.main import cli

try:
    cli(sys.argv[1:], standalone_mode=True)
except SystemExit:
    pass
print("DRIVER-IMPORTED" if "snowflake.connector" in sys.modules else "DRIVER-ABSENT", file=sys.stderr)
"""


@pytest.mark.parametrize("args", [("explain", "SST-DBT020"), ("--help",), ("list", "--help")])
def test_an_offline_command_does_not_import_the_snowflake_driver(args: tuple[str, ...], tmp_path: Path) -> None:
    env = guarded_environment(
        {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, (str(REPO_ROOT), os.environ.get("PYTHONPATH"))))}
    )
    done = subprocess.run(
        [sys.executable, "-c", _PROBE, *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
        check=False,
    )
    assert "DRIVER-ABSENT" in done.stderr, done.stdout + done.stderr


def test_a_session_imports_the_driver_when_it_connects(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.adapters.snowflake.connector.session import Session

    seen: list[dict[str, object]] = []

    def connect(**params: object) -> object:
        seen.append(params)
        return object()

    monkeypatch.setattr("snowflake.connector.connect", connect)
    Session({"account": "a"})
    assert seen == [{"account": "a"}]
