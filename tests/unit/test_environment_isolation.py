"""A developer's shell environment does not reach offline tests, module-scoped fixtures included.

`tests/unit/test_golden_ddl.py` loads the reference project once per module. With a
`DBT_PROFILES_DIR` exported for live work, that load used to resolve another project's profile,
and the goldens failed on a database name the fixture never declares.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_a_profiles_dir_in_the_shell_does_not_reach_module_scoped_fixtures(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles.yml"
    profiles.write_text(
        "sst_reference_impl:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev: {type: snowflake, account: x, user: x, role: x, database: ELSEWHERE, schema: S, warehouse: W}\n"
    )
    environment = {**os.environ, "DBT_PROFILES_DIR": str(tmp_path), "SST_PROFILES_DIR": str(tmp_path)}
    finished = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-o",
            "addopts=",
            "tests/unit/test_golden_ddl.py",
        ],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=110,
        check=False,
    )
    assert finished.returncode == 0, finished.stdout[-2000:]
