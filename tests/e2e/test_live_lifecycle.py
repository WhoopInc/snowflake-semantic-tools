"""The whole command sequence against a real account, as a user runs it, ending in the smoke suite.

Each step is the CLI in a subprocess with `--output json`, asserted on its documented exit code and
on silence on stderr: `validate` checks syntax against Snowflake, `plan` reports pending changes
(exit 2), `apply` publishes them, a second `plan` finds nothing (exit 0), and `sst test --suite
smoke` probes every published metric -- the check `apply` itself never makes.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.e2e_cli import run_sst
from tests.helpers.live_project import project_args, published_project
from tests.helpers.live_snowflake import LiveAccount

pytestmark = [pytest.mark.live, pytest.mark.e2e]


def test_validate_plan_apply_replan_and_smoke_each_exit_as_documented(
    live_account: LiveAccount,
    live_connector: SnowflakeConnector,
    scratch_schema: Callable[[str], SchemaScope],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    project, manifest = published_project(
        tmp_path_factory.mktemp("e2e"), live_connector, scratch_schema("e2e"), key_pair=live_account.key_pair
    )
    args = project_args(project, manifest)
    steps = (
        (("validate", *args, "--strict", "--snowflake-syntax-check"), 0),
        (("compile", *args), 0),
        (("plan", *args), 2),
        (("apply", *args, "--yes"), 0),
        (("plan", *args), 0),
        (("test", *args, "--suite", "smoke"), 0),
    )
    for command, expected in steps:
        run = run_sst("--output", "json", *command)
        assert (run.exit_code, run.stderr) == (expected, ""), f"sst {command[0]}: {run.stdout}"
        assert run.envelope["command"] == command[0]
