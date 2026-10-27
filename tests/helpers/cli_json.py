"""Run the CLI in-process with `--output json` and read the diagnostics its envelope reports."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli


def invoke_json(args: Sequence[str]) -> tuple[int, list[dict[str, Any]]]:
    """Run `sst <args> --output json`; return its exit code and the envelope's diagnostics."""
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    return result.exit_code, json.loads(result.output)["diagnostics"]
