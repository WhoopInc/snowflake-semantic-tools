"""SST-PRT108: `sst drop` was given an object name that is not three-part."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli


def _drop(tmp_path: Path, name: str) -> tuple[int, list[dict[str, object]]]:
    args = ["drop", name, "--type", "semantic_view", "--target", "dev", "--project-dir", str(tmp_path)]
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    return result.exit_code, json.loads(result.output)["diagnostics"]


def test_sst_prt108_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = _drop(tmp_path, "SCHEMA.ORDERS")
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT108", "error")
    assert diagnostic["message"] == "sst drop requires a fully-qualified name; 'SCHEMA.ORDERS' is not one"
    assert diagnostic["suggestion"] == "pass <database>.<schema>.<object>"


def test_sst_prt108_silent(tmp_path: Path) -> None:
    # A three-part name, quoted parts included, passes; the next refusal is the missing --yes.
    exit_code, [diagnostic] = _drop(tmp_path, 'DB."My.Schema".ORDERS')
    assert (exit_code, diagnostic["code"]) == (3, "SST-PRT109")
