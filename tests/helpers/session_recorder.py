"""Capture what Snowflake shows of a published project, and compare it with the committed recording.

The offline tests trust `FakeSnowflake` to answer the way Snowflake does; a recording is only
honest if it was captured. `python -m tests.helpers.session_recorder` publishes the live project
(`tests/helpers/live_project.py`) into `RECORDING_SCHEMA`, a schema nothing but this script
writes, observes it through the real connector -- SHOW, the ownership marker, the table it reads,
the state rows -- and drops the schema again.

The observation is written in the shape `run_recorded_plan.py` reads, with the account's database
and schema and every per-run value (timestamps, owners, run ids) replaced by placeholders, so two
captures of an unchanged Snowflake are byte-identical. Our side of the comparison cannot move,
because the project and the schema are fixed; a difference means Snowflake's behaviour moved.

Exit status: 0 when the capture matches the recording; 1 when it differs, or when there is no
recording yet, after writing the capture to `--out` for review and commit.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.sql import literal, scope, sql
from tests.helpers.e2e_cli import run_sst
from tests.helpers.live_project import TARGET, live_view, orders_table, project_args, published_project
from tests.helpers.live_snowflake import (
    SCRATCH_MARKER,
    LiveAccount,
    load_live_account,
    not_configured_reason,
    scratch_scope,
)
from tests.helpers.reference_project import REPO_ROOT

RECORDING = REPO_ROOT / "tests" / "fixtures" / "recordings" / "live_project.json"
RECORDING_SCHEMA = "SST_IT_RECORDING"
DATABASE, SCHEMA, VOLATILE, DIGEST = "<DATABASE>", "<SCHEMA>", "<volatile>", "<sha256>"
_VOLATILE_KEYS = frozenset({"created_on", "owner", "applied_at", "run_id", "git_sha"})
# SST's own digests (manifest, fingerprint, DDL) move with SST, not with Snowflake.
_SHA256 = re.compile(r"\b[0-9a-f]{64}\b")


def observe(port: SnowflakePort, schema: SchemaScope) -> dict[str, Any]:
    """What Snowflake shows of the live project published in `schema`, unnormalised."""
    view = live_view(schema)
    table = orders_table(schema)
    marker = port.describe_marker(view, "SEMANTIC VIEW")
    state = port.read_state(QualifiedName(schema.database, schema.schema, Identifier.parse("SST_STATE")), TARGET)
    return {
        "objects": {f"SEMANTIC VIEW|{schema.sql}": [asdict(row) for row in port.show_objects("SEMANTIC VIEW", schema)]},
        "markers": {}
        if marker is None
        else {view.sql: {"manifest_id": marker.manifest_id, "fingerprint": marker.fingerprint}},
        "existing": [table.sql] if port.object_exists("TABLE", table) else [],
        "state": {key: entry.as_dict() for key, entry in sorted((state or {}).items())},
    }


def normalise(observation: Mapping[str, Any], schema: SchemaScope) -> str:
    """The observation as canonical JSON, with names and per-run values replaced by placeholders."""

    def scrub(value: Any, key: str | None = None) -> Any:
        if key in _VOLATILE_KEYS and value:
            return VOLATILE
        if isinstance(value, Mapping):
            return {replace(str(name)): scrub(item, str(name)) for name, item in sorted(value.items())}
        if isinstance(value, list):
            return [scrub(item) for item in value]
        return replace(value) if isinstance(value, str) else value

    def replace(text: str) -> str:
        text = _SHA256.sub(DIGEST, text)
        text = re.sub(rf"\b{re.escape(schema.database.value)}\b", DATABASE, text)
        return re.sub(rf"\b{re.escape(schema.schema.value)}\b", SCHEMA, text)

    return json.dumps(scrub(observation), indent=2, sort_keys=True) + "\n"


def compare(captured: str, recording: Path) -> tuple[bool, str]:
    """Whether `captured` matches the committed `recording`, with a unified diff when it does not."""
    if not recording.is_file():
        return False, f"no recording at {recording}: review the capture and commit it\n"
    committed = recording.read_text(encoding="utf-8")
    if committed == captured:
        return True, ""
    diff = difflib.unified_diff(committed.splitlines(True), captured.splitlines(True), "committed", "captured")
    return False, "".join(diff)


def capture(port: SnowflakePort, account: LiveAccount, root: Path) -> str:
    """Publish the live project into the recording schema, observe it, and drop the schema.

    Raises:
        RuntimeError: Snowflake refused a statement, or `sst` exited non-zero.
    """
    schema = scratch_scope(account, RECORDING_SCHEMA)
    made = port.execute_script(
        (
            sql(
                "CREATE OR REPLACE SCHEMA {schema} COMMENT = {marker}",
                schema=scope(schema),
                marker=literal(SCRATCH_MARKER),
            ),
        )
    )
    if not made.ok:
        raise RuntimeError(f"could not create {schema.sql}: {made.error}")
    try:
        project, manifest = published_project(root, port, schema, key_pair=account.key_pair)
        for command in (("compile",), ("apply", "--yes")):
            run = run_sst(command[0], *project_args(project, manifest), *command[1:])
            if run.exit_code != 0:
                raise RuntimeError(f"sst {command[0]} exited {run.exit_code}: {run.stdout}{run.stderr}")
        return normalise(observe(port, schema), schema)
    finally:
        port.execute_script((sql("DROP SCHEMA IF EXISTS {schema} CASCADE", schema=scope(schema)),))


def main(argv: Sequence[str] | None = None) -> int:
    """Capture against the configured live account and compare with the recording."""
    parser = argparse.ArgumentParser(prog="python -m tests.helpers.session_recorder", description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="Where to write the capture.")
    parser.add_argument("--recording", type=Path, default=RECORDING, help="The committed recording.")
    options = parser.parse_args(argv)
    account = load_live_account(os.environ)
    if account is None:
        print(f"session_recorder: {not_configured_reason()}; nothing can be captured", file=sys.stderr)
        return 1
    from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector

    connector = SnowflakeConnector(account.connection_params())
    try:
        with tempfile.TemporaryDirectory() as root:
            captured = capture(connector, account, Path(root))
    finally:
        connector.close()
    options.out.parent.mkdir(parents=True, exist_ok=True)
    options.out.write_text(captured, encoding="utf-8")
    same, report = compare(captured, options.recording)
    sys.stdout.write(report or "the capture matches the committed recording\n")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
