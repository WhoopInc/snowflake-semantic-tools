#!/usr/bin/env python3
"""Run one CLI command against a recorded Snowflake observation."""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

from snowflake_semantic_tools.adapters.snowflake.memory import RecordedSnowflake
from snowflake_semantic_tools.cli import main as cli_module
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake import StagedFileMetadata
from snowflake_semantic_tools.domain.state.model import AppliedEntry


def recorded(path: pathlib.Path) -> RecordedSnowflake:
    value: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    objects = {
        tuple(raw_key.split("|", 1)): tuple(ShowRow(**row) for row in rows)
        for raw_key, rows in value.get("objects", {}).items()
    }
    markers = {
        key: OwnershipMarker(str(raw["manifest_id"]), str(raw["fingerprint"]))
        for key, raw in value.get("markers", {}).items()
    }
    state = {key: AppliedEntry.from_dict(raw) for key, raw in value.get("state", {}).items()}
    return RecordedSnowflake(
        objects=objects,
        markers=markers,
        state=state,
        existing=tuple(str(item) for item in value.get("existing", [])),
        stage_formats={str(key): str(item) for key, item in value.get("stage_formats", {}).items()},
        staged_file_metadata={
            str(key): StagedFileMetadata(**raw) for key, raw in value.get("staged_file_metadata", {}).items()
        },
        staged_file_contents={
            str(key): str(item).encode("utf-8") for key, item in value.get("staged_file_contents", {}).items()
        },
        role="SST_REFERENCE_OFFLINE",
        account_locator="SST_REFERENCE_OFFLINE",
    )


def main() -> None:
    observation = pathlib.Path(sys.argv[1])
    port = recorded(observation)
    port.close = lambda: None  # type: ignore[attr-defined]
    cli_module.SnowflakeConnector = lambda params: port  # type: ignore[assignment]
    cli_module.cli.main(args=sys.argv[2:], prog_name="sst", standalone_mode=True)


if __name__ == "__main__":
    main()
