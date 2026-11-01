"""The JSON Schemas of SST's authored files and `sst_config.yml`, by the kind each checks.

`schemas()` returns them all; `ProjectSession.schema_for` names the one that checks a file.
Each is generated from the tables SST itself reads by, so it cannot drift from what SST accepts.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.render.json_schema import file_schemas


def schemas() -> dict[str, dict[str, object]]:
    """Return every schema by kind: one per authored file kind, `semantic_file`, and `sst_config`."""
    return file_schemas()
