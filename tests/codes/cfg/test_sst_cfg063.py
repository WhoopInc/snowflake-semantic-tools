"""SST-CFG063: the baseline file is not JSON, or not a baseline."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.baseline import read_baseline
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_cfg063_fires(tmp_path: Path) -> None:
    # GitHub #272: its fix once spoke of links, as SST-PRT009's does.
    path = tmp_path / "baseline.json"
    path.write_text('{"version": 1, "entries": [', encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        read_baseline(path, ".sst/baseline.json")
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG063", Severity.ERROR)
    assert diagnostic.message.startswith("could not read .sst/baseline.json: ")
    assert diagnostic.suggestion == "fix the JSON, or delete the file and run `sst baseline add` to write it again"


def test_sst_cfg063_silent(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    path.write_text('{"version": 1, "expires_on": "2099-01-01", "entries": []}', encoding="utf-8")
    assert read_baseline(path, ".sst/baseline.json").entries == ()
    # A file that cannot be read at all is SST-PRT009's.
    with pytest.raises(ProjectError) as raised:
        read_baseline(tmp_path / "absent.json", "absent.json")
    assert [item.code for item in raised.value.diagnostics] == ["SST-PRT009"]
