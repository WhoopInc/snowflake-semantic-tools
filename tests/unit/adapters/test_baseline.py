"""The baseline file, read as every command reads it."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.baseline import BASELINE_FILE, read_baseline
from snowflake_semantic_tools.domain.diagnostics.baseline import BaselineEntry


def write(project: Path, text: str) -> Path:
    path = project / BASELINE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_each_entry_is_read_with_what_it_records(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        '{"version": 1, "expires_on": "2030-01-01", "entries": '
        '[{"fingerprint": "ab12", "code": "SST-VAL003", "note": "known"}, {"fingerprint": "cd"}]}',
    )
    baseline = read_baseline(path, BASELINE_FILE.as_posix())
    assert (baseline.path, baseline.expires_on) == (".sst/baseline.json", "2030-01-01")
    assert baseline.entries == (BaselineEntry("ab12", "SST-VAL003", note="known"), BaselineEntry("cd", ""))


@pytest.mark.parametrize(
    "text",
    [
        "{",
        "[]",
        '{"version": 2, "expires_on": "2030-01-01", "entries": []}',
        '{"version": 1, "entries": []}',
        '{"version": 1, "expires_on": "2030-01-01", "entries": {}}',
        '{"version": 1, "expires_on": "2030-01-01", "entries": [{"code": "x"}]}',
    ],
)
def test_a_baseline_that_cannot_be_read_is_refused(tmp_path: Path, text: str) -> None:
    path = write(tmp_path, text)
    with pytest.raises(ProjectError) as raised:
        read_baseline(path, BASELINE_FILE.as_posix())
    assert [item.code for item in raised.value.diagnostics] == ["SST-PRT009"]


def test_a_missing_baseline_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        read_baseline(tmp_path / "absent.json", "absent.json")
    assert [item.code for item in raised.value.diagnostics] == ["SST-PRT009"]
