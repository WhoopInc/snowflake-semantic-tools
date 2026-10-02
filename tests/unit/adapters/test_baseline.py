"""The project's diagnostic baseline file, read as `sst validate` reads it."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.baseline import BASELINE_FILE, read_baseline


def write(project: Path, text: str) -> None:
    (project / BASELINE_FILE).parent.mkdir(parents=True, exist_ok=True)
    (project / BASELINE_FILE).write_text(text, encoding="utf-8")


def test_a_project_without_a_baseline_has_no_entries(tmp_path: Path) -> None:
    assert read_baseline(tmp_path) == ()


def test_each_entry_contributes_its_fingerprint(tmp_path: Path) -> None:
    write(tmp_path, '{"version": 1, "entries": [{"fingerprint": "ab12", "code": "SST-VAL003"}, {"fingerprint": "cd"}]}')
    assert read_baseline(tmp_path) == ("ab12", "cd")


@pytest.mark.parametrize("text", ["{", '{"entries": {}}', '{"entries": [{"code": "x"}]}', "[]"])
def test_a_baseline_that_cannot_be_read_is_refused(tmp_path: Path, text: str) -> None:
    write(tmp_path, text)
    with pytest.raises(ProjectError, match="baseline"):
        read_baseline(tmp_path)
