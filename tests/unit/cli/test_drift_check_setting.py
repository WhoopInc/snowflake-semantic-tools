"""`plan.drift_check`: which live properties `plan` and `apply`'s re-plan compare with state."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.cli.settings import drift_check_setting
from tests.helpers.projects import project_paths


@pytest.mark.parametrize(
    ("config", "expected"),
    [("project: {}\n", "full"), ("plan:\n  drift_check: basic\n", "basic"), ("plan:\n  drift_check: full\n", "full")],
    ids=["unset", "basic", "full"],
)
def test_the_drift_check_is_the_configured_one_else_full(tmp_path: Path, config: str, expected: str) -> None:
    (tmp_path / "sst_config.yml").write_text(config, encoding="utf-8")
    assert drift_check_setting(project_paths(tmp_path)) == expected
