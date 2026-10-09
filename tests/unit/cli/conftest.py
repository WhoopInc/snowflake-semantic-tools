"""Every CLI test runs `sst debug` against a scripted `dbt --version`, never the `dbt` on `PATH`."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.cli.commands import debug
from tests.helpers.seam_projects import FakeDbt


@pytest.fixture(autouse=True)
def _scripted_dbt_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(debug, "dbt_runner", FakeDbt())
