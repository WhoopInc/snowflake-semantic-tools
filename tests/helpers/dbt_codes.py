"""What the per-code DBT tests share: a dbt that writes a manifest, and the refusal a parse raises."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import parse_project
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from tests.helpers.seam_projects import FakeDbt, manifest


def refused(
    tmp_path: Path, dbt: FakeDbt, *, auto_compile: bool = False, echo: Callable[[str], object] = print
) -> Diagnostic:
    """The one diagnostic parsing the project at `tmp_path` with `dbt` refuses with."""
    with pytest.raises(ProjectError) as raised:
        manifest_path = tmp_path / "target" / "manifest.json"
        parse_project(tmp_path, "dev", manifest_path, runner=dbt, auto_compile=auto_compile, echo=echo)
    [diagnostic] = raised.value.diagnostics
    return diagnostic


def writing_dbt(tmp_path: Path) -> FakeDbt:
    """A dbt whose run writes the seam manifest to `target/manifest.json` under `tmp_path`."""
    return FakeDbt(writes=(tmp_path / "target" / "manifest.json", manifest()))


def key_column(**sst: str) -> dict[str, object]:
    """A manifest column `products_id` whose `meta.sst` is `sst`."""
    return {"name": "products_id", "description": "The key.", "data_type": "VARCHAR", "meta": {"sst": sst}}
