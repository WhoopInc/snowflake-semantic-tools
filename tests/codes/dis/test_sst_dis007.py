"""SST-DIS007: two artifact types claim one directory."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml, registry_roots
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dis_codes import models


def test_sst_dis007_fires(tmp_path: Path) -> None:
    models(tmp_path, "metrics.yml")
    config = {"project": {"tools_dir": "semantic_models/tools"}}
    [diagnostic] = discover_yaml(tmp_path, "semantic_models", config=config).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS007", Severity.ERROR)
    assert diagnostic.message == "semantic_models/tools is claimed by ['semantic_view', 'tool']"


def test_sst_dis007_silent(tmp_path: Path) -> None:
    models(tmp_path, "metrics.yml")
    # An eval nests inside its agent's folder by design, so the two never clash.
    assert registry_roots({})["eval"] == "agents/*/evals"
    assert discover_yaml(tmp_path, "semantic_models", config={}).diagnostics == ()
