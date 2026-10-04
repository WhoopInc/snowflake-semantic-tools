"""SST-DIS001: the semantic-models directory does not exist."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dis_codes import models


def test_sst_dis001_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as caught:
        discover_yaml(tmp_path, "semantic_models")
    [diagnostic] = caught.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS001", Severity.ERROR)
    assert diagnostic.message == "semantic_models does not exist"


def test_sst_dis001_silent(tmp_path: Path) -> None:
    models(tmp_path, "metrics.yml")
    assert [item.path for item in discover_yaml(tmp_path, "semantic_models").files] == ["semantic_models/metrics.yml"]
