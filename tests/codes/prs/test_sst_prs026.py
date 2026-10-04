"""SST-PRS026: a view's tag value is longer than 256 characters."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, view_file

CONFIG = "project:\n  semantic_models_dir: semantic_models\ntags:\n  default_prefix: DB.SCH\n  tier: {}\n"


def _tagged(value: str) -> dict[str, str]:
    return {
        "sst_config.yml": CONFIG,
        **view_file(f"    tags:\n      - name: \"{{{{ tag('tier') }}}}\"\n        value: {value}\n"),
    }


def test_sst_prs026_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(SmallProject(tmp_path, files=_tagged("g" * 257)).load().diagnostics, "SST-PRS026")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:catalog: tag 'tier' value is 257 chars, over 256"
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_prs026_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files=_tagged("g" * 256)).load().diagnostics, "SST-PRS026") == []
