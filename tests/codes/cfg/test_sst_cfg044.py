"""SST-CFG044: a key reserved for a later release is set: an eval config's `sweep:` block."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.evals.config import parse_config
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity

_CONFIG = "agent: sales\nmetrics:\n  system: [tool_selection_accuracy]\n"
_FILE = "agents/sales/evals/config.yml"


def _parsed(text: str) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    parse_config((_FILE, parse_yaml_bytes(text.encode(), _FILE)), diagnostics)
    return [item for item in diagnostics if item.code == "SST-CFG044"]


def test_sst_cfg044_fires() -> None:
    [diagnostic] = _parsed(_CONFIG + "sweep:\n  enabled: true\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'sweep' is not supported in this release"
    assert diagnostic.origin is not None and diagnostic.origin.file == _FILE


def test_sst_cfg044_silent() -> None:
    assert _parsed(_CONFIG) == []
