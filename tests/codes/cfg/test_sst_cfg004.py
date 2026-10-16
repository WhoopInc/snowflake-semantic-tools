"""SST-CFG004: a configuration value has the wrong type."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config, validated_config
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import config_edited, config_line


def test_sst_cfg004_fires() -> None:
    [diagnostic] = validate_config({"validation": {"strict": "yes"}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG004", Severity.ERROR)
    assert diagnostic.message == "config key 'validation.strict' expects boolean, found str"
    assert diagnostic.subject == "config:validation.strict"


def test_sst_cfg004_silent() -> None:
    assert [item.code for item in validate_config({"validation": {"strict": False}})] == []


def test_sst_cfg004_on_a_location_key_is_reported_and_the_key_dropped(tmp_path: Path) -> None:
    tree, [diagnostic] = validated_config({"profiles": {"+registry_table": 5, "+stage": "S"}})
    assert diagnostic.message == "config key 'profiles.+registry_table' expects object name, found int"
    assert tree == {"profiles": {"+stage": "S"}}
    edit = ("+registry_table: PROFILE_REGISTRY", "+registry_table: 5")
    code, diagnostics = validate_json(config_edited(tmp_path, edit))
    [found] = [item for item in diagnostics if item["code"] == "SST-CFG004"]
    assert code == 1
    assert found["location"]["line"] == config_line(tmp_path, "+registry_table: 5")


@pytest.mark.parametrize(
    ("edit", "key"),
    [
        (("project:\n  # Every", "project:\n  - semantic_models\nproject_was:\n  # Every"), "project"),
        (("  semantic_models_dir: semantic_models", "  semantic_models_dir: 5"), "project.semantic_models_dir"),
    ],
)
def test_sst_cfg004_on_the_project_block_is_reported_before_discovery(
    tmp_path: Path, edit: tuple[str, str], key: str
) -> None:
    code, diagnostics = validate_json(config_edited(tmp_path, edit))
    codes = [item["code"] for item in diagnostics]
    assert code == 1
    assert any(item["code"] == "SST-CFG004" and item["member"]["name"] == key for item in diagnostics)
    assert "SST-INT001" not in codes and "SST-DIS001" not in codes
