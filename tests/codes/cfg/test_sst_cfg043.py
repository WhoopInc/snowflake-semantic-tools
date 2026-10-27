"""SST-CFG043: a key the schema records as removed is set."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg043_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"generation": {"publish_via": "ddl"}}) if item.code == "SST-CFG043"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'generation.publish_via' was removed: SST 1.0 renders DDL directly"
    assert diagnostic.subject == "config:generation.publish_via"


_GONE = (
    "CoCo Desktop profiles are gone: Cortex Code agent profiles replaced the registry; move each profile to "
    "coco_profiles/ and configure coco_profiles:"
)


def test_sst_cfg043_points_each_coco_desktop_profile_key_at_cortex_code_profiles() -> None:
    tree = {
        "project": {"profiles_dir": "p", "coco_desktop_profiles_dir": "c", "hooks_dir": "h"},
        "profiles": {"+stage": "S"},
        "coco_desktop_profiles": {"+stage": "S"},
    }
    found = {item.subject: item.message for item in validate_config(tree) if item.code == "SST-CFG043"}
    assert found == {
        "config:project.profiles_dir": f"config key 'project.profiles_dir' was removed: {_GONE}",
        "config:project.coco_desktop_profiles_dir": (
            f"config key 'project.coco_desktop_profiles_dir' was removed: {_GONE}"
        ),
        "config:project.hooks_dir": (
            "config key 'project.hooks_dir' was removed: Cortex Code agent profiles cannot carry hooks"
        ),
        "config:profiles": f"config key 'profiles' was removed: {_GONE}",
        "config:coco_desktop_profiles": f"config key 'coco_desktop_profiles' was removed: {_GONE}",
    }


def test_sst_cfg043_silent() -> None:
    assert "SST-CFG043" not in [item.code for item in validate_config({"apply": {"fail_fast": True}})]
