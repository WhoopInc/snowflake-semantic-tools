"""SST-VAL895: the project still has a `coco_desktop_profiles/` folder, which SST no longer reads."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.val_codes import load_profiles

PROFILE = {"coco_profiles/analyst/coco_profile.yml": "name: analyst\n"}


def test_sst_val895_fires(tmp_path: Path) -> None:
    files = {**PROFILE, "coco_desktop_profiles/analyst/coco_desktop_profile.yml": "name: analyst\n"}
    catalog, diagnostics = load_profiles(tmp_path, files)
    diagnostic = only(diagnostics, "SST-VAL895")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == ("'coco_desktop_profiles/' holds CoCo Desktop profiles, which SST no longer publishes")
    assert (diagnostic.subject, diagnostic.origin) == (None, Origin("coco_desktop_profiles"))
    assert [item.name for item in catalog.profiles] == ["analyst"]


def test_sst_val895_silent(tmp_path: Path) -> None:
    _, diagnostics = load_profiles(tmp_path, PROFILE)
    assert "SST-VAL895" not in codes(diagnostics)
