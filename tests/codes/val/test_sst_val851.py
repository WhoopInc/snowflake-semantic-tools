"""SST-VAL851: a Cortex Code profile sets a key agent profiles cannot carry.

A profile that sets `hooks`, which a Cortex Code agent profile has no place for, is refused with
the reason; its accepted keys load.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.val_codes import load_profiles

MANIFEST = "coco_profiles/analyst/coco_profile.yml"


def test_sst_val851_fires(tmp_path: Path) -> None:
    _, diagnostics = load_profiles(tmp_path, {MANIFEST: "name: analyst\nhooks: [sql-guard]\n"})
    diagnostic = only(diagnostics, "SST-VAL851")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "coco profile 'analyst': 'hooks' is not accepted: Cortex Code agent profiles cannot carry hooks"
    )
    assert diagnostic.subject == "coco_profile:analyst"


def test_sst_val851_silent(tmp_path: Path) -> None:
    _, diagnostics = load_profiles(tmp_path, {MANIFEST: "name: analyst\ndescription: Analyst.\n"})
    assert "SST-VAL851" not in codes(diagnostics)
