"""SST-VAL851: profile key is not accepted.

A profile that sets `active`, a registry column SST owns, is refused; its accepted keys load_profiles.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.val_codes import load_profiles


def test_sst_val851_fires(tmp_path: Path) -> None:
    catalog = load_profiles(tmp_path, {"profiles/analyst/profile.yml": "name: analyst\nactive: true\n"})
    diagnostic = only(catalog.diagnostics, "SST-VAL851")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "profile 'analyst': 'active' is not accepted: delete the profile and apply with --prune to deactivate it"
    )
    assert diagnostic.subject == "profile:analyst"


def test_sst_val851_silent(tmp_path: Path) -> None:
    catalog = load_profiles(tmp_path, {"profiles/analyst/profile.yml": "name: analyst\ndescription: Analyst.\n"})
    assert "SST-VAL851" not in codes(catalog.diagnostics)
