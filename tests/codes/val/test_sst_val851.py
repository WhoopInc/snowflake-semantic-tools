"""SST-VAL851: profile key is not accepted.

A profile that sets `active`, a registry column SST owns, is refused; its accepted keys load.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import write


def load(root: Path, files: Mapping[str, str | bytes]) -> ProfileCatalog:
    write(root, files)
    return load_profile_catalog(
        root, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers", commands_dir="commands"
    )


def test_sst_val851_fires(tmp_path: Path) -> None:
    catalog = load(tmp_path, {"profiles/analyst/profile.yml": "name: analyst\nactive: true\n"})
    diagnostic = only(catalog.diagnostics, "SST-VAL851")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "profile 'analyst': 'active' is not accepted: delete the profile and apply with --prune to deactivate it"
    )
    assert diagnostic.subject == "profile:analyst"


def test_sst_val851_silent(tmp_path: Path) -> None:
    catalog = load(tmp_path, {"profiles/analyst/profile.yml": "name: analyst\ndescription: Analyst.\n"})
    assert "SST-VAL851" not in codes(catalog.diagnostics)
