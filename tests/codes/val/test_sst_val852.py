"""SST-VAL852: hook definition is incomplete or ambiguous.

A hook folder with two scripts and no `script:` cannot say which one runs; naming it settles that.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import write


def load(root: Path, files: Mapping[str, str | bytes]) -> ProfileCatalog:
    write(root, files)
    return load_profile_catalog(
        root, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers", commands_dir="commands"
    )


def test_sst_val852_fires(tmp_path: Path) -> None:
    files = {"hooks/guard/hook.yml": "event: Stop\ncommand: bash\n", "hooks/guard/a.sh": "a", "hooks/guard/b.sh": "b"}
    diagnostic = only(load(tmp_path, files).diagnostics, "SST-VAL852")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "hook 'guard': has 2 candidate scripts and no script: key"
    assert diagnostic.subject == "hook:guard"


def test_sst_val852_silent(tmp_path: Path) -> None:
    files = {
        "hooks/guard/hook.yml": "event: Stop\ncommand: bash\nscript: a.sh\n",
        "hooks/guard/a.sh": "a",
        "hooks/guard/b.sh": "b",
    }
    assert "SST-VAL852" not in codes(load(tmp_path, files).diagnostics)
