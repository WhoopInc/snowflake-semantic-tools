"""SST-VAL852: hook definition is incomplete or ambiguous.

A hook folder with two scripts and no `script:` cannot say which one runs; naming it settles that.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.val_codes import load_profiles


def test_sst_val852_fires(tmp_path: Path) -> None:
    files = {"hooks/guard/hook.yml": "event: Stop\ncommand: bash\n", "hooks/guard/a.sh": "a", "hooks/guard/b.sh": "b"}
    diagnostic = only(load_profiles(tmp_path, files).diagnostics, "SST-VAL852")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "hook 'guard': has 2 candidate scripts and no script: key"
    assert diagnostic.subject == "hook:guard"


def test_sst_val852_silent(tmp_path: Path) -> None:
    files = {
        "hooks/guard/hook.yml": "event: Stop\ncommand: bash\nscript: a.sh\n",
        "hooks/guard/a.sh": "a",
        "hooks/guard/b.sh": "b",
    }
    assert "SST-VAL852" not in codes(load_profiles(tmp_path, files).diagnostics)
