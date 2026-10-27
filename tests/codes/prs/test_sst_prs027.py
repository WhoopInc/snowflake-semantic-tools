"""SST-PRS027: a view's `config.tags` is not a list of name and value entries.

A `config.tags` that is not a list is reported by the setting's own type check (SST-CFG004),
so this code is for an entry of the list.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, view_file


def test_sst_prs027_fires(tmp_path: Path) -> None:
    files = view_file("    config:\n      tags:\n        - name: tier\n")
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS027")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:catalog: tags must be a list of name and value entries, found {'name': 'tier'}"
    )
    assert diagnostic.subject == "semantic_view:catalog"


def test_a_tag_name_that_is_not_a_call_is_sst_ref050_not_sst_prs027(tmp_path: Path) -> None:
    files = view_file("    config:\n      tags:\n        - name: tier\n          value: gold\n")
    diagnostics = SmallProject(tmp_path, files=files).load().diagnostics
    assert coded(diagnostics, "SST-PRS027") == []
    [diagnostic] = coded(diagnostics, "SST-REF050")
    assert diagnostic.message == "semantic_view:catalog: config.tags.name is 'tier'; it takes a {{ tag(...) }} call"


def test_sst_prs027_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files=view_file("")).load().diagnostics, "SST-PRS027") == []
