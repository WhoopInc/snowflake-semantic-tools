"""SST-PRS017: a verified query's `verified_at` is neither an epoch nor an ISO-8601 timestamp."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import validate_json
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, member_file
from tests.helpers.seam_projects import SmallProject

QUERY = (
    "snowflake_verified_queries:\n  - name: how_many\n    question: How many?\n    sql: SELECT 1\n"
    "    tables: [\"{{{{ ref('products') }}}}\"]\n    verified_at: {value}\n"
)
FILE = "semantic_models/verified_queries/vq.yml"


def _project(tmp_path: Path, value: str) -> SmallProject:
    return SmallProject(tmp_path, files={FILE: QUERY.format(value=value)})


def test_sst_prs017_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(_project(tmp_path, "'last tuesday'").load().diagnostics, "SST-PRS017")
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRS017", Severity.ERROR)
    assert (
        diagnostic.message
        == "verified_query:how_many: 'verified_at' value 'last tuesday' is not an epoch or ISO timestamp"
    )
    assert diagnostic.subject == "verified_query:how_many"
    assert diagnostic.origin is not None
    assert (diagnostic.origin.file, diagnostic.origin.line) == (FILE, 6)


def test_sst_prs017_fires_for_an_unquoted_date(tmp_path: Path) -> None:
    [diagnostic] = coded(_project(tmp_path, "2026-10-12").load().diagnostics, "SST-PRS017")
    assert diagnostic.context["found"] == "2026-10-12"


def test_sst_prs017_silent(tmp_path: Path) -> None:
    assert coded(_project(tmp_path, "'2026-09-28T10:00:00Z'").load().diagnostics, "SST-PRS017") == []
    assert coded(_project(tmp_path / "epoch", "1790000000").load().diagnostics, "SST-PRS017") == []


def test_sst_prs017_is_reported_beside_the_other_errors_of_the_run(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        "semantic_models/verified_queries/revenue_by_location.yml",
        'verified_at: "2026-01-01"',
        "verified_at: 2026-01-01",
    )
    (project / member_file("metrics", "zz_bad")).write_text(
        "metric:\n  name: zz_bad\n  description: Not an aggregate.\n  expr: \"{{ ref('orders', 'subtotal') }} * 2\"\n",
        encoding="utf-8",
    )
    exit_code, diagnostics = validate_json(project)
    found = {item["code"]: item for item in diagnostics}
    assert exit_code == 1
    assert {"SST-PRS017", "SST-VAL101"} <= set(found)
    location = found["SST-PRS017"]["location"]
    assert (location["file"], location["line"]) == ("semantic_models/verified_queries/revenue_by_location.yml", 19)
