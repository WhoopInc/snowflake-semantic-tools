"""SST-PRS031: a member name is a Snowflake reserved word."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.prs_codes import NAMED_AGENT, NAMED_MEMBER, named_findings
from tests.helpers.seam_projects import COUNTED, SmallProject, metric_file


def test_sst_prs031_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=metric_file(COUNTED, name="select")).load().diagnostics,
        "SST-PRS031",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "'select' is a reserved word"
    assert diagnostic.subject == "metric:select"


def test_sst_prs031_silent(tmp_path: Path) -> None:
    assert (
        coded(
            SmallProject(tmp_path, files=metric_file(COUNTED, name="selection")).load().diagnostics,
            "SST-PRS031",
        )
        == []
    )


def test_sst_prs031_fires_for_an_agent_and_a_tool_member_name(tmp_path: Path) -> None:
    exit_code, found = named_findings(tmp_path, "SST-PRS031", agent="select", member="table")
    assert exit_code == 0
    assert [(item["artifact"], item["location"]["file"]) for item in found] == [
        ({"name": "select", "type": "agent"}, NAMED_AGENT),
        ({"name": "table", "type": "tool"}, NAMED_MEMBER),
    ]


def test_sst_prs031_silent_for_an_agent_and_a_tool_member_name(tmp_path: Path) -> None:
    assert named_findings(tmp_path, "SST-PRS031", agent="selection", member="tables") == (0, [])
