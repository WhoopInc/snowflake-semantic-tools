"""SST-VAL815: bundled script reads a credential or an absolute local path.

Fires when a bundled script names an absolute local path; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.skill_flatten import flatten_skill
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val815_fires() -> None:
    diagnostic = only(
        flatten_skill(
            skill(
                files={
                    "run.py": 'open("/Users/.../data.csv")\n',
                    "SKILL.md": REF.format(ref="reference/steps.md, then run run.py"),
                }
            )
        )[2],
        "SST-VAL815",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "skill 'month-close': 'run.py' contains the absolute path '/Users/.../data.csv'"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val815_silent() -> None:
    assert "SST-VAL815" not in codes(
        flatten_skill(
            skill(
                files={
                    "run.py": 'open("data.csv")\n',
                    "SKILL.md": REF.format(ref="reference/steps.md, then run run.py"),
                }
            )
        )[2]
    )


def _hazards(files: dict[str, str]) -> list[str]:
    links = REF.format(ref="reference/steps.md, then run run.sh")
    found = flatten_skill(skill(files={**files, "SKILL.md": links}))[2]
    return [item.context["detail"] for item in found if item.code == "SST-VAL815"]


@pytest.mark.parametrize(
    ("path", "text", "detail"),
    [
        ("run.sh", 'export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"\n', "a credential literal"),
        ("run.sh", "SNOWFLAKE_PASSWORD='s3cr3tvalue'\n", "a credential literal"),
        ("run.sh", "cat /etc/passwd\n", "the absolute path '/etc/passwd'"),
        ("reference/steps.md", "password: hunter2hunter2\n", "a credential literal"),
        ("run.sh", "open('/tmp/x.csv')\n", "the absolute path '/tmp/x.csv'"),
    ],
)
def test_sst_val815_fires_for_a_root_path_or_a_prefixed_or_unquoted_credential(
    path: str, text: str, detail: str
) -> None:
    assert _hazards({path: text}) == [detail]


@pytest.mark.parametrize(
    ("path", "text"),
    [
        ("reference/steps.md", "password: $SNOWFLAKE_PASSWORD\n"),
        ("reference/steps.md", "password: <yours>\n"),
        ("run.sh", 'curl "https://example.com/api/v2/statements"\n'),
        ("run.sh", "connect(password=get_password())\n"),
    ],
)
def test_sst_val815_silent_for_a_variable_a_placeholder_or_a_url_path(path: str, text: str) -> None:
    assert _hazards({path: text}) == []
