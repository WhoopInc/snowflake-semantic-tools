"""SST-VAL850: mCP config carries a literal credential.

Fires when an MCP config sets a credential to a literal; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.profile import (
    validate_profile_catalog,
)
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    mcp,
    profile_catalog,
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val850_fires() -> None:
    diagnostic = only(
        validate_profile_catalog(
            profile_catalog(mcp_configs=(mcp(servers={"dbt": {"env": {"API_KEY": "abc123"}}}),)), SKILLS
        ),
        "SST-VAL850",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "MCP config 'dbt': server 'dbt' sets 'API_KEY' to a literal value"
    assert diagnostic.subject == "mcp:dbt"


def test_sst_val850_silent() -> None:
    assert "SST-VAL850" not in codes(
        validate_profile_catalog(
            profile_catalog(mcp_configs=(mcp(servers={"dbt": {"env": {"API_KEY": "${DBT_KEY}"}}}),)), SKILLS
        )
    )
