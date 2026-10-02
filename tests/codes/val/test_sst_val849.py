"""SST-VAL849: two MCP configs define one server.

Fires when two MCP configs a profile uses define one server; the nearest legitimate input stays
quiet.
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
    profile,
    profile_catalog,
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val849_fires() -> None:
    diagnostic = only(
        validate_profile_catalog(
            profile_catalog(profile(mcp_servers=("dbt", "dbt-copy")), mcp_configs=(mcp(), mcp("dbt-copy"))), SKILLS
        ),
        "SST-VAL849",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "profile 'analyst': MCP server 'dbt' is defined by both 'dbt' and 'dbt-copy'"
    assert diagnostic.subject == "profile:analyst"


def test_sst_val849_silent() -> None:
    assert "SST-VAL849" not in codes(
        validate_profile_catalog(
            profile_catalog(profile(mcp_servers=("dbt",)), mcp_configs=(mcp(), mcp("dbt-copy"))), SKILLS
        )
    )
