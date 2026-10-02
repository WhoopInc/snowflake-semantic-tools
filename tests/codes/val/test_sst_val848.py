"""SST-VAL848: mCP server entry is not a configuration.

Fires when an MCP server entry is a placeholder; the nearest legitimate input stays quiet.
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


def test_sst_val848_fires() -> None:
    diagnostic = only(
        validate_profile_catalog(
            profile_catalog(profile(mcp_servers=("dbt",)), mcp_configs=(mcp(servers={"dbt": "TODO"}),)), SKILLS
        ),
        "SST-VAL848",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "profile 'analyst': MCP server 'dbt' is not a configuration object"
    assert diagnostic.subject == "profile:analyst"


def test_sst_val848_silent() -> None:
    assert "SST-VAL848" not in codes(
        validate_profile_catalog(profile_catalog(profile(mcp_servers=("dbt",)), mcp_configs=(mcp(),)), SKILLS)
    )
