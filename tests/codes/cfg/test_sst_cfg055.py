"""SST-CFG055: a root-only `+` key, `skills.+threads`, is set in a folder route or in a skill's `config:`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.validate.config import validate_config
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

ORIGIN = Origin("skills/finance/close/SKILL.md", 1)


def test_sst_cfg055_fires() -> None:
    [diagnostic] = validate_config({"skills": {"finance": {"+threads": 2}}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG055", Severity.ERROR)
    assert diagnostic.message == "'skills.finance.+threads' can be set only at the root of skills"
    assert diagnostic.subject == "config:skills.finance.+threads"
    assert diagnostic.suggestion == "set skills.+threads once, at the root of skills, and remove it here"
    [own] = item_config_diagnostics("skills", {"threads": 2}, subject="skill:close", origin_of=lambda key: ORIGIN)
    assert own.code == "SST-CFG055"
    assert (own.subject, own.origin) == ("skill:close", ORIGIN)


def test_sst_cfg055_silent() -> None:
    assert validate_config({"skills": {"+threads": 2, "finance": {"+schema": "FIN"}}}) == ()
    found = item_config_diagnostics("skills", {"schema": "FIN"}, subject="skill:close", origin_of=lambda key: ORIGIN)
    assert found == ()
