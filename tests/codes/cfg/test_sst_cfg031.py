"""SST-CFG031: the configuration file does not say whether expressions are compiled against Snowflake."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import unstated_policy


def test_sst_cfg031_fires() -> None:
    [diagnostic] = unstated_policy({"validation": {"strict": True}}, file="sst_config.yaml")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG031", Severity.ERROR)
    assert diagnostic.message == "validation.snowflake_syntax_check is unset"
    assert diagnostic.subject == "config:validation.snowflake_syntax_check"
    assert diagnostic.origin is not None and diagnostic.origin.file == "sst_config.yaml"
    assert [item.subject for item in unstated_policy({})] == ["config:validation"]


def test_sst_cfg031_silent() -> None:
    assert unstated_policy({"validation": {"snowflake_syntax_check": False}}) == ()
