"""SST-CFG057: a `governance.sensitive_tags` entry names no tag."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg057_fires() -> None:
    [diagnostic] = validate_config({"governance": {"sensitive_tags": ["TAG_DB.PRIVACY"]}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG057", Severity.ERROR)
    assert diagnostic.message == "'governance.sensitive_tags[0]' is 'TAG_DB.PRIVACY', which names no tag"
    assert diagnostic.subject == "config:governance.sensitive_tags"


def test_sst_cfg057_silent() -> None:
    entries = ["TAG_DB.PRIVACY.*", "tag_db.privacy.email", 'TAG_DB."Gov".TAG']
    assert list(validate_config({"governance": {"sensitive_tags": entries, "read_column_tags": True}})) == []
