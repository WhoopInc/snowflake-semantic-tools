"""SST-CFG036: a `skills.extensions` entry cannot be qualified to a three-part name."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.project import consumed_extensions
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.artifact_builders import target


def test_sst_cfg036_fires() -> None:
    resolved, [diagnostic] = consumed_extensions(
        {"skills": {"extensions": {"glossary": None}}}, target(), file="sst_config.yaml"
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG036", Severity.ERROR)
    assert diagnostic.message == (
        "skills.extensions: 'glossary' cannot be qualified -- no fqn: and the block sets no default_prefix"
    )
    assert (diagnostic.subject, diagnostic.origin) == ("config:skills.extensions.glossary", Origin("sst_config.yaml"))
    assert resolved == {}


def test_sst_cfg036_silent() -> None:
    resolved, diagnostics = consumed_extensions(
        {"skills": {"extensions": {"default_prefix": "DB.EXT", "glossary": None}}}, target()
    )
    assert (resolved["glossary"].sql, diagnostics) == ("DB.EXT.GLOSSARY", ())
