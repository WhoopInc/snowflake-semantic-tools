"""SST-VAL531: an object a tool calls, which SST does not publish, does not exist."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, observe, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _called(existing: set[str]) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.existing = existing
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    return coded(observe(port, result), "SST-VAL531")


def test_sst_val531_fires() -> None:
    [diagnostic] = _called(set())
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': 'DB.DEV.LOOKUP' does not exist in target 'dev'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val531_silent() -> None:
    assert _called({"DB.DEV.LOOKUP"}) == []


# MEASURED: SHOW GRANTS ON CORTEX EXTENSION for an extension in a schema that does not exist.
_SCHEMA_MISSING = SnowflakePortError(
    "002003 (02000): SQL compilation error:\nSchema 'DB.EXT' does not exist or not authorized.",
    errno=2003,
    sqlstate="02000",
)


def _version(name: str, alias: str | None = None) -> dict[str, object]:
    location = f"snow://cortex_extension/DB.EXT.VENDOR_PACK/versions/{name.lower()}/"
    return {"name": name, "alias": alias, "location": location, "files": [], "is_default": False, "certification": None}


def _consumed(port: FakeSnowflake, version: str = "VERSION$3", ref: str = "extension") -> list[Diagnostic]:
    port.grants["DB.EXT.VENDOR_PACK"] = (GrantRow("OWNERSHIP", "ROLE", "TEST_ROLE"),)
    skill = AgentSkill("vendor", "CORTEX_EXTENSION", "vendor_pack", version, ref=ref)
    return coded(observe(port, compile_agents(agent("sales_agent", skills=(skill,)))), "SST-VAL531")


def _with_versions(*listed: dict[str, object]) -> FakeSnowflake:
    port = FakeSnowflake()
    port.extensions["DB.EXT.VENDOR_PACK"] = {"type": "SKILL", "comment": None, "versions": list(listed), "live": None}
    return port


def test_sst_val531_fires_for_a_consumed_extension_snowflake_cannot_see() -> None:
    port = FakeSnowflake()
    port.fail("show_grants", _SCHEMA_MISSING)

    [diagnostic] = _consumed(port)

    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': 'DB.EXT.VENDOR_PACK' does not exist in target 'dev'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val531_fires_for_a_pinned_version_the_extension_does_not_list() -> None:
    port = _with_versions(_version("VERSION$1"), _version("VERSION$2", "v2"))

    [diagnostic] = _consumed(port)

    assert diagnostic.message == (
        "agent 'sales_agent': 'DB.EXT.VENDOR_PACK version VERSION$3' does not exist in target 'dev'"
    )


def test_sst_val531_silent_for_a_pinned_version_listed_by_name_or_alias() -> None:
    port = _with_versions(_version("VERSION$1"), _version("VERSION$2", "v2"), _version("VERSION$3"))

    assert _consumed(port) == []
    assert _consumed(port, version="V2") == []
    assert _consumed(port, version="LIVE") == []


def test_sst_val531_silent_when_the_read_fails_for_another_reason_or_the_extension_is_published_here() -> None:
    refused = FakeSnowflake()
    refused.refuse("show_grants")
    assert _consumed(refused) == []
    published = FakeSnowflake()
    published.fail("show_grants", _SCHEMA_MISSING)
    assert _consumed(published, ref="skill") == []
    unlisted = FakeSnowflake()
    unlisted.refuse("extension_versions")
    assert _consumed(unlisted) == []
