"""SST-CFG008: a value has the right type and is outside its allowed domain."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.validate.config import validate_config, validated_config
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import AGENTS_SCHEMA, config_edited


def test_sst_cfg008_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"enrichment": {"distinct_limit": 0}}) if item.code == "SST-CFG008"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'enrichment.distinct_limit' value 0 is outside 1..1000"
    assert diagnostic.subject == "config:enrichment.distinct_limit"


def test_sst_cfg008_silent() -> None:
    assert "SST-CFG008" not in [item.code for item in validate_config({"enrichment": {"distinct_limit": 25}})]


def test_sst_cfg008_for_a_location_that_names_no_snowflake_object(tmp_path: Path) -> None:
    tree, [diagnostic] = validated_config({"agents": {"+schema": " ", "+database": "DB"}})
    assert diagnostic.message == "config key 'agents.+schema' value ' ' is outside Snowflake identifiers"
    assert tree == {"agents": {"+database": "DB"}}
    code, diagnostics = validate_json(config_edited(tmp_path, (AGENTS_SCHEMA, AGENTS_SCHEMA.replace("JAFFLE", '" "'))))
    assert code == 1 and "SST-CFG008" in [item["code"] for item in diagnostics]


def test_sst_cfg008_silent_for_a_location_that_names_one() -> None:
    names = {
        "+schema": '"lower case"',
        "+database": "{{ target.database }}",
        "+bundle_stage": "DB.SCH.STAGE",
    }
    plugins = {"+schema": "", "+bundle_stage": "{{ target.database }}.S.STAGE"}
    assert validate_config({"skills": names, "plugins": plugins, "state": {"+schema": "S$1"}}) == ()
    assert [item.code for item in validate_config({"skills": {"+bundle_stage": "A.B"}})] == ["SST-CFG008"]
    assert [item.code for item in validate_config({"skills": {"+database": "A.B"}})] == ["SST-CFG008"]


def test_sst_cfg008_for_an_agent_budget_or_timeout_below_one() -> None:
    # Snowflake stores a zero or negative budget verbatim and runs the agent as if none were set.
    root = {"agents": {"+budget_seconds": -1, "+budget_tokens": 0, "+query_timeout": 30}}
    found = [item for item in validate_config(root) if item.code == "SST-CFG008"]
    assert [item.message for item in found] == [
        "config key 'agents.+budget_seconds' value -1 is outside the integers from 1 up",
        "config key 'agents.+budget_tokens' value 0 is outside the integers from 1 up",
    ]
    own = item_config_diagnostics(
        "agents",
        {"budget_seconds": -5, "query_timeout": -30, "budget_tokens": 16000},
        subject="agent:zz",
        origin_of=lambda key: Origin("agents/zz/agent.yml"),
    )
    assert [(item.code, item.context["key"]) for item in own] == [
        ("SST-CFG008", "config.budget_seconds"),
        ("SST-CFG008", "config.query_timeout"),
    ]
    assert validate_config({"agents": {"+budget_seconds": 1, "+budget_tokens": 1, "+query_timeout": 1}}) == ()
