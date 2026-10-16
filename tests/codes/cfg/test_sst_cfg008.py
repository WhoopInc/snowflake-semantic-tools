"""SST-CFG008: a value has the right type and is outside its allowed domain."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config, validated_config
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
