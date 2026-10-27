"""SST-CFG029: a `var()` call, in a member or a configuration value, names no variable `vars:` declares."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import render_config
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import AGENTS_SCHEMA, config_edited, config_line
from tests.helpers.resolve_builders import resolved


def test_sst_cfg029_fires() -> None:
    value, (diagnostic,) = resolved("{{ var('missing') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG029", Severity.ERROR)
    assert diagnostic.message == "{{ var('missing') }} is not declared in config"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_cfg029_fires_in_a_configuration_value() -> None:
    tree, [diagnostic] = render_config(
        {"tags": {"default_prefix": "{{ var('gov') }}.TAGS"}}, target_name=None, file="sst_config.yml"
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG029", Severity.ERROR)
    assert diagnostic.message == "{{ var('gov') }} is not declared in config"
    assert diagnostic.subject == "config:tags.default_prefix"
    # Removed, so the key's reader sees it unset and reports nothing further.
    assert tree["tags"] == {}


def test_sst_cfg029_silent() -> None:
    value, diagnostics = resolved("{{ var('state') }}", METRIC_EXPR, variables={"state": "completed"})
    assert diagnostics == ()
    assert not value.poisoned
    tree, found = render_config(
        {"vars": {"gov": "GOVERNANCE"}, "tags": {"default_prefix": "{{ var('gov') }}.TAGS"}, "list": ["a", 1]},
        target_name=None,
        file="sst_config.yml",
    )
    assert (tree["tags"]["default_prefix"], tree["vars"], found) == ("GOVERNANCE.TAGS", {"gov": "GOVERNANCE"}, ())


def test_sst_cfg029_on_an_agents_location_is_reported_with_the_other_config_codes(tmp_path: Path) -> None:
    schema = AGENTS_SCHEMA.replace("JAFFLE", "\"{{ var('nope') }}\"")
    budget = ("  +budget_seconds: 120", "  +budget_seconds: \"{{ var('nope2') }}\"")
    code, diagnostics = validate_json(config_edited(tmp_path, (AGENTS_SCHEMA, schema), budget))
    found = {item["params"]["var"]: item for item in diagnostics if item["code"] == "SST-CFG029"}
    assert code == 1 and sorted(found) == ["nope", "nope2"]
    assert found["nope"]["location"]["line"] == config_line(tmp_path, "+schema: \"{{ var('nope')")
    assert found["nope2"]["location"]["line"] == config_line(tmp_path, "+budget_seconds:")
    # Removed once its template failed, the key is not then refused for its type too.
    assert "SST-CFG004" not in [item["code"] for item in diagnostics]


def test_sst_cfg029_on_a_skills_location(tmp_path: Path) -> None:
    edit = ("jaffle-operations:\n    +schema: JAFFLE", "jaffle-operations:\n    +schema: \"{{ var('nope') }}\"")
    code, diagnostics = validate_json(config_edited(tmp_path, edit))
    assert code == 1 and "SST-CFG029" in [item["code"] for item in diagnostics]
