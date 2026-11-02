"""`sst_config.yml` edited by key path: in place, in reference order, its comments kept."""

from __future__ import annotations

import pytest
import yaml

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.api.config_edit import edit_config, edit_target_value
from snowflake_semantic_tools.domain.resolve.config import render_config
from snowflake_semantic_tools.domain.resolve.target_value import parse_target_value
from tests.helpers.reference_project import FIXTURE

_TEXT = (FIXTURE / "sst_config.yml").read_text()

_SMALL = """\
# The project's settings.
vars:
  dev_targets: [scratch, studio]

agents:
  # Every agent publishes here.
  +schema: AGENTS
  sales:
    +database: SALES  # the sales team's
"""


def test_setting_a_key_in_place_changes_only_its_line() -> None:
    edited = edit_config(_SMALL, ("agents", "+schema"), "AGENT_VIEWS")
    assert edited == _SMALL.replace("+schema: AGENTS", "+schema: AGENT_VIEWS")


def test_a_new_key_goes_where_the_reference_orders_it() -> None:
    edited = edit_config(_SMALL, ("agents", "+database"), "ANALYTICS")
    assert "  # Every agent publishes here.\n  +database: ANALYTICS\n  +schema: AGENTS\n" in edited
    routed = edit_config(_SMALL, ("agents", "sales", "+schema"), "SALES_AGENTS")
    assert "    +database: SALES  # the sales team's\n    +schema: SALES_AGENTS\n" in routed


def test_a_missing_route_and_block_are_added() -> None:
    edited = edit_config(_SMALL, ("tools", "search", "+warehouse"), "SEARCH_WH")
    assert yaml.safe_load(edited)["tools"] == {"search": {"+warehouse": "SEARCH_WH"}}
    # The reference lists tools: before agents:, so the block lands between vars: and agents:.
    assert edited.index("tools:") < edited.index("agents:")
    kept = [line for line in edited.splitlines() if line and line in _SMALL.splitlines()]
    assert kept == [line for line in _SMALL.splitlines() if line]


def test_removing_a_key_removes_each_mapping_it_leaves_empty() -> None:
    edited = edit_config(_SMALL, ("agents", "sales", "+database"), None)
    assert yaml.safe_load(edited)["agents"] == {"+schema": "AGENTS"}
    emptied = edit_config(edited, ("agents", "+schema"), None)
    assert yaml.safe_load(emptied) == {"vars": {"dev_targets": ["scratch", "studio"]}}
    assert edit_config(_SMALL, ("agents", "nowhere", "+schema"), None) == _SMALL


def test_values_are_written_as_their_kind_and_text_quoted_when_another_loader_would_misread_it() -> None:
    edited = edit_config(_SMALL, ("validation", "strict"), True)
    edited = edit_config(edited, ("session", "threads"), 4)
    edited = edit_config(edited, ("validation", "exclude_dirs"), ["a/*", "b"])
    edited = edit_config(edited, ("agents", "+alias"), "no")
    tree = yaml.safe_load(edited)
    assert tree["validation"] == {"strict": True, "exclude_dirs": ["a/*", "b"]}
    assert tree["session"] == {"threads": 4}
    assert "+alias: 'no'" in edited


def test_the_reference_project_s_file_keeps_every_other_byte() -> None:
    edited = edit_config(_TEXT, ("state", "+table"), "SST_STATE_V2")
    before, after = _TEXT.splitlines(), edited.splitlines()
    assert len(before) == len(after)
    assert [pair for pair in zip(before, after, strict=True) if pair[0] != pair[1]] == [
        (line, line.replace(line.split(":", 1)[1].strip(), "SST_STATE_V2"))
        for line in before
        if line.strip().startswith("+table:")
    ]


def test_a_per_target_value_is_written_in_the_grammar_a_run_resolves() -> None:
    value = parse_target_value("{{ '' if target.name in var('dev_targets') else 'STATE' }}")
    edited = edit_target_value(_SMALL, ("state", "+schema"), value)
    tree = yaml.safe_load(edited)
    for target, expected in (("prod", "STATE"), ("scratch", "")):
        rendered, diagnostics = render_config(tree, target_name=target, file="sst_config.yml")
        assert not diagnostics
        assert rendered["state"]["+schema"] == expected


def test_an_empty_file_starts_one() -> None:
    assert yaml.safe_load(edit_config("", ("agents", "+schema"), "AGENTS")) == {"agents": {"+schema": "AGENTS"}}


def test_a_path_through_a_value_or_a_file_that_is_no_mapping_is_refused() -> None:
    with pytest.raises(ProjectError, match=r"agents\.\+schema is not a mapping"):
        edit_config(_SMALL, ("agents", "+schema", "deeper"), "x")
    with pytest.raises(ProjectError, match="the file's top is not a mapping"):
        edit_config("- a\n- b\n", ("agents", "+schema"), "x")
    with pytest.raises(ValueError, match="at least one key"):
        edit_config(_SMALL, (), "x")
    assert edit_config(_SMALL, ("agents", "+schema", "deeper"), None) == _SMALL
