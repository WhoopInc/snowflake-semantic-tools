"""`load_cowork_profiles` reads `cowork_profile.yml` files, a Snowflake private preview artifact."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.cowork_profiles import load_cowork_profiles
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentProfile, AgentSkill
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileTool, CoworkSubagent
from tests.helpers.diagnostic_filters import codes, only

FILE = "cowork_profiles/company/everyone/cowork_profile.yml"
ONE_SUBAGENT = "spec:\n  subagents:\n    - agent: \"{{ agent('sales') }}\"\n"


def _write(tmp_path: Path, text: str, file: str = FILE) -> Path:
    path = tmp_path / file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_a_complete_profile_reads_every_field_and_resolves_its_settings(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
name: Everyone
comment: The company's front door.
display:
  display_name: Everyone
  avatar: compass
  color: blue
spec:
  instructions:
    orchestration: "{{ file('routing.md') }}"
    response: Answer briefly.
    sample_questions:
      - question: Who sold the most?
  subagents:
    - agent: "{{ agent('sales') }}"
      display_name: Sales
      description: Revenue and pipeline.
      sample_questions:
        - question: What closed last week?
    - agent: "{{ tool('support_desk') }}"
      sample_questions: []
  tools:
    - type: web_search
      name: search_the_web
  skills:
    - name: month-close
      source:
        type: snowflake
        path: "{{ skill('month-close') }}"
  passthrough:
    experimental: {}
config:
  alias: live_profile
deprecated: true
meta:
  owner: analytics
""".strip(),
    )
    (path.parent / "routing.md").write_text("Send revenue questions to Sales.\n", encoding="utf-8")
    config = {"cowork_profiles": {"+database": "SNOWFLAKE_INTELLIGENCE", "company": {"+schema": "PROFILES"}}}
    profiles, diagnostics = load_cowork_profiles(tmp_path, config=config)
    assert codes(diagnostics) == ["SST-PRS024"]
    [profile] = profiles
    assert (profile.name, profile.key, profile.comment) == (
        "Everyone",
        "cowork_profile:everyone",
        "The company's front door.",
    )
    assert profile.display == AgentProfile("Everyone", "compass", "blue")
    assert profile.source_files == (FILE, "cowork_profiles/company/everyone/routing.md")
    assert (profile.orchestration_instructions, profile.response_instructions) == (
        "Send revenue questions to Sales.",
        "Answer briefly.",
    )
    assert profile.sample_questions == ("Who sold the most?",)
    assert profile.subagents == (
        CoworkSubagent(
            "agent", "sales", Origin(FILE, 14, 7), "Sales", "Revenue and pipeline.", ("What closed last week?",)
        ),
        CoworkSubagent("tool", "support_desk", Origin(FILE, 19, 7), sample_questions=()),
    )
    assert profile.tools == (CoworkProfileTool("web_search", "search_the_web", Origin(FILE, 22, 7)),)
    assert profile.skills == (AgentSkill("month-close", "snowflake", "month-close", "", ref="skill"),)
    assert (dict(profile.meta), dict(profile.passthrough), profile.deprecated) == (
        {"owner": "analytics"},
        {"experimental": {}},
        True,
    )
    assert (profile.folder, profile.enabled) == (("company", "everyone"), True)
    assert {key: (item.value, item.source) for key, item in profile.settings.items()} == {
        "database": ("SNOWFLAKE_INTELLIGENCE", "root"),
        "schema": ("PROFILES", "route:company"),
        "alias": ("live_profile", "config"),
    }


def test_a_missing_directory_holds_no_profiles_and_a_route_must_name_a_folder(tmp_path: Path) -> None:
    assert load_cowork_profiles(tmp_path) == ((), ())
    [diagnostic] = load_cowork_profiles(tmp_path, config={"cowork_profiles": {"sales": {"+schema": "S"}}})[1]
    assert (diagnostic.code, diagnostic.subject) == ("SST-CFG041", "config_route:sales")


def test_a_disabled_profile_is_kept_and_not_checked(tmp_path: Path) -> None:
    _write(tmp_path, "name: everyone\n")
    profiles, diagnostics = load_cowork_profiles(tmp_path, config={"cowork_profiles": {"+enabled": False}})
    assert [(profile.name, profile.enabled) for profile in profiles] == [("everyone", False)]
    assert diagnostics == ()


def test_a_folder_with_both_spellings_reads_neither(tmp_path: Path) -> None:
    _write(tmp_path, f"name: everyone\n{ONE_SUBAGENT}")
    _write(tmp_path, f"name: everyone_too\n{ONE_SUBAGENT}", FILE.replace(".yml", ".yaml"))
    profiles, diagnostics = load_cowork_profiles(tmp_path)
    diagnostic = only(diagnostics, "SST-VAL801")
    assert diagnostic.message == (
        "'cowork_profiles/company/everyone': CoWork profile folder has both cowork_profile.yml and cowork_profile.yaml"
    )
    assert profiles == ()


@pytest.mark.parametrize("spelling", ["cowork_profile.yml", "cowork_profile.yaml"])
def test_either_spelling_is_read(tmp_path: Path, spelling: str) -> None:
    _write(tmp_path, f"name: everyone\n{ONE_SUBAGENT}", f"cowork_profiles/everyone/{spelling}")
    profiles, diagnostics = load_cowork_profiles(tmp_path)
    assert ([profile.name for profile in profiles], diagnostics) == (["everyone"], ())


def test_an_unknown_key_is_reported_where_it_is_written(tmp_path: Path) -> None:
    _write(tmp_path, f"name: everyone\nprofile:\n  color: red\n{ONE_SUBAGENT}      descripton: Sales.\n")
    _, diagnostics = load_cowork_profiles(tmp_path)
    # Each is placed where its value starts.
    assert [(item.code, item.origin) for item in diagnostics] == [
        ("SST-PRS004", Origin(FILE, 3, 3)),
        ("SST-PRS022", Origin(FILE, 7, 19)),
    ]
    assert "spec.subagents[0].descripton" in diagnostics[1].message


def test_a_profile_without_a_usable_name_is_left_out(tmp_path: Path) -> None:
    _write(tmp_path, ONE_SUBAGENT)
    _write(tmp_path, f"name: '1 bad'\n{ONE_SUBAGENT}", "cowork_profiles/bad/cowork_profile.yml")
    profiles, diagnostics = load_cowork_profiles(tmp_path)
    assert profiles == ()
    assert codes(diagnostics) == ["SST-PRS005", "SST-PRS002"]


def test_a_subagent_that_names_no_agent_is_left_out(tmp_path: Path) -> None:
    _write(
        tmp_path,
        """
name: everyone
spec:
  subagents:
    - sales
    - display_name: Nobody
    - agent: sales
    - agent: "{{ agent('sales') }}"
      display_name: "{{ var('label') }}"
""".strip(),
    )
    [profile], diagnostics = load_cowork_profiles(tmp_path)
    assert codes(diagnostics) == ["SST-PRS018", "SST-PRS002", "SST-REF050", "SST-REF008"]
    assert diagnostics[1].message == f"{FILE}: required field 'spec.subagents[1].agent' is missing"
    assert [(item.name, item.origin) for item in profile.subagents] == [("sales", Origin(FILE, 7, 7))]


def test_a_tool_that_is_not_a_typed_mapping_is_left_out(tmp_path: Path) -> None:
    _write(tmp_path, f"name: everyone\n{ONE_SUBAGENT}  tools:\n    - web_search\n    - name: search\n")
    [profile], diagnostics = load_cowork_profiles(tmp_path)
    assert codes(diagnostics) == ["SST-PRS018", "SST-PRS002"]
    assert profile.tools == ()


def test_a_passthrough_key_sst_renders_is_reported(tmp_path: Path) -> None:
    _write(tmp_path, f"name: everyone\n{ONE_SUBAGENT}  passthrough:\n    tools: []\n")
    _, diagnostics = load_cowork_profiles(tmp_path)
    diagnostic = only(diagnostics, "SST-PRS023")
    assert diagnostic.subject == "cowork_profile:everyone"
    assert "'tools'" in diagnostic.message


def test_a_config_key_the_block_does_not_take_is_reported(tmp_path: Path) -> None:
    _write(tmp_path, f"name: everyone\n{ONE_SUBAGENT}config:\n  warehouse: WH\n")
    _, diagnostics = load_cowork_profiles(tmp_path)
    diagnostic = only(diagnostics, "SST-CFG053")
    assert (diagnostic.origin, diagnostic.subject) == (Origin(FILE, 6, 14), "cowork_profile:everyone")
