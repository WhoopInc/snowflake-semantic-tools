from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.agents import load_agents


def test_agent_loader_reads_complete_documents_and_sidecars(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "sales"
    root.mkdir(parents=True)
    (root / "instructions.md").write_text("Route sales questions to Sales.\n", encoding="utf-8")
    (root / "agent.yml").write_text(
        """
name: sales_agent
profile:
  display_name: Sales
config:
  orchestration_model: model
  alias: promoted
spec:
  instructions:
    orchestration: "{{ file('instructions.md') }}"
    sample_questions:
      - question: What is revenue?
  tools:
    - type: cortex_analyst_text_to_sql
      semantic_view: "{{ semantic_view('sales') }}"
      description: Use for sales. Do not use for support.
evals:
  dataset: "{{ eval_dataset('sales_questions') }}"
  config: "{{ eval_config('sales_run') }}"
""".strip(),
        encoding="utf-8",
    )
    agents, diagnostics = load_agents(tmp_path)
    assert diagnostics == ()
    assert len(agents) == 1
    agent = agents[0]
    assert agent.name == "sales_agent"
    assert agent.orchestration_instructions == "Route sales questions to Sales."
    assert agent.source_files == (
        "agents/sales/agent.yml",
        "agents/sales/instructions.md",
    )
    assert agent.tools[0].semantic_view == "sales"
    assert agent.evals is not None
    assert (agent.evals.dataset, agent.evals.config) == ("sales_questions", "sales_run")
    assert agent.evals.folder == "agents/sales/evals"
    assert {key: (item.value, item.source) for key, item in agent.settings.items()} == {
        "orchestration_model": ("model", "config"),
        "alias": ("promoted", "config"),
    }


def test_an_eval_file_path_is_refused_for_a_reference(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "bad"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        """
name: bad
evals:
  dataset: ../../../outside.yml
  config: "{{ eval_config('run') }}"
""".strip(),
        encoding="utf-8",
    )
    agents, diagnostics = load_agents(tmp_path)
    assert len(agents) == 1
    assert agents[0].evals is not None
    assert (agents[0].evals.dataset, agents[0].evals.config) == (None, "run")
    # A path is never read: the agent names its dataset by the name the file declares.
    [diagnostic] = diagnostics
    assert diagnostic.code == "SST-REF047"
    assert diagnostic.message == (
        "agents/bad/agent.yml: evals.dataset is '../../../outside.yml'; it takes one {{ eval_dataset('<name>') }} call"
    )


def test_agent_loader_reports_missing_sidecar_and_bad_sample_shape(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "bad"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        """
name: bad
spec:
  instructions:
    orchestration: "{{ file('missing.md') }}"
    sample_questions:
      - bare string
""".strip(),
        encoding="utf-8",
    )
    _, diagnostics = load_agents(tmp_path)
    assert {diagnostic.code for diagnostic in diagnostics} == {"SST-REF014", "SST-PRS118"}


# (YAML after `name: typed`, the field reported, what it expects, what it found)
WRONG_TYPES = [
    pytest.param("spec:\n  tools: 5\n", "spec.tools", "a list", "int", id="tools"),
    pytest.param("spec:\n  skills: {a: 1}\n", "spec.skills", "a list", "dict", id="skills"),
    pytest.param(
        "spec:\n  instructions:\n    sample_questions: What is revenue?\n",
        "spec.instructions.sample_questions",
        "a list",
        "str",
        id="sample-questions",
    ),
    pytest.param("meta: text\n", "meta", "a mapping", "str", id="meta"),
    pytest.param("spec:\n  passthrough: [1]\n", "spec.passthrough", "a mapping", "list", id="passthrough"),
    pytest.param(
        "spec:\n  tools:\n    - type: generic\n      filter: text\n",
        "tools[0].filter",
        "a mapping",
        "str",
        id="tool-filter",
    ),
    pytest.param(
        "spec:\n  tools:\n    - type: generic\n      input_schema: 3\n",
        "tools[0].input_schema",
        "a mapping",
        "int",
        id="tool-input-schema",
    ),
]


@pytest.mark.parametrize(("body", "field", "expected", "found"), WRONG_TYPES)
def test_a_field_of_the_wrong_type_is_reported_and_holds_the_agent_back(
    tmp_path: Path, body: str, field: str, expected: str, found: str
) -> None:
    root = tmp_path / "agents" / "typed"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text("name: typed\n" + body, encoding="utf-8")

    agents, diagnostics = load_agents(tmp_path)

    [diagnostic] = diagnostics
    assert diagnostic.code == "SST-PRS003"
    assert dict(diagnostic.context) == {
        "artifact": "agents/typed/agent.yml",
        "field": field,
        "expected": expected,
        "found": found,
    }
    # The error names the agent, so compile keeps it back rather than publish it without the field.
    assert diagnostic.subject == "agent:typed"
    assert [agent.name for agent in agents] == ["typed"]


def test_a_sidecar_that_is_not_utf8_is_reported_instead_of_raising(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "sales"
    root.mkdir(parents=True)
    (root / "instructions.md").write_bytes(b"Route \xff sales questions.\n")
    (root / "agent.yml").write_text(
        "name: sales\nspec:\n  instructions:\n    orchestration: \"{{ file('instructions.md') }}\"\n",
        encoding="utf-8",
    )

    agents, diagnostics = load_agents(tmp_path)

    assert [(item.code, dict(item.context)) for item in diagnostics] == [
        ("SST-LOD006", {"file": "agents/sales/instructions.md", "offset": 6})
    ]
    assert agents[0].orchestration_instructions is None
    assert agents[0].source_files == ("agents/sales/agent.yml",)


def test_each_tool_is_placed_where_its_entry_starts(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "placed"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        "name: placed\n"
        "spec:\n"
        "  tools:\n"
        "    - type: generic\n"
        "      name: first\n"
        "\n"
        "    - type: generic\n"
        "      name: second\n"
        "    - just text\n",
        encoding="utf-8",
    )

    agents, diagnostics = load_agents(tmp_path)

    # Lines 4 and 7, where each entry is written, not 1 and 2, its place in the list.
    assert [(tool.name, tool.origin.line, tool.origin.col) for tool in agents[0].tools] == [
        ("first", 4, 7),
        ("second", 7, 7),
    ]
    [not_a_mapping] = diagnostics
    assert not_a_mapping.code == "SST-PRS018"
    assert not_a_mapping.origin is not None
    assert (not_a_mapping.origin.file, not_a_mapping.origin.line) == ("agents/placed/agent.yml", 9)


def _agent_file(tmp_path: Path, text: str) -> Path:
    root = tmp_path / "agents" / "sales"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(text, encoding="utf-8")
    return tmp_path


def test_a_bare_string_instructions_block_is_refused_rather_than_dropped(tmp_path: Path) -> None:
    project = _agent_file(tmp_path, "name: sales_agent\nspec:\n  instructions: Route sales questions to Sales.\n")
    agents, diagnostics = load_agents(project)
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.subject) == ("SST-PRS003", "agent:sales_agent")
    assert "spec.instructions" in diagnostic.message
    assert agents[0].orchestration_instructions is None


def test_a_token_budget_documented_beside_it_is_marked_so(tmp_path: Path) -> None:
    text = (
        "name: sales_agent\n"
        "config:\n"
        "  budget_seconds: 60  # wall clock\n"
        "  # Bounds orchestration tokens only,\n"
        "  # not what the tools spend.\n"
        "  budget_tokens: 16000\n"
    )
    [documented], _ = load_agents(_agent_file(tmp_path / "a", text))
    assert documented.settings["budget_tokens"].value == 16000 and documented.budget_tokens_documented
    trailing = text.replace("  # Bounds orchestration tokens only,\n  # not what the tools spend.\n", "")
    trailing = trailing.replace("budget_tokens: 16000", "budget_tokens: 16000  # orchestration only")
    [inline], _ = load_agents(_agent_file(tmp_path / "b", trailing))
    assert inline.budget_tokens_documented
    bare = text.replace("  # Bounds orchestration tokens only,\n  # not what the tools spend.\n", "")
    [undocumented], _ = load_agents(_agent_file(tmp_path / "c", bare))
    assert not undocumented.budget_tokens_documented


def test_agents_in_grouping_folders_load_with_their_folder_and_routes_are_checked(tmp_path: Path) -> None:
    for folder in ("top", "finance/fin", "finance/restricted/sec"):
        (tmp_path / "agents" / folder).mkdir(parents=True)
        (tmp_path / "agents" / folder / "agent.yml").write_text(f"name: {folder.rsplit('/')[-1]}\n", encoding="utf-8")
    # A file directly in agents/ names no agent folder, as before.
    (tmp_path / "agents" / "agent.yml").write_text("name: stray\n", encoding="utf-8")
    config = {"agents": {"+schema": "A", "finance": {"restricted": {"+secure": True}, "missing": {}}}}

    agents, diagnostics = load_agents(tmp_path, config=config)

    assert [(agent.name, agent.folder) for agent in agents] == [
        ("fin", ("finance", "fin")),
        ("sec", ("finance", "restricted", "sec")),
        ("top", ("top",)),
    ]
    assert [(item.code, item.subject) for item in diagnostics] == [("SST-CFG041", "config_route:finance.missing")]
    assert load_agents(tmp_path)[1] == ()
    # Without an agents folder there are no agents, and every route names a missing folder.
    [missing] = load_agents(tmp_path, agents_dir="bots", config=config)[1]
    assert (missing.code, missing.subject) == ("SST-CFG041", "config_route:finance")


def test_an_agents_own_enabled_wins_over_its_routes_and_an_unset_one_takes_them(tmp_path: Path) -> None:
    for folder, extra in (
        ("retired/kept", "config:\n  enabled: true\n"),
        ("retired/gone", ""),
        ("live/dark", "config:\n  enabled: false\n"),
    ):
        (tmp_path / "agents" / folder).mkdir(parents=True)
        name = folder.rsplit("/")[-1]
        (tmp_path / "agents" / folder / "agent.yml").write_text(f"name: {name}\n{extra}", encoding="utf-8")
    config = {"agents": {"retired": {"+enabled": False}, "live": {"+enabled": True}}}

    agents, _ = load_agents(tmp_path, config=config)

    assert {agent.name: agent.enabled for agent in agents} == {"dark": False, "gone": False, "kept": True}
    # Without a configuration, an agent that sets no `enabled` is enabled.
    assert all(agent.enabled for agent in load_agents(tmp_path)[0] if agent.name != "dark")


def test_keys_an_agent_file_does_not_model_are_reported_and_a_near_miss_is_a_typo(tmp_path: Path) -> None:
    text = (
        "name: sales_agent\n"
        "owner: data\n"
        "profile:\n  display_name: Sales\n  colour: red\n"
        "config:\n  budget_seconds: 60\n  tags:\n    - {name: t, value: v, note: x}\n"
        "spec:\n"
        "  models:\n    orchestration: m\n"
        "  tools:\n"
        "    - type: cortex_analyst_text_to_sql\n"
        "      semantic_view: \"{{ semantic_view('sales') }}\"\n"
        "      description: Sales.\n"
        "      warehosue: WH\n"
        "  skills:\n"
        "    - name: s\n      source: {type: CORTEX_EXTENSION, path: x, pin: 1}\n"
        "meta:\n  anything: goes\n"
        "evals:\n  dataset: \"{{ eval_dataset('d') }}\"\n  config: \"{{ eval_config('c') }}\"\n  extra: 1\n"
    )
    agents, diagnostics = load_agents(_agent_file(tmp_path, text))

    assert len(agents) == 1
    found = sorted((item.code, item.message, item.subject) for item in diagnostics)
    file = "agents/sales/agent.yml"
    assert found == [
        ("SST-PRS004", f"{file}: unknown field 'config.tags[0].note'", "agent:sales_agent"),
        ("SST-PRS004", f"{file}: unknown field 'evals.extra'", "agent:sales_agent"),
        ("SST-PRS004", f"{file}: unknown field 'owner'", "agent:sales_agent"),
        ("SST-PRS004", f"{file}: unknown field 'spec.skills[0].source.pin'", "agent:sales_agent"),
        (
            "SST-PRS022",
            f"{file}: unknown field 'profile.colour'; did you mean 'profile.color'?",
            "agent:sales_agent",
        ),
        (
            "SST-PRS022",
            f"{file}: unknown field 'spec.tools[0].warehosue'; did you mean 'spec.tools[0].warehouse'?",
            "agent:sales_agent",
        ),
        (
            "SST-PRS126",
            "agent:sales_agent: 'spec.models' is no longer written: an agent sets its model under config:, "
            "as config.orchestration_model",
            "agent:sales_agent",
        ),
    ]
    lines = {item.message.split("'")[1]: item.origin.line for item in diagnostics if item.origin is not None}
    assert (lines["spec.tools[0].warehosue"], lines["owner"]) == (17, 2)


def test_an_instruction_that_is_not_a_string_is_refused_rather_than_dropped(tmp_path: Path) -> None:
    project = _agent_file(tmp_path, "name: sales_agent\nspec:\n  instructions:\n    response: [one, two]\n")

    agents, diagnostics = load_agents(project)

    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.subject) == ("SST-PRS003", "agent:sales_agent")
    assert diagnostic.message == "agents/sales/agent.yml: 'spec.instructions.response' expects string, found list"
    assert agents[0].response_instructions is None


def test_an_agent_file_that_cannot_be_read_is_reported_as_unreadable(tmp_path: Path) -> None:
    project = _agent_file(tmp_path, "name: sales_agent\n")
    path = project / "agents" / "sales" / "agent.yml"
    path.chmod(0)
    try:
        agents, diagnostics = load_agents(project)
    finally:
        path.chmod(0o644)

    assert agents == ()
    [diagnostic] = diagnostics
    assert diagnostic.code == "SST-LOD018"
    assert diagnostic.origin is not None and diagnostic.origin.file == "agents/sales/agent.yml"
