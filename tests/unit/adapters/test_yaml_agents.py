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
spec:
  models:
    orchestration: model
  instructions:
    orchestration: "{{ file('instructions.md') }}"
    sample_questions:
      - question: What is revenue?
  tools:
    - type: cortex_analyst_text_to_sql
      semantic_view: "{{ semantic_view('sales') }}"
      description: Use for sales. Do not use for support.
alias: promoted
evals:
  dataset: evals/dataset.yml
  config: evals/config.yml
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
    assert agent.evals.dataset == "agents/sales/evals/dataset.yml"
    assert agent.evals.config == "agents/sales/evals/config.yml"


def test_agent_loader_reports_eval_pointer_escape(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "bad"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        """
name: bad
evals:
  dataset: ../../../outside.yml
  config: evals/config.yml
""".strip(),
        encoding="utf-8",
    )
    agents, diagnostics = load_agents(tmp_path)
    assert len(agents) == 1
    assert agents[0].evals is not None
    assert agents[0].evals.dataset is None
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-REF027"]


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
    assert {diagnostic.code for diagnostic in diagnostics} == {"SST-LOD018", "SST-PRS118"}


# (YAML after `name: typed`, the field reported, what it expects, what it found)
WRONG_TYPES = [
    pytest.param("spec:\n  tools: 5\n", "spec.tools", "a list", "int", id="tools"),
    pytest.param("spec:\n  skills: {a: 1}\n", "spec.skills", "a list", "dict", id="skills"),
    pytest.param("tags: 5\n", "tags", "a list", "int", id="tags"),
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
        ("SST-PRS122", {"file": "agents/sales/instructions.md", "offset": 6})
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
