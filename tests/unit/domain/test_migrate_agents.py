"""`sst migrate refs` for agents and eval files."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.agents import (
    agent_references,
    agent_structure,
    eval_structure,
)
from snowflake_semantic_tools.domain.migrate.tools import ToolInfo
from tests.helpers.migrate_outline import applied, lines, outline

INDEX = {
    "docs": ToolInfo("docs", "cortex_search_service", True, "docs_model"),
    "remote": ToolInfo("remote", "cortex_search_service", False, relations=("DB.S.REMOTE",)),
    "loose": ToolInfo("loose", "cortex_search_service", True),
    "files": ToolInfo("files", "stage", True),
}

AGENT = """name: a
tags:
  - name: cost_center
    value: x
spec:
  models:
    orchestration: m
  tools:
    - name: partner
      type: agent_toolset
    - name: kept
      type: agent_toolset
      agent: "{{ agent('b') }}"
    - {name: inline, type: agent_toolset}
    - name: docs
      type: cortex_search
      search_service: "{{ tool('docs') }}"
      id_column: doc_id
      title_column: "{{ ref('docs_model', 'title') }}"
      stage_path: "@X.Y.FILES/pdf/"
      columns_and_descriptions:
        doc_id:
          description: Id.
    - name: remote
      type: cortex_search
      search_service: "{{ tool('remote') }}"
      id_column: rid
    - name: loose
      type: cortex_search
      search_service: "{{ tool('loose') }}"
      id_column: lid
    - name: unknown
      type: cortex_search
      search_service: "{{ tool('nope') }}"
      id_column: uid
    - name: broken
      type: cortex_search
      search_service: "{{ broken( }}"
    - type: generic
  skills:
    - name: member
      source:
        type: plugin
        path: "{{ plugin('kit') }}"
    - name: listed
      source:
        type: extension
        path: "{{ extension('x') }}"
    - name: broken
      source:
        type: plugin
        path: "{{ broken( }}"
alias: promoted
"""


def test_agent_references() -> None:
    root = outline(AGENT)
    changes = agent_references(root, lines(AGENT), INDEX)
    text = applied(AGENT, changes)
    assert "  - name: \"{{ tag('cost_center') }}\"\n" in text
    assert "    - name: partner\n      identifier: \"{{ tool('partner') }}\"\n      type: agent_toolset\n" in text
    assert "      id_column: \"{{ ref('docs_model', 'doc_id') }}\"\n" in text
    assert "      stage_path: \"{{ tool('files') }}/pdf/\"\n" in text
    assert "      columns:\n        - column: \"{{ ref('docs_model', 'doc_id') }}\"\n" in text
    assert "      id_column: rid\n" in text
    assert "    - name: \"{{ skill('member') }}\"\n" in text and "    - name: listed\n" in text
    assert [(item.text, item.reason) for item in changes.untouched] == [
        ("inline", "a flow mapping: add identifier: by hand"),
        ("lid", "the member's on: names no model to ref the column of"),
        ("id_column", "the tool's search_service names no tool member"),
    ]


def test_agent_settings_move_under_config() -> None:
    root = outline(AGENT)
    text = applied(AGENT, agent_structure(lines(AGENT), root))
    assert text.startswith("name: a\nconfig:\n  tags:\n")
    assert "  orchestration_model: m\n  alias: promoted\nspec:\n  tools:\n" in text


EVAL = """agent: a
name: c
agent_version: committed
run:
  variant: sweep
  retention:
    audit: [ci]
    decision: [sweep]
    decision_window_days: 30
  tier: blocking
"""


def test_eval_files_lose_agent_and_move_settings() -> None:
    text = applied(EVAL, eval_structure(lines(EVAL), outline(EVAL)))
    assert (
        text == "name: c\nconfig:\n  agent_version: committed\n  retention: decision\n  eval_tier: blocking\n"
        "run:\n  variant: sweep\n"
    )
    unlisted = EVAL.replace("variant: sweep", "variant: other")
    assert "retention" not in applied(unlisted, eval_structure(lines(unlisted), outline(unlisted)))
    novariant = "run:\n  retention:\n    audit: [ci]\n  label: x\n"
    assert applied(novariant, eval_structure(lines(novariant), outline(novariant))) == "run:\n  label: x\n"
    plain = "name: d\nrun:\n  retention: audit\n"
    assert eval_structure(lines(plain), outline(plain)).edits == []
