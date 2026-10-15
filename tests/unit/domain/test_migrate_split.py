"""`sst migrate split`: one file per listed member, and tool group members in their group's folder."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.split import split_semantic, split_tools
from tests.helpers.migrate_outline import lines, outline

METRICS = """---
# The metrics file.
snowflake_metrics:

  # Section comment.

  # About a.
  - name: a
    expr: x
    # Inside a.
    synonyms:
      - aa

  - {name: b, expr: y}
  -
    name: c
    expr: z
# Trailing.
"""


def test_each_listed_member_gets_a_file_under_its_singular_root() -> None:
    plan = split_semantic(lines(METRICS), outline(METRICS), "semantic_models/metrics/metrics.yml")
    assert plan is not None and plan.refused is None and plan.routes == ()
    files = {item.path: item.text for item in plan.files}
    assert files == {
        "semantic_models/metrics/a.yml": (
            "---\n# The metrics file.\n# Section comment.\n# About a.\n"
            "metric:\n  name: a\n  expr: x\n  # Inside a.\n  synonyms:\n    - aa\n"
        ),
        "semantic_models/metrics/b.yml": "---\nmetric: {name: b, expr: y}\n",
        "semantic_models/metrics/c.yml": "---\nmetric:\n  name: c\n  expr: z\n# Trailing.\n",
    }
    assert [item.name for item in plan.files] == ["a", "b", "c"]


def test_a_file_that_cannot_split_is_refused_or_skipped() -> None:
    def plan(text: str) -> object:
        result = split_semantic(lines(text), outline(text), "m/m.yml")
        return None if result is None else result.refused

    assert plan("metric:\n  name: a\n") is None
    assert plan("snowflake_metrics:\n  - name: a\nother: 1\n") == "it holds other beside its member lists"
    assert plan("snowflake_metrics:\n  - expr: x\n") == "an entry of snowflake_metrics is not a mapping with a name"
    assert plan("snowflake_metrics:\n  - x\n") == "an entry of snowflake_metrics is not a mapping with a name"
    assert plan("snowflake_metrics:\n  - name: a\nsnowflake_filters:\n  - name: a\n") == (
        "two members would both be written to m/a.yml"
    )
    flow = split_semantic(
        lines("snowflake_filters: [{name: f, expr: x}]\n"),
        outline("snowflake_filters: [{name: f, expr: x}]\n"),
        "f.yml",
    )
    assert flow is not None and flow.files[0].path == "f.yml" and flow.files[0].text == "filter: {name: f, expr: x}\n"


GROUPS = """# The tool groups.
tools:
  - group: platform
    description: |-
      Platform tools.

      Owned here.
    owner: analytics
    define:
      # About search.
      - name: search
        type: cortex_search_service
        meta:
          deprecated: false
      - name: proc
        type: procedure
        body: "{{ file('sql/proc.sql') }}"
        owner: someone
        meta:
          owner: someone
  - group: partner
    immutable: true
    owner: partners
    reference:
      - name: agent
        type: agent
        relations:
          dev: X.Y.Z
  - group: taken
    immutable: true
    define:
      - name: lone
        type: stage
        config:
          schema: S
      - name: other
        type: stage
  - name: loose
    type: stage
"""


def test_tool_groups_move_to_their_folder_with_owner_and_routes() -> None:
    plan = split_tools(lines(GROUPS), outline(GROUPS), "tools/all.yml", "tools", lambda folder: folder == "tools/taken")
    assert plan is not None and plan.refused is None
    files = {item.path: item.text for item in plan.files}
    assert files["tools/platform/search.yml"] == (
        "# The tool groups.\n# From tool group platform:\n#   Platform tools.\n#\n#   Owned here.\n# About search.\n"
        "tool:\n  name: search\n  type: cortex_search_service\n  meta:\n    deprecated: false\n    owner: analytics\n"
    )
    assert files["tools/platform/proc.yml"].endswith(
        "  body: \"{{ file('../sql/proc.sql') }}\"\n  owner: someone\n  meta:\n    owner: someone\n"
    )
    assert files["tools/partner/agent.yml"].endswith("    dev: X.Y.Z\n  meta:\n    owner: partners\n")
    assert files["tools/taken/lone.yml"].endswith("  config:\n    schema: S\n")
    assert files["tools/taken/other.yml"].endswith("  config:\n    immutable: true\n")
    assert files["tools/loose.yml"] == "tool:\n  name: loose\n  type: stage\n"
    assert plan.routes == (("partner",),)


def test_a_tools_file_that_cannot_split_is_refused_or_skipped() -> None:
    assert (
        split_tools(
            lines("tool:\n  name: t\n"), outline("tool:\n  name: t\n"), "tools/t.yml", "tools", set().__contains__
        )
        is None
    )
    text = "tools:\n  - name: t\ntool:\n  name: u\n"
    refused = split_tools(lines(text), outline(text), "tools/t.yml", "tools", set().__contains__)
    assert refused is not None and refused.refused == "it holds tool beside tools:"
    text = "tools:\n  - group: g\n    immutable: true\n    define:\n      - type: x\n"
    unnamed = split_tools(lines(text), outline(text), "tools/t.yml", "tools", set().__contains__)
    assert unnamed is not None and unnamed.refused is not None and unnamed.routes == ()


def test_a_group_in_a_subfolder_routes_by_its_path_and_keeps_a_body_on_two_lines() -> None:
    text = (
        "tools:\n  - group: g\n    immutable: true\n    owner: |-\n      two\n      lines\n"
        "    reference:\n      - name: r\n        type: stage\n"
    )
    plan = split_tools(lines(text), outline(text), "tools/sub/t.yml", "tools", set().__contains__)
    assert plan is not None and plan.routes == (("sub", "g"),)
    assert "owner" not in plan.files[0].text
    text = (
        "tools:\n  - group: g\n    define:\n      - name: r\n        type: procedure\n"
        '        body: "{{ broken( }}"\n        meta: {a: 1}\n'
    )
    plan = split_tools(lines(text), outline(text), "tools/t.yml", "tools", set().__contains__)
    assert plan is not None and "{{ broken( }}" in plan.files[0].text
    text = (
        "tools:\n  - group: g\n    owner: o\n    define:\n      - name: r\n        type: procedure\n"
        "        body: \"{{ file('a', 'b') }}\"\n        meta: {a: 1}\n"
    )
    plan = split_tools(lines(text), outline(text), "tools/t.yml", "tools", set().__contains__)
    assert plan is not None and plan.files[0].text.endswith("  body: \"{{ file('a', 'b') }}\"\n  meta: {a: 1}\n")
