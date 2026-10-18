"""The definitions of an artifact made of files and rows: a skill, plugin, profile or eval."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.plan.definitions.composite import (
    eval_definition,
    extension_definition,
    file_text,
    files_members,
    payload_questions,
    profile_definition,
    tree_root,
)
from snowflake_semantic_tools.domain.plan.member_diff import ADDED, CHANGED, REMOVED, diff_definitions

_MARKER = "[sst:" + "a" * 64 + ":" + "b" * 64 + "]"


def test_a_file_compares_as_text_unless_it_is_not_utf8_text() -> None:
    assert file_text(b"# Title\n") == "# Title\n"
    assert file_text(b"\xff\xfe").startswith("(binary, 2 bytes, sha256 ")
    assert file_text(b"a\x00b").startswith("(binary, 3 bytes, sha256 ")
    assert [member.name for member in files_members({"b.md": b"", "a.md": b""}, under="x/")] == ["x/a.md", "x/b.md"]


def test_a_skill_edit_shows_as_the_file_it_changes_and_its_comment_without_a_marker() -> None:
    live = extension_definition({"SKILL.md": b"old\n", "ref.md": b"ref\n"}, f"Jaffle. {_MARKER}")
    desired = extension_definition({"SKILL.md": b"new\n", "ref.md": b"ref\n", "extra.md": b"x\n"}, "Jaffle.")
    # Files are listed by path, as the desired side holds them.
    assert [(item.op, item.label) for item in diff_definitions(live, desired)] == [
        (CHANGED, "file SKILL.md"),
        (ADDED, "file extra.md"),
    ]


def test_a_profile_compares_its_columns_without_digests_and_its_trees_by_file() -> None:
    pointer = "@DB.S.BUNDLES/skills/team/0123456789ab/"
    moved = "@DB.S.BUNDLES/skills/team/ba9876543210/"
    live = profile_definition(
        {"VERSION": "1-aaa", "SKILL_REPOS": [{"source": pointer}], "MODEL": "auto"},
        {"skills/team/": {"a/SKILL.md": b"old\n"}},
    )
    desired = profile_definition(
        {"VERSION": "1-bbb", "SKILL_REPOS": json.dumps([{"source": moved}]), "MODEL": "auto"},
        {"skills/team/": {"a/SKILL.md": b"new\n"}, "hooks/team/": {"guard.sh": b"exit 0\n"}},
    )
    # The version and the moved pointer are not changes; the files are.
    assert [(item.op, item.label) for item in diff_definitions(live, desired)] == [
        (ADDED, "file hooks/team/guard.sh"),
        (CHANGED, "file skills/team/a/SKILL.md"),
    ]
    assert [member.name for member in desired.members][:2] == ["model", "skill_repos"]


def test_a_pointer_names_the_tree_it_points_into_and_its_root() -> None:
    root = "@DB.S.BUNDLES/hooks/team/0123456789ab/"
    assert tree_root(f"{root}guard/guard.sh") == ("hooks/team/", root)
    assert tree_root(root) == ("hooks/team/", root)
    assert tree_root(root[:-1]) == ("hooks/team/", root)
    assert tree_root("@DB.S.BUNDLES/plugins/") is None


def test_an_eval_compares_its_config_and_each_question_by_its_text() -> None:
    payload = json.dumps(
        [
            {"input_query": "How many orders?", "ground_truth": {"ground_truth_output": "42"}},
            {"input_query": "Busiest store?", "ground_truth": {"ground_truth_output": "Philly"}},
        ]
    )
    desired = eval_definition("evaluation:\n  agent: A\n", payload_questions(payload))
    live = eval_definition(
        "evaluation:\n  agent: B\n",
        (("How many orders?", '{"ground_truth_output": "41"}'), ("Gone?", {"ground_truth_output": "x"})),
    )
    assert [(item.op, item.label) for item in diff_definitions(live, desired)] == [
        (CHANGED, "config yaml"),
        (CHANGED, "question How many orders?"),
        (ADDED, "question Busiest store?"),
        (REMOVED, "question Gone?"),
    ]
    # A question asked twice is two members; a ground truth that is plain text compares as it is.
    twice = eval_definition("", (("Q?", "plain"), ("Q?", '"quoted"')))
    assert [(member.name, member.properties) for member in twice.members[1:]] == [
        ("Q?", (("ground_truth", "plain"),)),
        ("Q?#2", (("ground_truth", "quoted"),)),
    ]


def test_an_eval_whose_questions_were_not_read_holds_its_config_alone() -> None:
    partial = eval_definition("a: 1\n", None)
    assert ([member.kind for member in partial.members], partial.kinds) == (["config"], frozenset(("config",)))
