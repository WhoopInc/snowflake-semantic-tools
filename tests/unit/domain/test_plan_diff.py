"""Two states of a project compared: the four classes `state:` selects, and what `sst diff` lists."""

from __future__ import annotations

from snowflake_semantic_tools.domain.plan.diff import ArtifactState, compare_states, member_differences, state_classes
from snowflake_semantic_tools.domain.plan.member_diff import Definition, Member


def test_state_classes_name_new_modified_unmodified_and_orphaned() -> None:
    classes = state_classes({"a": "1", "b": "2", "c": "3"}, {"b": "2", "c": "9", "d": "4"})
    assert classes == {
        "new": frozenset({"a"}),
        "modified": frozenset({"c"}),
        "unmodified": frozenset({"b"}),
        "orphaned": frozenset({"d"}),
    }


def test_compare_names_only_the_fields_both_states_record() -> None:
    before = {"v:x": ArtifactState("1", "DB.S.X", ""), "v:y": ArtifactState("1", "DB.S.Y", "m")}
    after = {"v:x": ArtifactState("2", "DB.S.Z", "m"), "v:y": ArtifactState("1", "DB.S.Y", "n")}
    [modified] = compare_states(before, after)
    assert (modified.key, modified.status, modified.properties) == ("v:x", "modified", ("fingerprint", "target"))
    assert (modified.artifact_type, modified.name) == ("v", "x")


def test_new_and_orphaned_artifacts_carry_only_the_side_that_holds_them() -> None:
    new, orphaned = compare_states({"v:old": ArtifactState("1")}, {"v:new": ArtifactState("2")})
    assert (new.key, new.status, new.before, new.after) == ("v:new", "new", None, ArtifactState("2"))
    assert (orphaned.status, orphaned.after, orphaned.properties) == ("orphaned", None, ())
    assert compare_states({"v:same": ArtifactState("1")}, {"v:same": ArtifactState("1", "DB.S.X")}) == ()


def test_a_modified_artifact_carries_its_member_diff_and_across_targets_needs_one() -> None:
    before = {"v:a": ArtifactState("1"), "v:b": ArtifactState("1"), "v:c": ArtifactState("1")}
    after = {"v:a": ArtifactState("2"), "v:b": ArtifactState("2"), "v:c": ArtifactState("2"), "v:d": ArtifactState("3")}
    same = Definition((Member("metric", "m", (("comment", "x"),)),))
    edited = Definition((Member("metric", "m", (("comment", "y"),)),))
    differences = compare_states(before, after)
    defined = {"v:a": same, "v:b": same}
    changed = {"v:a": same, "v:b": edited}
    within = member_differences(differences, defined, changed, across_targets=False)
    assert [(item.key, item.member_diff is None, bool(item.member_diff)) for item in within] == [
        ("v:a", False, False),
        ("v:b", False, True),
        ("v:c", True, False),
        ("v:d", True, False),
    ]
    # Across targets, members that agree are no difference; one compared by fingerprint alone still is.
    across = member_differences(differences, defined, changed, across_targets=True)
    assert [item.key for item in across] == ["v:b", "v:c", "v:d"]
