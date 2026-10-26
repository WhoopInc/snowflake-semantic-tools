"""`targets_by_kind`: the objects plan observes for each artifact type, from what renders and from state."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.plan_artifacts import targets_by_kind
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.artifact_builders import state


def test_a_state_entry_whose_name_does_not_parse_is_left_out_rather_than_failing_the_plan() -> None:
    gone = AppliedEntry("f" * 64, "OTHER.SCHEMA.GONE", "now", "run", "applied", "f" * 64, "m" * 64)
    broken = replace(gone, qualified_name="not a name")
    recorded = replace(state(), applied=MappingProxyType({"skill:gone": gone, "skill:broken": broken}))
    assert targets_by_kind((), recorded) == {"skill": (QualifiedName.parse("OTHER.SCHEMA.GONE"),)}
