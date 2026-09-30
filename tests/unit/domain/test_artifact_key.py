"""Artifact keys are `<kind>:<name>`, built and split without normalizing either part."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key


@pytest.mark.parametrize(
    ("kind", "name", "key"),
    [
        ("semantic_view", "jaffle_menu", "semantic_view:jaffle_menu"),
        ("metric", "order_count", "metric:order_count"),
        ("tool", "docs_search", "tool:docs_search"),
        ("eval", "sales", "eval:sales"),
        # Persisted keys keep the caller's spelling: casefolding is the caller's to apply.
        ("skill", "Mixed_Case", "skill:Mixed_Case"),
        ("command", "sql/check:v2", "command:sql/check:v2"),
        ("agent", "", "agent:"),
    ],
)
def test_a_key_is_the_kind_a_colon_and_the_name_as_given(kind: str, name: str, key: str) -> None:
    assert artifact_key(kind, name) == key
    assert split_artifact_key(key) == (kind, name)


def test_a_name_holding_a_colon_splits_at_the_first_colon_only() -> None:
    assert split_artifact_key("tool:group:member") == ("tool", "group:member")
    assert split_artifact_key(":name") == ("", "name")


@pytest.mark.parametrize("key", ["sales", ""])
def test_a_key_without_a_colon_is_refused_by_name(key: str) -> None:
    with pytest.raises(ValueError, match=f"artifact key {key!r} has no ':'"):
        split_artifact_key(key)
