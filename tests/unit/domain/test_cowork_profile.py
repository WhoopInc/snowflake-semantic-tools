"""A CoWork profile's key, and the AGENT object type it shares (a Snowflake private preview)."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.cowork_profile import (
    COWORK_PROFILE_SCHEMA,
    COWORK_PROFILE_TOOLS,
    ROUTER_NAME,
    CoworkProfileModel,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from tests.helpers.registry_types import artifact, freeze

ORIGIN = Origin("cowork_profiles/everyone/cowork_profile.yml", 1, 1)


def test_a_profile_is_keyed_by_its_kind_and_casefolded_name() -> None:
    assert CoworkProfileModel("Everyone", ORIGIN, (ORIGIN.file,)).key == "cowork_profile:everyone"
    assert (ROUTER_NAME, COWORK_PROFILE_SCHEMA) == ("router", ("SNOWFLAKE_INTELLIGENCE", "PROFILES"))
    assert frozenset(("web_search", "code_execution")) == COWORK_PROFILE_TOOLS


def test_agents_and_cowork_profiles_share_the_agent_object_type_and_nothing_else_is_shared() -> None:
    assert SEMANTIC_REGISTRY.shared_object_types() == frozenset({"AGENT"})
    assert freeze((artifact("semantic_view"), artifact("agent", 300))).shared_object_types() == frozenset()
