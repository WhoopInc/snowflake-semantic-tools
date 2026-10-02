"""SST-REG020: the resolver set and the registry's reference functions disagree.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.registry import NON_ARTIFACT_FUNCTIONS, SEMANTIC_REGISTRY, build_registry
from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg020_fires() -> None:
    functions = frozenset({"semantic_view", "dashboard"})
    refused(
        lambda: build_registry((artifact("semantic_view"),), (), functions=functions),
        "SST-REG020",
        "'dashboard' -- a resolver addresses no registered type",
    )


def test_sst_reg020_fires_for_a_type_no_resolver_handles() -> None:
    refused(
        lambda: build_registry((artifact("semantic_view"),), (), functions=NON_ARTIFACT_FUNCTIONS),
        "SST-REG020",
        "'semantic_view' -- no resolver handles the type's function",
    )


def test_sst_reg020_fires_for_a_type_with_no_ref_function() -> None:
    refused(
        lambda: freeze((artifact("semantic_view"), artifact("tool", 200, ref_function=None))),
        "SST-REG020",
        "'tool' -- the type has no ref_function",
    )


def test_sst_reg020_silent() -> None:
    # An eval and a profile are leaves, deliberately unreferenced.
    assert freeze((artifact("semantic_view"), artifact("eval", 200, ref_function=None))).artifacts["eval"]
    assert SEMANTIC_REGISTRY.artifacts["profile"].ref_function is None
