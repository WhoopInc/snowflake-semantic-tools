"""SST-REG900: a type is registered after the registry is frozen.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.registry import RegistryBuilder
from tests.helpers.registry_types import artifact, refused


def test_sst_reg900_fires() -> None:
    builder = RegistryBuilder()
    builder.artifact(artifact("semantic_view"))
    builder.freeze(functions=frozenset({"semantic_view"}))
    refused(
        lambda: builder.artifact(artifact("tool", 200)),
        "SST-REG900",
        "registry mutated after freeze: artifact type tool after the registry was frozen",
    )


def test_sst_reg900_silent() -> None:
    builder = RegistryBuilder()
    builder.artifact(artifact("semantic_view"))
    builder.artifact(artifact("tool", 200))
    assert set(builder.freeze(functions=frozenset({"semantic_view", "tool"})).artifacts) == {"semantic_view", "tool"}
