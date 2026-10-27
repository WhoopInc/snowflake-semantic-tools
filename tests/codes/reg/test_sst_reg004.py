"""SST-REG004: two artifact types share a DDL position, or two members of one owner a clause position.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, member, refused


def test_sst_reg004_fires() -> None:
    refused(
        lambda: freeze((artifact("semantic_view"), artifact("tool"))),
        "SST-REG004",
        "position 100 is claimed by ['semantic_view', 'tool']",
    )


def test_sst_reg004_fires_for_a_clause_position() -> None:
    view = artifact("semantic_view", member_types=("metric", "filter"))
    refused(
        lambda: freeze((view,), (member("metric"), member("filter"))),
        "SST-REG004",
        "position 10 is claimed by ['metric', 'filter']",
    )


def test_sst_reg004_silent() -> None:
    assert len(freeze((artifact("semantic_view"), artifact("tool", 200))).artifacts) == 2
