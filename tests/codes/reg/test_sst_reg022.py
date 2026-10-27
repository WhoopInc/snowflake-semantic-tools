"""SST-REG022: an artifact type other than semantic_view declares member types.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, member, refused


def test_sst_reg022_fires() -> None:
    agent = artifact("agent", member_types=("metric",))
    refused(
        lambda: freeze((agent,), (member("metric", "agent"),)),
        "SST-REG022",
        "artifact type 'agent' declares member_types ['metric']; only semantic_view may",
    )


def test_sst_reg022_silent() -> None:
    view = artifact("semantic_view", member_types=("metric",))
    assert freeze((view,), (member("metric"),)).artifacts["semantic_view"].member_types == ("metric",)
