"""SST-REG005: two types claim one template reference function.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, member, refused


def test_sst_reg005_fires() -> None:
    view = artifact("semantic_view", ref_function="metric", member_types=("metric",))
    refused(
        lambda: freeze((view,), (member("metric", ref_function="metric"),)),
        "SST-REG005",
        "ref_function 'metric' is claimed twice",
    )


def test_sst_reg005_silent() -> None:
    view = artifact("semantic_view", member_types=("metric",))
    assert freeze((view,), (member("metric", ref_function="metric"),)).members["metric"].ref_function == "metric"
