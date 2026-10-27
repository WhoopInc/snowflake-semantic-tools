"""SST-REG003: two types claim one YAML root key.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, member, refused


def test_sst_reg003_fires() -> None:
    view = artifact("semantic_view", root_key="shared", member_types=("metric",))
    refused(
        lambda: freeze((view,), (member("metric", root_key="shared"),)),
        "SST-REG003",
        "root_key 'shared' is claimed by ['semantic_view', 'metric']",
    )


def test_sst_reg003_silent() -> None:
    view = artifact("semantic_view", member_types=("metric",))
    assert "metric" in freeze((view,), (member("metric"),)).members
