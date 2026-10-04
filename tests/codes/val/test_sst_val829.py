"""SST-VAL829: a skill's live catalog version and its live Desktop stage tree hold different files.

The catalog bundle is flattened and the stage tree nested, so the two differ by renames by
design. A deploy that published one channel and not the other leaves a difference renames
do not explain, and the next plan says so.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.lifecycle.channels import channel_divergence
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.publications import (
    both_channels_published,
)


def test_sst_val829_fires() -> None:
    port, result = both_channels_published()
    [stage_skill] = [path for path in port.stage_files if path.endswith("/month-close/SKILL.md") and "PROFILES" in path]
    # The stage channel holds a file the catalog never published.
    port.stage_files.add(stage_skill.replace("SKILL.md", "reference/leftover.md"))
    [diagnostic] = channel_divergence(port, result)
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL829", Severity.WARNING)
    assert diagnostic.message == "skill 'month-close': catalog and stage hashes diverge by 1 file(s)"
    assert diagnostic.subject == "skill:month-close"


def test_sst_val829_silent() -> None:
    # Flattening renames reference/steps.md in the catalog, which explains that difference.
    port, result = both_channels_published()
    assert channel_divergence(port, result) == ()
