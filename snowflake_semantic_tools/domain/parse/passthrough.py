"""Check a passthrough block: keys SST copies into a rendered spec without reading them."""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin

# The top-level keys SST renders into an agent's specification.
AGENT_SPEC_KEYS = frozenset(("models", "orchestration", "instructions", "tools", "tool_resources", "skills"))
# The keys SST renders into each agent tool's `tool_spec`.
TOOL_SPEC_KEYS = frozenset(("type", "name", "description", "input_schema"))


def passthrough_diagnostics(
    artifact: str, block: Mapping[object, object], rendered: frozenset[str], *, subject: str, origin: Origin
) -> tuple[Diagnostic, ...]:
    """Report the keys of a passthrough block: each SST already renders, then how many it does not.

    Args:
        rendered: The keys SST renders where the block is copied.

    Diagnostics:
        SST-PRS023: a key is one SST renders itself; once per key, in block order.
        SST-PRS024: the other keys, which are rendered unvalidated; once, with how many.
    """
    collisions = [str(key) for key in block if str(key) in rendered]
    found = [D("SST-PRS023", artifact=artifact, key=key, subject=subject, origin=origin) for key in collisions]
    rest = len(block) - len(collisions)
    if rest:
        found.append(D("SST-PRS024", artifact=artifact, count=rest, subject=subject, origin=origin))
    return tuple(found)
