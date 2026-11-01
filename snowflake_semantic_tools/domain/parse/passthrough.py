"""Check a passthrough block: keys SST copies into a rendered spec without reading them."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.snowflake_spec import SpecNode, passthrough_split


def passthrough_diagnostics(
    artifact: str, block: Mapping[Any, object], rendered: SpecNode, *, subject: str, origin: Origin
) -> tuple[Diagnostic, ...]:
    """Report the keys of a passthrough block: each that overwrites what SST writes, then how many it adds.

    The block merges into what SST renders from `rendered`, mapping into mapping, so a key it
    adds inside a section SST writes, such as `models.fallback`, is added; a key SST writes
    itself, such as `models.orchestration` or `tools`, would be overwritten.

    Args:
        rendered: The table of what SST renders where the block is copied.

    Diagnostics:
        SST-PRS023: a key SST writes itself, by its dotted path; once per key, in block order.
        SST-PRS024: the keys the block adds, which are rendered unvalidated; once, with how many.
    """
    overwritten, added = passthrough_split(rendered, block)
    found = [D("SST-PRS023", artifact=artifact, key=key, subject=subject, origin=origin) for key in overwritten]
    if added:
        found.append(D("SST-PRS024", artifact=artifact, count=added, subject=subject, origin=origin))
    return tuple(found)
