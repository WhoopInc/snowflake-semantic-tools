"""How an editor names, groups and lays out every key of every authored file and of `sst_config.yml`.

The tables are one module per family of kinds, as the authored-file tables are; this module joins
them, keyed by a key's place as `REFERENCE_FIELDS` names it (`agent.spec.tools.name`), or, for
`sst_config.yml`, by the key's dotted path (`agents.+schema`). The JSON Schemas carry them as
`x-studio`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.presentation import agents, config, extensions, semantic
from snowflake_semantic_tools.domain.model.presentation.types import (
    Group,
    KindUi,
    ListLayout,
    ListUi,
    Section,
    Ui,
    Widget,
)

# Every file kind's presentation, by kind name.
KIND_UI: Mapping[str, KindUi] = MappingProxyType({**semantic.KINDS, **agents.KINDS, **extensions.KINDS})
# Every authored key's presentation, by its place.
FIELD_UI: Mapping[str, Ui] = MappingProxyType({**semantic.FIELDS, **agents.FIELDS, **extensions.FIELDS})
# Every list of mappings' presentation, by its place.
LIST_UI: Mapping[str, ListUi] = MappingProxyType({**semantic.LISTS, **agents.LISTS, **extensions.LISTS})
# Every `sst_config.yml` key's presentation, by its dotted path.
CONFIG_UI: Mapping[str, Ui] = MappingProxyType(config.CONFIG)
# The sections of `sst_config.yml`, in order.
CONFIG_SECTIONS: tuple[Section, ...] = config.SECTIONS

__all__ = [
    "CONFIG_SECTIONS",
    "CONFIG_UI",
    "FIELD_UI",
    "KIND_UI",
    "LIST_UI",
    "Group",
    "KindUi",
    "ListLayout",
    "ListUi",
    "Section",
    "Ui",
    "Widget",
]
