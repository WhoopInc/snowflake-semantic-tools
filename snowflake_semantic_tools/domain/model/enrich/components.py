"""The parts of a column `sst enrich` fills, and how `--include` and `--force` resolve to them."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic


class Component(Enum):
    """One kind of metadata enrich can fill, named as `--include` and `--force` take it."""

    COLUMN_TYPES = "column-types"
    DATA_TYPES = "data-types"
    SAMPLE_VALUES = "sample-values"
    ENUMS = "enums"
    COLUMN_SYNONYMS = "column-synonyms"
    TABLE_SYNONYMS = "table-synonyms"


# What a run fills when `--include` is not given: cheap, deterministic, and metadata only.
DEFAULT_COMPONENTS = frozenset((Component.COLUMN_TYPES, Component.DATA_TYPES))

# Names `--include` and `--force` accept besides the components themselves.
COMPONENT_GROUPS: Mapping[str, frozenset[Component]] = MappingProxyType(
    {
        "synonyms": frozenset((Component.COLUMN_SYNONYMS, Component.TABLE_SYNONYMS)),
        "all": frozenset(Component),
    }
)

# The components that read row data, which `enrichment.allow_sample_value_collection` refuses.
DATA_COMPONENTS = frozenset((Component.SAMPLE_VALUES, Component.ENUMS))

# The components that call Cortex.
SYNONYM_COMPONENTS = COMPONENT_GROUPS["synonyms"]

# Every name the flags accept, in the order help text lists them.
COMPONENT_NAMES: tuple[str, ...] = (*(component.value for component in Component), *COMPONENT_GROUPS)


def parse_components(values: Iterable[str]) -> tuple[frozenset[Component], tuple[str, ...]]:
    """Expand flag values into components; each value may hold several names separated by commas.

    Names compare without case or surrounding whitespace, and an empty name is ignored.

    Returns:
        The components named, and each name that is not a component or a group, in order.
    """
    by_name = {component.value: frozenset((component,)) for component in Component} | dict(COMPONENT_GROUPS)
    components: set[Component] = set()
    unknown: list[str] = []
    for value in values:
        for name in (part.strip().casefold() for part in value.split(",")):
            if not name:
                continue
            if name in by_name:
                components |= by_name[name]
            elif name not in unknown:
                unknown.append(name)
    return frozenset(components), tuple(unknown)


@dataclass(frozen=True, slots=True)
class EnrichOptions:
    """What one run fills, and which of those it re-derives over values already written.

    Attributes:
        components: The components the run fills where a value is missing.
        forced: The components it re-derives and overwrites; always a subset of `components`.
    """

    components: frozenset[Component]
    forced: frozenset[Component] = frozenset()

    def includes(self, component: Component) -> bool:
        """Report whether the run fills `component`."""
        return component in self.components

    def forces(self, component: Component) -> bool:
        """Report whether the run overwrites values `component` already wrote."""
        return component in self.forced

    @property
    def ordered(self) -> tuple[Component, ...]:
        """The components in declaration order, as reports list them."""
        return tuple(component for component in Component if component in self.components)

    @property
    def reads_data(self) -> bool:
        """Report whether the run reads row data."""
        return bool(self.components & DATA_COMPONENTS)


def resolve_options(included: frozenset[Component], forced: frozenset[Component]) -> EnrichOptions:
    """Resolve the components named by `--include` and `--force` into what a run does.

    No `--include` means the default components. Forcing a component includes it, and `enums`
    includes `sample-values`: an enum is decided from the values collected with it.
    """
    components = set(included or DEFAULT_COMPONENTS) | forced
    if Component.ENUMS in components:
        components.add(Component.SAMPLE_VALUES)
    return EnrichOptions(frozenset(components), frozenset(forced))


def collection_refusal(options: EnrichOptions, *, allowed: bool) -> Diagnostic | None:
    """Return the refusal for a run that reads row data in a project that forbids collecting it.

    Diagnostics:
        SST-CFG038: the run includes `sample-values` or `enums` and collection is not allowed.
    """
    if allowed or not options.reads_data:
        return None
    named = " ".join(component.value for component in options.ordered if component in DATA_COMPONENTS)
    return D("SST-CFG038", components=named, subject="config:enrichment.allow_sample_value_collection")
