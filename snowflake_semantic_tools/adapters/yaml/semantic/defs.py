"""The member records as authored, before any reference resolves, and the parsers of their parts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from ....domain.model.diagnostic import Origin
from ....domain.model.reference import TemplateCall, TemplateSyntaxError, scan_template_calls
from ....domain.model.semantic_view import SortKey
from ..fields import optional_string
from .nodes import _list_of


@dataclass(frozen=True, slots=True)
class NonAdditiveDef:
    """One `non_additive_dimensions` entry as authored: a dimension, its table, and its sort."""

    dimension: str
    table: str | None = None
    descending: bool | None = None
    nulls_first: bool | None = None

    @property
    def key(self) -> SortKey:
        name = self.dimension.upper()
        return SortKey(f"{self.table.upper()}.{name}" if self.table else name, self.descending, self.nulls_first)


SORT_DIRECTIONS: Mapping[str, bool] = MappingProxyType({"ascending": False, "descending": True})
NULL_ORDERS: Mapping[str, bool] = MappingProxyType({"first": True, "last": False})


def _non_additive(entry: Mapping[str, Any]) -> NonAdditiveDef:
    return NonAdditiveDef(
        dimension=str(entry["dimension"]).strip(),
        table=optional_string(entry.get("table")),
        descending=SORT_DIRECTIONS.get(str(entry.get("sort_direction"))),
        nulls_first=NULL_ORDERS.get(str(entry.get("null_order"))),
    )


@dataclass(frozen=True, slots=True)
class WindowOrderDef:
    """One `window.order_by` entry: a `{{ ref() }}` or `{{ metric() }}` call and its sort."""

    ref: str
    descending: bool | None = None
    nulls_first: bool | None = None


@dataclass(frozen=True, slots=True)
class WindowDef:
    """A metric's `window:` block as authored, with its references unresolved."""

    partition_by: tuple[str, ...] = ()
    partition_excluding: tuple[str, ...] = ()
    order_by: tuple[WindowOrderDef, ...] = ()
    # The canonical frame clause; None when absent or not a frame (SST-PRS124).
    frame: str | None = None
    has_frame: bool = False

    def references(self) -> tuple[tuple[str, str], ...]:
        """Every entry, with the field label a diagnostic names it by."""
        return (
            *((f"partition_by[{index}]", text) for index, text in enumerate(self.partition_by)),
            *((f"partition_by_excluding[{index}]", text) for index, text in enumerate(self.partition_excluding)),
            *((f"order_by[{index}]", entry.ref) for index, entry in enumerate(self.order_by)),
        )


# Snowflake's window frame grammar, which is all a frame may be: it is never passed
# through as free SQL.
_FRAME_BOUND = (
    r"(?:UNBOUNDED\s+(?:PRECEDING|FOLLOWING)|CURRENT\s+ROW|(?:\d+|INTERVAL\s+'[^']*')\s+(?:PRECEDING|FOLLOWING))"
)
_FRAME = re.compile(rf"(ROWS|RANGE)\s+BETWEEN\s+({_FRAME_BOUND})\s+AND\s+({_FRAME_BOUND})", re.IGNORECASE)


def _frame(value: object) -> str | None:
    """The frame clause in canonical spelling, or None when `value` is not one."""
    match = _FRAME.fullmatch(value.strip()) if isinstance(value, str) else None
    if match is None:
        return None
    return f"{match.group(1).upper()} BETWEEN {_frame_bound(match.group(2))} AND {_frame_bound(match.group(3))}"


def _frame_bound(bound: str) -> str:
    return " ".join(token if token.startswith("'") else token.upper() for token in re.findall(r"'[^']*'|\S+", bound))


def _window(value: object) -> WindowDef | None:
    if not isinstance(value, dict):
        return None

    def strings(field: str) -> tuple[str, ...]:
        return tuple(item for item in _list_of(value.get(field)) if isinstance(item, str))

    order_by: list[WindowOrderDef] = []
    for entry in _list_of(value.get("order_by")):
        if isinstance(entry, str):
            order_by.append(WindowOrderDef(entry))
        elif isinstance(entry, dict) and isinstance(entry.get("ref"), str):
            order_by.append(
                WindowOrderDef(
                    entry["ref"],
                    SORT_DIRECTIONS.get(str(entry.get("sort_direction"))),
                    NULL_ORDERS.get(str(entry.get("null_order"))),
                )
            )
    return WindowDef(
        partition_by=strings("partition_by"),
        partition_excluding=strings("partition_by_excluding"),
        order_by=tuple(order_by),
        frame=_frame(value.get("frame")),
        has_frame=value.get("frame") is not None,
    )


@dataclass(frozen=True, slots=True)
class MetricDef:
    """A metric as authored, with its `ref()`s still unresolved."""

    name: str
    expr: str
    description: str | None
    synonyms: tuple[str, ...]
    tables: tuple[str, ...] = ()
    derived: bool = False
    using_relationships: tuple[str, ...] = ()
    non_additive: tuple[NonAdditiveDef, ...] = ()
    access_modifier: str = "public_access"
    has_tables_key: bool = False
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
    window: WindowDef | None = None

    @property
    def calls(self) -> tuple[TemplateCall, ...]:
        if self.template_calls:
            return self.template_calls
        try:
            return scan_template_calls(self.expr)
        except TemplateSyntaxError:
            return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        """Which models this metric's expr refs, in first-seen order."""
        seen: list[str] = []
        for call in self.calls:
            if call.function == "ref" and call.args and call.args[0].casefold() not in seen:
                seen.append(call.args[0].casefold())
        return tuple(seen)

    @property
    def referenced_metrics(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                call.args[0].casefold() for call in self.calls if call.function == "metric" and len(call.args) == 1
            )
        )


@dataclass(frozen=True, slots=True)
class FilterDef:
    name: str
    expr: str
    description: str | None
    tables: tuple[str, ...]
    entity_level: bool
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
    labeled: bool = False


@dataclass(frozen=True, slots=True)
class InstructionDef:
    name: str
    ai_sql_generation: str | None
    ai_question_categorization: str | None
    origin: Origin | None = None
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class VerifiedQueryDef:
    name: str
    question: str
    sql: str
    tables: tuple[str, ...]
    verified_at: int | None
    verified_by: str | None
    onboarding_question: bool | None
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
