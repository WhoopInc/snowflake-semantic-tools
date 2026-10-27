"""Read the parts of a member record whose YAML shape needs parsing: non-additive entries and windows.

The records themselves are `domain.model.authored`'s; this module reads their parts from the
loaded YAML, leaving every check of what was read to `domain.validate.semantic`.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.authored import (
    NULL_ORDERS,
    SORT_DIRECTIONS,
    NonAdditiveDef,
    WindowDef,
    WindowOrderDef,
)
from snowflake_semantic_tools.domain.parse.frame import canonical_frame
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, single_template_call
from snowflake_semantic_tools.domain.validate.semantic.nodes import list_of


def _non_additive(entry: object) -> NonAdditiveDef | None:
    """Read one `non_additive_dimensions` entry: the model and column its `ref:` names, and its sort.

    None when the entry is not a mapping whose `ref` is one `{{ ref('<model>', '<column>') }}`
    call; the shape check reports it. An unknown `sort_direction` or `null_order` reads as unset.
    """
    if not isinstance(entry, Mapping) or not isinstance(entry.get("ref"), str):
        return None
    try:
        call = single_template_call(str(entry["ref"]).strip(), "ref")
    except TemplateSyntaxError:
        return None
    if call is None or len(call.args) != 2:
        return None
    return NonAdditiveDef(
        dimension=call.args[1],
        table=call.args[0],
        descending=SORT_DIRECTIONS.get(str(entry.get("sort_direction"))),
        nulls_first=NULL_ORDERS.get(str(entry.get("null_order"))),
    )


def _window(value: object) -> WindowDef | None:
    """Read a metric's `window:` block as written; None when it is absent or not a mapping.

    Entries of the wrong type are dropped here, and an `order_by` entry is kept only when it is
    a reference or a mapping with a string `ref`; the shape check reports the rest. An unknown
    `sort_direction` or `null_order` reads as unset, and a `frame` that is not a frame clause
    as None, with `has_frame` still True.
    """
    if not isinstance(value, dict):
        return None

    def strings(field: str) -> tuple[str, ...]:
        return tuple(item for item in list_of(value.get(field)) if isinstance(item, str))

    order_by: list[WindowOrderDef] = []
    for entry in list_of(value.get("order_by")):
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
        frame=canonical_frame(value.get("frame")),
        has_frame=value.get("frame") is not None,
    )
