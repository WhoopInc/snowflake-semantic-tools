"""Rules on one relationship's conditions and its target's keys, which the relationship reader applies.

A relationship's conditions must each reach the DDL as written, as one column pair; its two
sides must be two logical tables; and its target should hold one row per joined key.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.semantic_view import Relationship

_REF = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_OPERATOR = re.compile(r"\s(?:=|>=|<=|<>|!=|<|>)\s|\sBETWEEN\s", re.IGNORECASE)


def unparsable_condition(condition: object, *, relationship: object, origin: Origin, subject: str) -> Diagnostic:
    """The diagnostic for a condition the reader cannot parse: multi-column, or of another shape.

    A condition is multi-column when one side of its operator refs two or more columns.

    Diagnostics:
        SST-VAL213: one side of the condition spans more than one column.
        SST-PRS110: the condition is not an equality, an ASOF comparison or a range.
    """
    text = str(condition)
    sides = _OPERATOR.split(text, maxsplit=1)
    if len(sides) == 2 and any(len(_REF.findall(side)) > 1 for side in sides):
        return D("SST-VAL213", origin=origin, subject=subject, relationship=relationship, value=text)
    return D("SST-PRS110", origin=origin, subject=subject, artifact=subject, value=condition)


def dropped_condition(
    kinds: Sequence[str], conditions: Sequence[object], *, relationship: object, origin: Origin, subject: str
) -> Diagnostic | None:
    """Report the first condition the renderer would drop, given each parsed condition's kind.

    Kinds are `equality`, `asof` and `range`. A relationship renders one ASOF column and, for a
    range, only the range: a second ASOF or range condition, or any condition beside a range,
    would not be emitted.

    Diagnostics:
        SST-VAL202: a parsed condition would be dropped from the DDL.
    """
    ranges = [index for index, kind in enumerate(kinds) if kind == "range"]
    asofs = [index for index, kind in enumerate(kinds) if kind == "asof"]
    dropped: list[int] = []
    if ranges:
        dropped = [index for index in range(len(kinds)) if index != ranges[-1]]
    elif len(asofs) > 1:
        dropped = asofs[:-1]
    if not dropped:
        return None
    return D("SST-VAL202", origin=origin, subject=subject, relationship=relationship, value=str(conditions[dropped[0]]))


def is_self_loop(relationship: Relationship) -> bool:
    """Report whether a relationship joins one logical table to itself."""
    return relationship.from_table.casefold() == relationship.to_table.casefold()


def key_diagnostic(
    relationship: Relationship, target: DbtModel, *, subject: str, origin: Origin | None
) -> Diagnostic | None:
    """Report how an equality join's target keys fail to give one row per joined key, or None.

    Diagnostics:
        SST-VAL311: the target declares neither `primary_key` nor `unique_keys`.
        SST-VAL208: the join columns are part of a composite key, so the target's grain is finer
            than one row per joined key.
        SST-VAL210: no key of the target lies within the join columns.
    """
    join_columns = {column.casefold() for column in relationship.to_columns}
    keys = tuple({column.casefold() for column in key} for key in (target.primary_key, *target.unique_keys) if key)
    if not keys:
        # Snowflake refuses a REFERENCES target that declares no key at all.
        return D("SST-VAL311", artifact=subject, name=target.name, subject=subject, origin=origin)
    if any(key <= join_columns for key in keys):
        return None
    if any(join_columns < key for key in keys):
        return D(
            "SST-VAL208",
            relationship=relationship.name.casefold(),
            a=relationship.from_table.casefold(),
            b=target.name,
            subject=subject,
            origin=origin,
        )
    return D(
        "SST-VAL210",
        relationship=relationship.name.casefold(),
        name=target.name,
        value=", ".join(relationship.to_columns),
        subject=subject,
        origin=origin,
    )
