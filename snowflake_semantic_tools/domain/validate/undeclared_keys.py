"""Warn on keys a skill's front matter, an eval dataset or an eval config writes and does not declare.

These kinds were read without an unknown-key check, so a key SST ignores is a warning here, not
the error a near miss is in the files checked from the start. Every key a table declares is
known, whatever its status: a refused or removed key has its own report.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.authored import NodePath
from snowflake_semantic_tools.domain.model.authored_schema import Node
from snowflake_semantic_tools.domain.parse.fields import nearest_field
from snowflake_semantic_tools.domain.validate.loose_values import field_label


def undeclared_key_diagnostics(
    node: Node,
    value: object,
    path: NodePath,
    *,
    artifact: str,
    origin_of: Callable[[NodePath], Origin],
    subject: str | None = None,
    start: int | None = None,
) -> list[Diagnostic]:
    """Report each key below `value`, a declaration of `node` at `path`, that its node does not declare.

    A mapping's own keys come first, in the order written, then each nested node's, in table
    order; an open node and a `config:` block, which its settings check, are not read.

    Diagnostics:
        SST-PRS132: a key the node does not declare, with the declared key it is a near miss of.
    """
    begin = len(path) if start is None else start
    found: list[Diagnostic] = []
    if not isinstance(value, Mapping):
        return found
    declared = [item.name for item in node.all_fields()]
    if not node.open and not node.checked_elsewhere:
        for key in value:
            if str(key) in declared:
                continue
            nearest = nearest_field(str(key), declared)
            found.append(
                D(
                    "SST-PRS132",
                    artifact=artifact,
                    field=field_label((*path, str(key))[begin:]),
                    detail=f"; did you mean '{nearest}'?" if nearest else "",
                    subject=subject,
                    origin=origin_of((*path, str(key))),
                )
            )
    for item in node.all_fields():
        nested, held = (item.nested if item.read else None), value.get(item.name)
        if nested is None or item.config_block is not None or held is None:
            continue
        where = (*path, item.name)
        if isinstance(item.items, Node):
            for index, entry in enumerate(held if isinstance(held, list) else ()):
                found.extend(
                    undeclared_key_diagnostics(
                        nested,
                        entry,
                        (*where, index),
                        artifact=artifact,
                        origin_of=origin_of,
                        subject=subject,
                        start=begin,
                    )
                )
        else:
            found.extend(
                undeclared_key_diagnostics(
                    nested, held, where, artifact=artifact, origin_of=origin_of, subject=subject, start=begin
                )
            )
    return found
