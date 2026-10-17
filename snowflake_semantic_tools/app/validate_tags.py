"""The connected check that each column Snowflake tags sensitive is recorded in its YAML (SST-VAL338).

With `governance.read_column_tags` on, the column tags of each table the views read are read
once, beside its DESCRIBE. A column a `governance.sensitive_tags` entry matches must carry
`pii_tags` or list the tag in `meta.sst.governance_tags`; `sst enrich` records it. Nothing here
writes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypeAlias

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.enrich import column_tags_sql, parse_column_tags
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.governance import TagPattern, matching_tags, records_tag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort

# What one relation's tag read answered: each column's tags by casefolded name, or the refusal.
TagAnswer: TypeAlias = Mapping[str, tuple[QualifiedName, ...]] | SnowflakePortError


def read_tags(port: ExecutionPort, fqn: str) -> TagAnswer:
    """Read the tags on each column of one relation, keeping a refusal as the answer.

    Never raises for a SQL error. An answer that names no tag or column is kept as a refusal too.
    """
    try:
        tags = parse_column_tags(port.query(column_tags_sql(QualifiedName.parse(fqn))))
    except SnowflakePortError as error:
        return error
    if tags is None:
        return SnowflakePortError("TAG_REFERENCES_ALL_COLUMNS answered without its tag and column names")
    return tags


def tag_findings(
    views: Sequence[SemanticView], answers: Mapping[str, TagAnswer], patterns: Sequence[TagPattern]
) -> list[Diagnostic]:
    """Report each sensitive tag on a column of a model the views read that the column's YAML lacks.

    Each relation is checked once, in name order, against the model the first view reading it
    names; a `source()` table has no model and is not checked. Columns come in name order.

    Args:
        answers: Each relation's tag read, by the table's FQN as the views name it.

    Diagnostics:
        SST-VAL338: a column carries a sensitive tag its YAML records neither as `pii_tags` nor
            in `meta.sst.governance_tags`; an excluded column is not reported.
        SST-VAL020: a relation's column tags could not be read, so its columns were not checked.
    """
    models: dict[str, DbtModel] = {}
    for view in views:
        for table in view.tables:
            model = view.table_models.get(table.logical_name)
            if model is not None:
                models.setdefault(table.fqn, model)
    found: list[Diagnostic] = []
    for fqn in sorted(models):
        answer = answers[fqn]
        if isinstance(answer, SnowflakePortError):
            detail = (str(answer).strip().splitlines() or [type(answer).__name__])[0]
            found.append(
                D("SST-VAL020", rule_id="SST-VAL338", detail=f"the column tags of {fqn} were not read: {detail}")
            )
            continue
        found.extend(_unrecorded(models[fqn], answer, patterns))
    return found


def _unrecorded(
    model: DbtModel, tags: Mapping[str, tuple[QualifiedName, ...]], patterns: Sequence[TagPattern]
) -> list[Diagnostic]:
    """Report each sensitive tag on one model's columns that its YAML does not record."""
    found: list[Diagnostic] = []
    for folded in sorted(tags):
        column = model.column(folded)
        if column is not None and column.excluded:
            continue
        name = column.name if column is not None else folded
        found.extend(
            D(
                "SST-VAL338",
                model=model.name,
                column=name,
                name=tag,
                subject=f"dbt_column:{model.name}.{name}",
            )
            for tag in matching_tags(tags[folded], patterns)
            if not records_tag(column, tag)
        )
    return found
