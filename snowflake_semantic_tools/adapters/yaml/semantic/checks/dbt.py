"""Check the dbt models and columns the views use, and the descriptions views and metrics need."""

from __future__ import annotations

import re

from .....domain.model.artifact_key import artifact_key
from .....domain.model.dbt import DbtColumn, DbtModel
from .....domain.model.diagnostic import D, Diagnostic
from .....domain.model.project import ParsedView
from ..defs import MetricDef

NUMERIC_TYPES = frozenset(
    (
        "BIGINT",
        "BYTEINT",
        "DECIMAL",
        "DOUBLE",
        "DOUBLE PRECISION",
        "FLOAT",
        "INT",
        "INTEGER",
        "NUMBER",
        "NUMERIC",
        "REAL",
        "SMALLINT",
        "TINYINT",
    )
)
TEMPORAL_TYPES = frozenset(
    (
        "DATE",
        "DATETIME",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
    )
)


def _base_type(data_type: str | None) -> str:
    return re.sub(r"\s*\(.*\)\s*$", "", (data_type or "").strip().upper())


# A sample value that is really a missing value, written out as text.
_SENTINELS = frozenset(("nan", "none", "null", "<na>"))


def _dbt_column_diagnostics(
    models: dict[str, DbtModel],
    referenced_models: frozenset[str] | None = None,
) -> tuple[Diagnostic, ...]:
    """Check the metadata of every column of every dbt model, model by model in name order.

    Only a model a view uses is checked for its metadata: `referenced_models` holds their
    casefolded names, and None means every model. Sentinel sample values are reported on every
    model. Per column the rules run in the order below, so its diagnostics come out rule by rule:
    role, data type, sample values, description, unknown meta keys, declared type, sentinels.
    """
    diagnostics: list[Diagnostic] = []
    for model in sorted(models.values(), key=lambda item: item.name):
        is_referenced = referenced_models is None or model.name.casefold() in referenced_models
        for column in model.columns:
            if is_referenced:
                diagnostics.extend(_role_diagnostics(model, column))
                diagnostics.extend(_data_type_diagnostics(model, column))
                diagnostics.extend(_sample_value_diagnostics(model, column))
                diagnostics.extend(_column_description_diagnostics(model, column))
                diagnostics.extend(_column_meta_diagnostics(model, column))
                diagnostics.extend(_declared_type_diagnostics(model, column))
            diagnostics.extend(_sentinel_diagnostics(model, column))
    return tuple(diagnostics)


def _column_keys(model: DbtModel, column: DbtColumn) -> tuple[str, str]:
    """Return the artifact key of the column's model and the subject key of the column."""
    return f"dbt_model:{model.name}", f"dbt_column:{model.name}.{column.name}"


def _role_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report a column that declares no role.

    Diagnostics:
        SST-VAL308: when the column declares no `column_type`.
    """
    if column.column_type is not None:
        return []
    artifact, subject = _column_keys(model, column)
    return [D("SST-VAL308", artifact=artifact, member=column.name, subject=subject)]


def _data_type_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report a column with no data type, or with one its role cannot have.

    Diagnostics:
        SST-VAL309: when the column has no data type.
        SST-VAL305: when a fact's data type is not numeric.
        SST-VAL306: when a time dimension's data type is not temporal.
    """
    artifact, subject = _column_keys(model, column)
    if column.data_type is None:
        return [D("SST-VAL309", artifact=artifact, member=column.name, subject=subject)]
    if column.column_type == "fact" and _base_type(column.data_type) not in NUMERIC_TYPES:
        return [D("SST-VAL305", artifact=artifact, member=column.name, found=column.data_type, subject=subject)]
    if column.column_type == "time_dimension" and _base_type(column.data_type) not in TEMPORAL_TYPES:
        return [D("SST-VAL306", artifact=artifact, member=column.name, found=column.data_type, subject=subject)]
    return []


def _sample_value_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report an enum without sample values, or sample values that look like an enum's.

    Diagnostics:
        SST-VAL314: when an `is_enum` column declares no sample values.
        SST-VAL315: when a column that is not `is_enum` declares five or more sample values.
    """
    artifact, subject = _column_keys(model, column)
    if column.is_enum and not column.sample_values:
        return [D("SST-VAL314", artifact=artifact, member=column.name, subject=subject)]
    if not column.is_enum and len(column.sample_values) >= 5:
        return [
            D(
                "SST-VAL315",
                artifact=artifact,
                member=column.name,
                count=len(column.sample_values),
                subject=subject,
            )
        ]
    return []


def _column_description_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report a column without a description.

    Diagnostics:
        SST-VAL003: when the column has no description.
    """
    if column.description is not None:
        return []
    _, subject = _column_keys(model, column)
    return [D("SST-VAL003", type="column", name=f"{model.name}.{column.name}", subject=subject)]


def _column_meta_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report each `meta.sst` key of a column that SST does not read.

    Diagnostics:
        SST-PRS004: when a column's `meta.sst` holds a key SST does not read, once per key.
    """
    _, subject = _column_keys(model, column)
    return [
        D("SST-PRS004", artifact=subject, field=f"meta.sst.{key}", subject=subject) for key in column.unknown_meta_keys
    ]


def _declared_type_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report a `meta.sst.data_type` that disagrees with the data type dbt records.

    Diagnostics:
        SST-DBT004: when the declared data type disagrees with dbt's.
    """
    if column.declared_data_type is None:
        return []
    _, subject = _column_keys(model, column)
    return [
        D(
            "SST-DBT004",
            model=model.name,
            column=column.name,
            found=column.data_type,
            expected=column.declared_data_type,
            subject=subject,
        )
    ]


def _sentinel_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report each sample value that is a missing value written as text, such as `nan`.

    Diagnostics:
        SST-VAL316: when a sample value is `nan`, `none`, `null` or `<na>`, in any case.
    """
    artifact, subject = _column_keys(model, column)
    return [
        D(
            "SST-VAL316",
            artifact=artifact,
            member=column.name,
            field="sample_values",
            value=value,
            subject=subject,
        )
        for value in column.sample_values
        if value.casefold() in _SENTINELS
    ]


def _dbt_model_diagnostics(
    models: dict[str, DbtModel],
    referenced_models: frozenset[str] | None = None,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for model in sorted(models.values(), key=lambda item: item.name):
        subject = f"dbt_model:{model.name}"
        diagnostics.extend(
            D("SST-DBT030", model=model.name, key=key, subject=subject) for key in model.forbidden_location_keys
        )
        if referenced_models is not None and model.name.casefold() not in referenced_models:
            continue
        diagnostics.extend(
            D("SST-DBT005", model=model.name, field=field, subject=subject) for field in model.legacy_key_fields
        )
        diagnostics.extend(
            D("SST-PRS004", artifact=subject, field=f"meta.sst.{key}", subject=subject)
            for key in model.unknown_meta_keys
        )
        if not model.primary_key and not model.unique_keys and not model.legacy_key_fields:
            diagnostics.append(
                D(
                    "SST-VAL312",
                    artifact=subject,
                    name=model.name,
                    subject=subject,
                )
            )
        column_names = {column.name.casefold() for column in model.columns}
        declared_keys = tuple(model.primary_key) + tuple(
            column for unique_key in model.unique_keys for column in unique_key
        )
        for column in declared_keys:
            if column.casefold() not in column_names:
                diagnostics.append(
                    D(
                        "SST-VAL310",
                        artifact=subject,
                        column=column,
                        name=model.name,
                        subject=subject,
                    )
                )
        primary = {column.casefold(): column for column in model.primary_key}
        unique = {column.casefold(): column for unique_key in model.unique_keys for column in unique_key}
        for folded in sorted(primary.keys() & unique.keys()):
            diagnostics.append(
                D(
                    "SST-VAL223",
                    artifact=subject,
                    column=primary[folded],
                    model=model.name,
                    subject=subject,
                )
            )
    return tuple(diagnostics)


def _description_diagnostics(
    views: tuple[ParsedView, ...],
    metrics: tuple[MetricDef, ...],
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for view in views:
        if not str(view.source.get("description") or "").strip():
            diagnostics.append(
                D(
                    "SST-VAL003",
                    type="semantic_view",
                    name=view.name,
                    subject=artifact_key("semantic_view", view.name),
                    origin=view.origin,
                )
            )
    diagnostics.extend(
        D(
            "SST-VAL003",
            type="metric",
            name=metric.name,
            subject=artifact_key("metric", metric.name),
            origin=metric.origin,
        )
        for metric in metrics
        if metric.description is None
    )
    return tuple(diagnostics)
