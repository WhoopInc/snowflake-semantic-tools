"""Check the dbt models and columns the views use, and the descriptions views and metrics need."""

from __future__ import annotations

import re

from .....domain.model.artifact_key import artifact_key
from .....domain.model.dbt import DbtModel
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


def _dbt_column_diagnostics(
    models: dict[str, DbtModel],
    referenced_models: frozenset[str] | None = None,
) -> tuple[Diagnostic, ...]:
    sentinels = frozenset(("nan", "none", "null", "<na>"))
    diagnostics: list[Diagnostic] = []
    for model in sorted(models.values(), key=lambda item: item.name):
        is_referenced = referenced_models is None or model.name.casefold() in referenced_models
        for column in model.columns:
            subject = f"dbt_column:{model.name}.{column.name}"
            artifact = f"dbt_model:{model.name}"
            if is_referenced and column.column_type is None:
                diagnostics.append(
                    D(
                        "SST-VAL308",
                        artifact=artifact,
                        member=column.name,
                        subject=subject,
                    )
                )
            if is_referenced and column.data_type is None:
                diagnostics.append(
                    D(
                        "SST-VAL309",
                        artifact=artifact,
                        member=column.name,
                        subject=subject,
                    )
                )
            elif is_referenced and column.column_type == "fact" and _base_type(column.data_type) not in NUMERIC_TYPES:
                diagnostics.append(
                    D(
                        "SST-VAL305",
                        artifact=artifact,
                        member=column.name,
                        found=column.data_type,
                        subject=subject,
                    )
                )
            elif (
                is_referenced
                and column.column_type == "time_dimension"
                and _base_type(column.data_type) not in TEMPORAL_TYPES
            ):
                diagnostics.append(
                    D(
                        "SST-VAL306",
                        artifact=artifact,
                        member=column.name,
                        found=column.data_type,
                        subject=subject,
                    )
                )
            if is_referenced and column.is_enum and not column.sample_values:
                diagnostics.append(
                    D(
                        "SST-VAL314",
                        artifact=artifact,
                        member=column.name,
                        subject=subject,
                    )
                )
            elif is_referenced and not column.is_enum and len(column.sample_values) >= 5:
                diagnostics.append(
                    D(
                        "SST-VAL315",
                        artifact=artifact,
                        member=column.name,
                        count=len(column.sample_values),
                        subject=subject,
                    )
                )
            if is_referenced and column.description is None:
                diagnostics.append(
                    D(
                        "SST-VAL003",
                        type="column",
                        name=f"{model.name}.{column.name}",
                        subject=subject,
                    )
                )
            if is_referenced:
                diagnostics.extend(
                    D("SST-PRS004", artifact=subject, field=f"meta.sst.{key}", subject=subject)
                    for key in column.unknown_meta_keys
                )
            if is_referenced and column.declared_data_type is not None:
                diagnostics.append(
                    D(
                        "SST-DBT004",
                        model=model.name,
                        column=column.name,
                        found=column.data_type,
                        expected=column.declared_data_type,
                        subject=subject,
                    )
                )
            for value in column.sample_values:
                if value.casefold() in sentinels:
                    diagnostics.append(
                        D(
                            "SST-VAL316",
                            artifact=artifact,
                            member=column.name,
                            field="sample_values",
                            value=value,
                            subject=subject,
                        )
                    )
    return tuple(diagnostics)


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
