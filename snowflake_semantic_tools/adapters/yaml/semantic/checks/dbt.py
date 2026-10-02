"""Check the dbt models and columns the views use, and the descriptions views and metrics need."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.validate.column_metadata import is_numeric, is_sentinel, is_temporal


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
    if column.column_type == "fact" and not is_numeric(column.data_type):
        return [D("SST-VAL305", artifact=artifact, member=column.name, found=column.data_type, subject=subject)]
    if column.column_type == "time_dimension" and not is_temporal(column.data_type):
        return [D("SST-VAL306", artifact=artifact, member=column.name, found=column.data_type, subject=subject)]
    return []


def _sample_value_diagnostics(model: DbtModel, column: DbtColumn) -> list[Diagnostic]:
    """Report an enum without sample values, or sample values whose completeness nobody declared.

    An explicit `is_enum: false` declares the values a sample, so it is not reported; nor is a
    fact, which cannot be an enum.

    Diagnostics:
        SST-VAL314: when an `is_enum` column declares no sample values.
        SST-VAL315: when a column that is not a fact declares five or more sample values and no
            `is_enum`.
    """
    artifact, subject = _column_keys(model, column)
    if column.is_enum and not column.sample_values:
        return [D("SST-VAL314", artifact=artifact, member=column.name, subject=subject)]
    if column.is_enum is None and column.column_type != "fact" and len(column.sample_values) >= 5:
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
    """Report each `meta.sst` key of a column that SST does not read, and a private dimension.

    A dimension's `access_modifier: private_access` is reported as unsupported rather than
    as an unread key: it is read only to refuse it.

    Diagnostics:
        SST-VAL222: when a dimension declares `access_modifier: private_access`.
        SST-PRS004: when a column's `meta.sst` holds a key SST does not read, once per key.
    """
    artifact, subject = _column_keys(model, column)
    private = column.access_modifier == "private_access" and column.column_type in ("dimension", "time_dimension")
    diagnostics: list[Diagnostic] = []
    if private:
        diagnostics.append(
            D("SST-VAL222", artifact=artifact, member=column.name, value=column.access_modifier, subject=subject)
        )
    diagnostics.extend(
        D("SST-PRS004", artifact=subject, field=f"meta.sst.{key}", subject=subject)
        for key in column.unknown_meta_keys
        if not (private and key == "access_modifier")
    )
    return diagnostics


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
        if is_sentinel(value)
    ]


def _dbt_model_diagnostics(
    models: dict[str, DbtModel],
    referenced_models: frozenset[str] | None = None,
) -> tuple[Diagnostic, ...]:
    """Check the `meta.sst` keys and the key metadata of every dbt model, in name order.

    A forbidden location key is reported on every model; the other rules run only on a model a
    view uses: `referenced_models` holds their casefolded names, and None means every model.
    Key columns compare casefolded.

    Diagnostics:
        SST-DBT030: when `meta.sst` holds a forbidden location key, once per key.
        SST-DBT005: when the model writes key metadata in the 0.3 form, once per field.
        SST-PRS004: when `meta.sst` holds a key SST does not read, once per key.
        SST-VAL312: when the model declares neither `primary_key` nor `unique_keys`, in either form.
        SST-VAL310: when a declared key column is not a column of the model.
        SST-VAL223: when a column is in both `primary_key` and `unique_keys`.
    """
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
    """Report each view, then each metric, that has no description.

    Diagnostics:
        SST-VAL003: when a view's `description` is absent or blank, or a metric has none.
    """
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
