"""Check what the dbt manifest says about the models the semantic layer consumes.

Every function here reads a `DbtCatalog` and the names of the models the semantic layer uses,
casefolded, and returns its diagnostics in model-name order. The column and key metadata rules
live with the semantic load; these are the rules about the seam itself.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel


def _subject(model: DbtModel) -> str:
    return f"dbt_model:{model.name}"


def empty_catalog(catalog: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Report a manifest with no model at all, readable or not.

    Diagnostics:
        SST-DBT001: the manifest lists no model.
    """
    if catalog.models or catalog.unreadable_models:
        return ()
    return (D("SST-DBT001"),)


def unreadable_model(catalog: DbtCatalog, name: str, *, subject: str | None = None) -> Diagnostic | None:
    """Report a consumed model that produces no relation; None when the catalog has no such model.

    Diagnostics:
        SST-DBT009: the model is disabled, or materialised so that it has no relation.
    """
    reason = catalog.unreadable_models.get(name.casefold())
    if reason is None:
        return None
    return D("SST-DBT009", model=name, found=reason, subject=subject or f"dbt_model:{name}")


def consumed_model_diagnostics(catalog: DbtCatalog, consumed: Iterable[str]) -> tuple[Diagnostic, ...]:
    """Check each consumed model the catalog has, in name order.

    Diagnostics:
        SST-DBT015: the model has no checksum, so a change to it is invisible to change detection.
        SST-DBT023: no dbt test is attached to the model.
        SST-DBT024: the model enforces no contract and leaves a column's type to inference, so an
            upstream type change would re-infer it unseen. A model that types every column with
            `meta.sst.data_type` (or dbt's own `data_type`) has pinned them without a contract.
    """
    wanted = {name.casefold() for name in consumed}
    diagnostics: list[Diagnostic] = []
    for model in sorted(catalog.models, key=lambda item: item.name.casefold()):
        if model.name.casefold() not in wanted:
            continue
        if model.checksum is None:
            diagnostics.append(D("SST-DBT015", model=model.name, subject=_subject(model)))
        if model.test_count == 0:
            diagnostics.append(D("SST-DBT023", model=model.name, subject=_subject(model)))
        if not model.has_contract and _infers_a_type(model):
            diagnostics.append(D("SST-DBT024", model=model.name, subject=_subject(model)))
    return tuple(diagnostics)


def _infers_a_type(model: DbtModel) -> bool:
    """Report whether a column of the model declares no data type, or the model lists no column."""
    return not model.columns or any(column.data_type is None for column in model.columns)


def collapse_diagnostics(catalog: DbtCatalog, consumed: Iterable[str]) -> tuple[Diagnostic, ...]:
    """Report two models that resolve to one relation and declare different semantic columns.

    A consumed model is compared with every other model of its relation, in name order; the
    columns compared are those with a `column_type`, casefolded, and the first that only one of
    them declares is named.

    Diagnostics:
        SST-DBT010: two models collapse to one relation and differ in a semantic column.
    """
    wanted = {name.casefold() for name in consumed}
    by_relation: dict[str, list[DbtModel]] = {}
    for model in sorted(catalog.models, key=lambda item: item.name.casefold()):
        by_relation.setdefault(model.relation_name.casefold(), []).append(model)
    diagnostics: list[Diagnostic] = []
    for group in by_relation.values():
        first = next((model for model in group if model.name.casefold() in wanted), None)
        for other in group:
            if first is None or other is first:
                continue
            differing = sorted(_semantic_columns(first) ^ _semantic_columns(other))
            if differing:
                diagnostics.append(
                    D(
                        "SST-DBT010",
                        a=first.name,
                        b=other.name,
                        value=first.relation_name,
                        column=differing[0],
                        subject=_subject(first),
                    )
                )
    return tuple(diagnostics)


def _semantic_columns(model: DbtModel) -> frozenset[str]:
    return frozenset(column.name.casefold() for column in model.columns if column.column_type is not None)


def fan_out_diagnostics(feeds: Mapping[str, Iterable[str]], catalog: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Report each model that feeds more than one artifact, in model-name order.

    Args:
        feeds: The artifact keys each artifact reads, by artifact key; the models are named casefolded.

    Diagnostics:
        SST-DBT016: a model feeds two or more artifacts.
    """
    fed: dict[str, set[str]] = {}
    for artifact, models in feeds.items():
        for name in models:
            fed.setdefault(name.casefold(), set()).add(artifact)
    names = {model.name.casefold(): model.name for model in catalog.models}
    return tuple(
        D("SST-DBT016", model=names.get(name, name), count=len(artifacts), subject=f"dbt_model:{names.get(name, name)}")
        for name, artifacts in sorted(fed.items())
        if len(artifacts) > 1
    )


def seam_summary(catalog: DbtCatalog, consumed: Iterable[str], refs_resolved: int) -> Diagnostic:
    """Summarise what the semantic layer read across the dbt seam.

    Diagnostics:
        SST-DBT025: always, once: models and sources read, refs resolved, and columns checked.
    """
    wanted = {name.casefold() for name in consumed}
    columns = sum(len(model.columns) for model in catalog.models if model.name.casefold() in wanted)
    value = (
        f"{len(catalog.models)} models read, {len(catalog.sources)} sources read, "
        f"{refs_resolved} refs resolved, {columns} columns checked"
    )
    return D("SST-DBT025", value=value)


def literal_relation_diagnostic(catalog: DbtCatalog, relation: str, *, subject: str) -> Diagnostic | None:
    """Report a literal relation that names a model where dbt builds that model under another name.

    The literal `<database>.<schema>.<model name>` points at what dbt would build without an
    `alias:`; when the model's resolved relation has another name, nothing exists there.

    Diagnostics:
        SST-DBT006: the relation names a model of the same database and schema by its model name,
            and the model resolves to a relation of another name.
    """
    parts = [part.strip().strip('"').casefold() for part in relation.split(".")]
    if len(parts) != 3:
        return None
    for model in catalog.models:
        resolved = [part.strip().strip('"').casefold() for part in model.relation_name.split(".")]
        if len(resolved) != 3 or resolved[:2] != parts[:2]:
            continue
        if parts[2] == model.name.casefold() and resolved[2] != parts[2]:
            return D("SST-DBT006", model=model.name, value=model.relation_name, subject=subject)
    return None
