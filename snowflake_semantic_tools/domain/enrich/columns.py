"""What enrich writes for one model: which columns it reads, and the `meta.sst` keys it sets on each.

The warehouse says which columns exist; the model YAML, through the manifest, says what is
already written. A value already written is kept unless its component is forced, a column the
YAML describes and the relation lacks is reported and never dropped, and an excluded column is
left alone. A column carrying `pii_tags` is never sampled, and its values reach no prompt.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.enrich.components import Component, EnrichOptions
from snowflake_semantic_tools.domain.enrich.infer import (
    clean_synonyms,
    decide_samples,
    derive_column_type,
    is_sampled_type,
    same_data_type,
    semantic_data_type,
    taken_names,
)
from snowflake_semantic_tools.domain.enrich.prompts import EXAMPLES_PER_COLUMN, PromptColumn
from snowflake_semantic_tools.domain.model.config_schema import EnrichmentConfig
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.identifier import Identifier

# The `meta.sst` keys enrich writes, in the order it adds them to a column.
WRITTEN_KEYS = ("column_type", "data_type", "synonyms", "sample_values", "is_enum")

# The key each component fills; `sample-values` also decides `is_enum` beside the values it writes.
COMPONENT_KEYS: Mapping[Component, str] = MappingProxyType(
    {
        Component.COLUMN_TYPES: "column_type",
        Component.DATA_TYPES: "data_type",
        Component.SAMPLE_VALUES: "sample_values",
        Component.ENUMS: "is_enum",
        Component.COLUMN_SYNONYMS: "synonyms",
    }
)


@dataclass(frozen=True, slots=True)
class WarehouseColumn:
    """One column of a relation as INFORMATION_SCHEMA describes it.

    Attributes:
        name: The name as Snowflake stores it: upper-cased unless it was created quoted.
        data_type: INFORMATION_SCHEMA's `DATA_TYPE`, such as `TEXT` or `NUMBER`.
    """

    name: str
    data_type: str


@dataclass(frozen=True, slots=True)
class ColumnUpdate:
    """The `meta.sst` keys enrich sets on one column, in `WRITTEN_KEYS` order.

    Attributes:
        name: The column as the YAML names it; for an added column, the name enrich writes.
        values: Each key and the value it takes; a list value is a tuple of text.
        added: The YAML does not describe the column yet, so enrich adds its entry.
    """

    name: str
    values: tuple[tuple[str, object], ...]
    added: bool = False

    @property
    def keys(self) -> tuple[str, ...]:
        """The keys the update sets."""
        return tuple(key for key, _ in self.values)


@dataclass(frozen=True, slots=True)
class ModelEnrichment:
    """What enrich writes for one model, and what it found that it does not change.

    Attributes:
        model: The dbt model's name.
        updates: One entry per column that gains or changes a value, in the relation's order.
        diagnostics: Warnings about columns the run leaves as they are.
    """

    model: str
    updates: tuple[ColumnUpdate, ...] = ()
    diagnostics: tuple[Diagnostic, ...] = ()

    @property
    def added(self) -> tuple[str, ...]:
        """The columns the YAML gains."""
        return tuple(update.name for update in self.updates if update.added)

    def filled(self, component: Component) -> int:
        """Count the columns on which `component` sets a value."""
        key = COMPONENT_KEYS.get(component)
        return sum(1 for update in self.updates if key in update.keys)


def yaml_column_name(stored: str) -> str:
    """Return the name enrich writes for a column it adds: lowercase when Snowflake stores it unquoted.

    A name created quoted, such as `"MixedCase"`, keeps its exact text.
    """
    return stored if Identifier.shown(stored).quoted else stored.lower()


def _subject(model: DbtModel, column: str) -> str:
    return f"dbt_column:{model.name}.{column}"


def _declared(existing: DbtColumn | None) -> frozenset[str]:
    return existing.declared_keys if existing is not None else frozenset()


def role(model: DbtModel, existing: DbtColumn | None, column: WarehouseColumn, options: EnrichOptions) -> str:
    """Return the column type a column has once enrich runs: the one written, unless re-derived."""
    written = existing.column_type if existing is not None else None
    if written is not None and not options.forces(Component.COLUMN_TYPES):
        return written
    return derive_column_type(column.data_type, key=model.is_key_column(column.name))


def _unique_columns(warehouse: Sequence[WarehouseColumn]) -> tuple[WarehouseColumn, ...]:
    """Drop a column whose name repeats an earlier one's but for case: one YAML entry names both."""
    seen: set[str] = set()
    unique: list[WarehouseColumn] = []
    for column in warehouse:
        if column.name.casefold() not in seen:
            seen.add(column.name.casefold())
            unique.append(column)
    return tuple(unique)


def _enrichable(
    model: DbtModel, warehouse: Sequence[WarehouseColumn]
) -> list[tuple[WarehouseColumn, DbtColumn | None]]:
    """Pair each relation column enrich may touch with the YAML entry describing it, if any."""
    paired = ((column, model.column(column.name)) for column in _unique_columns(warehouse))
    return [(column, existing) for column, existing in paired if existing is None or not existing.excluded]


def sample_columns(
    model: DbtModel, warehouse: Sequence[WarehouseColumn], options: EnrichOptions
) -> tuple[WarehouseColumn, ...]:
    """Return the columns whose distinct values a run reads, in the relation's order.

    A column is read when its type is sampled, it carries no `pii_tags`, and the run fills or
    forces its sample values, or decides `is_enum` for the values it already has.
    """
    if not options.includes(Component.SAMPLE_VALUES):
        return ()
    selected = []
    for column, existing in _enrichable(model, warehouse):
        if (existing is not None and existing.pii_tagged) or not is_sampled_type(column.data_type):
            continue
        declared = _declared(existing)
        wants_values = "sample_values" not in declared or options.forces(Component.SAMPLE_VALUES)
        wants_enum = (
            options.includes(Component.ENUMS)
            and ("is_enum" not in declared or options.forces(Component.ENUMS))
            and existing is not None
            and bool(existing.sample_values)
            and role(model, existing, column, options) != "fact"
        )
        if wants_values or wants_enum:
            selected.append(column)
    return tuple(selected)


def synonym_columns(
    model: DbtModel, warehouse: Sequence[WarehouseColumn], options: EnrichOptions
) -> tuple[WarehouseColumn, ...]:
    """Return the columns a run asks Cortex synonyms for: those with none written, or all when forced."""
    if not options.includes(Component.COLUMN_SYNONYMS):
        return ()
    forced = options.forces(Component.COLUMN_SYNONYMS)
    return tuple(
        column
        for column, existing in _enrichable(model, warehouse)
        if forced or existing is None or not existing.synonyms
    )


def prompt_columns(
    model: DbtModel,
    columns: Sequence[WarehouseColumn],
    samples: Mapping[str, Sequence[str]],
) -> tuple[PromptColumn, ...]:
    """Describe columns for a synonym prompt, with examples from the values sampled or already written.

    `samples` holds the values read this run, by casefolded relation column name. A column that
    carries `pii_tags` is described without examples.
    """
    described = []
    for column in columns:
        existing = model.column(column.name)
        examples: tuple[str, ...] = ()
        if existing is None or not existing.pii_tagged:
            fetched = samples.get(column.name.casefold())
            written = existing.sample_values if existing is not None else ()
            examples = tuple(fetched if fetched is not None else written)[:EXAMPLES_PER_COLUMN]
        described.append(
            PromptColumn(
                name=existing.name if existing is not None else yaml_column_name(column.name),
                data_type=semantic_data_type(column.data_type),
                description=existing.description if existing is not None else None,
                examples=examples,
            )
        )
    return tuple(described)


def column_synonyms(
    model: DbtModel,
    warehouse: Sequence[WarehouseColumn],
    proposals: Mapping[str, Sequence[object]],
    *,
    limit: int,
) -> dict[str, tuple[str, ...]]:
    """Clean each column's proposed synonyms against the names and synonyms of the model's other columns.

    `proposals` holds Cortex's answer by casefolded column name. Columns are cleaned in the
    relation's order, and a synonym one column keeps is unavailable to the columns after it.

    Returns:
        The synonyms to write, by casefolded column name; a column left with none is absent.
    """
    names = [column.name for column in warehouse] + [column.name for column in model.columns]
    kept: dict[str, tuple[str, ...]] = {}
    for column in _unique_columns(warehouse):
        folded = column.name.casefold()
        if folded not in proposals:
            continue
        others = [synonym for other in model.columns if other.name.casefold() != folded for synonym in other.synonyms]
        taken = taken_names([name for name in names if name.casefold() != folded], others)
        used = {synonym.casefold() for chosen in kept.values() for synonym in chosen}
        cleaned = clean_synonyms(proposals[folded], name=column.name, taken=taken | used, limit=limit)
        if cleaned:
            kept[folded] = cleaned
    return kept


def _data_type_value(
    model: DbtModel, existing: DbtColumn | None, column: WarehouseColumn, options: EnrichOptions
) -> tuple[str | None, Diagnostic | None]:
    """Return the data type to write, or the warning for a written one that disagrees with the relation.

    dbt's own `data_type`, which a contract enforces, is never written over.

    Diagnostics:
        SST-VAL327: the written `meta.sst.data_type` names another type than the relation has.
    """
    if not options.includes(Component.DATA_TYPES) or (existing is not None and existing.native_data_type):
        return None, None
    written = existing.data_type if existing is not None else None
    found = semantic_data_type(column.data_type)
    if written is None or options.forces(Component.DATA_TYPES):
        return (found if found != written else None), None
    if same_data_type(written, column.data_type) or existing is None:
        return None, None
    warning = D(
        "SST-VAL327",
        model=model.name,
        column=existing.name,
        declared=written,
        found=found,
        subject=_subject(model, existing.name),
    )
    return None, warning


def _column_type_value(
    model: DbtModel, existing: DbtColumn | None, column: WarehouseColumn, options: EnrichOptions
) -> str | None:
    """Return the column type to write: derived when none is written, or when forced."""
    if not options.includes(Component.COLUMN_TYPES):
        return None
    written = existing.column_type if existing is not None else None
    if written is not None and not options.forces(Component.COLUMN_TYPES):
        return None
    derived = derive_column_type(column.data_type, key=model.is_key_column(column.name))
    return derived if derived != written else None


def _sample_values(
    existing: DbtColumn | None,
    column_role: str,
    options: EnrichOptions,
    settings: EnrichmentConfig,
    fetched: Sequence[str] | None,
) -> list[tuple[str, object]]:
    """Return the sample values and `is_enum` to write from one column's sampled values.

    Values are written where none are, or when forced; with them a dimension gets `is_enum`,
    so the pair always agrees, and `enums` decides `is_enum` for values already written.
    `is_enum` is true only when the values on the column are every value it holds. A written
    `is_enum: true` the data contradicts is left, with its values unwritten, for its author.
    """
    if fetched is None:
        return []
    decision = decide_samples(fetched, distinct_limit=settings.distinct_limit, display_limit=settings.display_limit)
    declared = _declared(existing)
    written_values = existing.sample_values if existing is not None else ()
    written_enum = existing.is_enum if existing is not None else None
    enum_open = "is_enum" not in declared or options.forces(Component.ENUMS)
    write_values = bool(decision.values) and (
        "sample_values" not in declared or options.forces(Component.SAMPLE_VALUES)
    )
    if write_values and not enum_open and written_enum is True and not decision.complete and column_role != "fact":
        # Left for its author, whom SST-VAL317 tells.
        return []
    values = decision.values if write_values else written_values
    result: list[tuple[str, object]] = []
    if write_values and decision.values != written_values:
        result.append(("sample_values", decision.values))
    decides_enum = enum_open and (write_values or options.includes(Component.ENUMS))
    if column_role != "fact" and values and decides_enum:
        is_enum = decision.complete and set(values) == set(decision.values)
        if is_enum != written_enum:
            result.append(("is_enum", is_enum))
    return result


def _hand_edited_enum(
    model: DbtModel,
    existing: DbtColumn | None,
    column_role: str,
    options: EnrichOptions,
    settings: EnrichmentConfig,
    fetched: Sequence[str] | None,
) -> list[Diagnostic]:
    """Report a written `is_enum: true` that the sampled data contradicts, which enrich leaves as written.

    Enrich owns `is_enum`; without `--force enums` it keeps an author's value, so a value the
    data shows is not a closed set is reported rather than overwritten.

    Diagnostics:
        SST-VAL317: a dimension's written `is_enum: true` is not what enrich would write.
    """
    if fetched is None or existing is None or existing.is_enum is not True or column_role == "fact":
        return []
    declared = _declared(existing)
    decision = decide_samples(fetched, distinct_limit=settings.distinct_limit, display_limit=settings.display_limit)
    enum_open = "is_enum" not in declared or options.forces(Component.ENUMS)
    write_values = bool(decision.values) and (
        "sample_values" not in declared or options.forces(Component.SAMPLE_VALUES)
    )
    # Exactly the case `_sample_values` leaves unwritten for the author.
    if enum_open or not write_values or decision.complete:
        return []
    return [
        D(
            "SST-VAL317",
            artifact=f"dbt_model:{model.name}",
            member=existing.name,
            field="is_enum",
            subject=_subject(model, existing.name),
        )
    ]


def _synonym_value(
    existing: DbtColumn | None, options: EnrichOptions, synonyms: tuple[str, ...] | None
) -> tuple[str, ...] | None:
    """Return the synonyms to write: where none are written, or when forced."""
    if not options.includes(Component.COLUMN_SYNONYMS) or not synonyms:
        return None
    written = existing.synonyms if existing is not None else ()
    if written and not options.forces(Component.COLUMN_SYNONYMS):
        return None
    return synonyms if synonyms != written else None


def _absent_columns(model: DbtModel, warehouse: Sequence[WarehouseColumn]) -> list[Diagnostic]:
    """Report each column the YAML describes and the relation lacks.

    Diagnostics:
        SST-VAL325: once per described column the relation does not have.
    """
    present = {column.name.casefold() for column in warehouse}
    return [
        D("SST-VAL325", model=model.name, column=column.name, subject=_subject(model, column.name))
        for column in model.columns
        if column.name.casefold() not in present
    ]


def _pii_samples(model: DbtModel, options: EnrichOptions) -> list[Diagnostic]:
    """Report each column with `pii_tags` that carries sample values, when the run reads row data.

    Diagnostics:
        SST-VAL328: once per such column.
    """
    if not options.reads_data:
        return []
    return [
        D(
            "SST-VAL328",
            model=model.name,
            column=column.name,
            count=len(column.sample_values),
            subject=_subject(model, column.name),
        )
        for column in model.columns
        if column.pii_tagged and column.sample_values
    ]


def enrich_model(
    model: DbtModel,
    warehouse: Sequence[WarehouseColumn],
    options: EnrichOptions,
    settings: EnrichmentConfig,
    *,
    samples: Mapping[str, Sequence[str]],
    synonyms: Mapping[str, tuple[str, ...]],
) -> ModelEnrichment:
    """Decide what enrich writes for one model, from its relation's columns and what the run read.

    `samples` and `synonyms` hold, by casefolded relation column name, the values sampled and the
    cleaned synonyms; a column absent from either is not changed by that component. A column the
    YAML does not describe is added only when it gains a value.

    Diagnostics:
        SST-VAL325: a described column is absent from the relation.
        SST-VAL327: a written data type disagrees with the relation's.
        SST-VAL328: a column with `pii_tags` carries sample values, and the run reads row data.
        SST-VAL317: a written `is_enum: true` the sampled data contradicts.
    """
    diagnostics = _absent_columns(model, warehouse)
    updates: list[ColumnUpdate] = []
    for column, existing in _enrichable(model, warehouse):
        folded = column.name.casefold()
        column_role = role(model, existing, column, options)
        data_type, warning = _data_type_value(model, existing, column, options)
        if warning is not None:
            diagnostics.append(warning)
        fetched = None if existing is not None and existing.pii_tagged else samples.get(folded)
        found: dict[str, object] = {
            "column_type": _column_type_value(model, existing, column, options),
            "data_type": data_type,
            "synonyms": _synonym_value(existing, options, synonyms.get(folded)),
        }
        found.update(_sample_values(existing, column_role, options, settings, fetched))
        diagnostics.extend(_hand_edited_enum(model, existing, column_role, options, settings, fetched))
        values = tuple((key, found[key]) for key in WRITTEN_KEYS if found.get(key) is not None)
        if values:
            name = existing.name if existing is not None else yaml_column_name(column.name)
            updates.append(ColumnUpdate(name, values, added=existing is None))
    diagnostics.extend(_pii_samples(model, options))
    return ModelEnrichment(model.name, tuple(updates), tuple(diagnostics))
