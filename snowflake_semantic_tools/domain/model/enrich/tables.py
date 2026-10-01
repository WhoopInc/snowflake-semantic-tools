"""Table synonyms: which semantic views a model's synonyms go to, and what each view gains.

A view keeps a table's synonyms in its `table_config`, so a model's synonyms are written
into every view that uses it and has none for it. They are generated once per model and cleaned
per view against that view's other tables, so no two of its tables share a name.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.enrich.components import Component, EnrichOptions
from snowflake_semantic_tools.domain.model.enrich.infer import clean_synonyms


@dataclass(frozen=True, slots=True)
class ViewTable:
    """One table of one semantic view, as its table-synonym edit needs it.

    Attributes:
        view: The view's name as its file writes it.
        path: The view's file, relative to the project root.
        table: The dbt model the table is, as the view's `tables` names it.
        synonyms: The synonyms its `table_config` already gives the table.
        taken: The casefolded names and synonyms of the view's other tables.
    """

    view: str
    path: str
    table: str
    synonyms: tuple[str, ...]
    taken: frozenset[str]


@dataclass(frozen=True, slots=True)
class TableSynonymEdit:
    """The synonyms enrich writes for one table into one view's `table_config`."""

    path: str
    view: str
    table: str
    synonyms: tuple[str, ...]


def needs_table_synonyms(targets: Sequence[ViewTable], options: EnrichOptions) -> bool:
    """Report whether any view using the model gains table synonyms from this run."""
    if not options.includes(Component.TABLE_SYNONYMS):
        return False
    return options.forces(Component.TABLE_SYNONYMS) or any(not target.synonyms for target in targets)


def avoided_names(targets: Sequence[ViewTable]) -> tuple[str, ...]:
    """Return every name the model's synonyms must avoid in any of its views, sorted."""
    return tuple(sorted({name for target in targets for name in target.taken}))


def table_synonym_edits(
    targets: Sequence[ViewTable], proposals: Sequence[object], options: EnrichOptions, *, limit: int
) -> tuple[TableSynonymEdit, ...]:
    """Return each view's edit: the proposals cleaned against that view's other tables.

    A view that already has synonyms for the table is left alone unless the run forces table
    synonyms, and a view whose cleaned synonyms are what it has, or none, gets no edit.
    """
    if not options.includes(Component.TABLE_SYNONYMS):
        return ()
    edits = []
    for target in targets:
        if target.synonyms and not options.forces(Component.TABLE_SYNONYMS):
            continue
        cleaned = clean_synonyms(proposals, name=target.table, taken=target.taken, limit=limit)
        if cleaned and cleaned != target.synonyms:
            edits.append(TableSynonymEdit(target.path, target.view, target.table, cleaned))
    return tuple(edits)
