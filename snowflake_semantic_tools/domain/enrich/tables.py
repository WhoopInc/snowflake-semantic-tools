"""Table synonyms: what a dbt model's `meta.sst.synonyms` gains.

A table's synonyms are its model's, one set for every semantic view that uses it. They are
cleaned against the names and synonyms of every other table those views hold, so no view has two
tables that share a name.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.enrich.components import Component, EnrichOptions
from snowflake_semantic_tools.domain.enrich.infer import clean_synonyms


def needs_table_synonyms(synonyms: Sequence[str], options: EnrichOptions) -> bool:
    """Report whether a model with `synonyms` gains table synonyms from this run."""
    if not options.includes(Component.TABLE_SYNONYMS):
        return False
    return options.forces(Component.TABLE_SYNONYMS) or not synonyms


def table_synonyms(
    model: str,
    synonyms: tuple[str, ...],
    proposals: Sequence[object],
    taken: frozenset[str],
    options: EnrichOptions,
    *,
    limit: int,
) -> tuple[str, ...]:
    """Return the synonyms enrich writes on the model: the proposals cleaned against `taken`.

    Args:
        synonyms: The synonyms the model already has.
        taken: The casefolded names and synonyms of the other tables of every view using it.

    Returns:
        The synonyms to write; empty when the run writes none, or they are what it has.
    """
    if not needs_table_synonyms(synonyms, options):
        return ()
    cleaned = clean_synonyms(proposals, name=model, taken=taken, limit=limit)
    return cleaned if cleaned != synonyms else ()
