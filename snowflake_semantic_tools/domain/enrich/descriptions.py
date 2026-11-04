"""Descriptions: what a model's or column's `description` gains from its Snowflake COMMENT.

Teams whose catalog already documents tables and columns as Snowflake comments, synced by a
data catalog or written with `COMMENT ON`, should not have to copy each one into the model
YAML by hand. A comment fills a description only where none is written, unless the run forces
`descriptions`, and a comment that is blank, or that is the description already written, changes
nothing.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.enrich.components import Component, EnrichOptions
from snowflake_semantic_tools.domain.validate.column_metadata import has_template_syntax


def needs_description(written: str | None, options: EnrichOptions) -> bool:
    """Report whether a model or column with the description `written` gains one from this run."""
    if not options.includes(Component.DESCRIPTIONS):
        return False
    return options.forces(Component.DESCRIPTIONS) or not (written or "").strip()


def description_value(written: str | None, comment: str | None, options: EnrichOptions) -> str | None:
    """Return the description to write from a Snowflake comment, or None when the run writes none.

    Args:
        written: The description the model or column has, as the manifest holds it.
        comment: The relation's or column's COMMENT; None when it has none.

    Returns:
        The comment without surrounding whitespace, where no description is written or the run
        forces `descriptions`; None when the comment is blank or is what is written already.
    """
    if not needs_description(written, options):
        return None
    found = (comment or "").strip()
    if not found or found == (written or "").strip():
        return None
    return found


def table_description(
    model: str, written: str | None, comment: str | None, options: EnrichOptions
) -> tuple[str | None, tuple[Diagnostic, ...]]:
    """Return the description a model gains from its relation's COMMENT, or why it gains none.

    dbt renders the YAML as Jinja, so a comment that holds template syntax is reported and not
    written, as a column's is.

    Args:
        model: The dbt model's name.
        written: The model's description, as the manifest holds it.
        comment: Its relation's COMMENT; None when it has none.

    Returns:
        The description to write, or None; and the refusal of a templated comment.

    Diagnostics:
        SST-SNO031: the comment holds template syntax; nothing is written for it.
    """
    description = description_value(written, comment, options)
    if description is None or not has_template_syntax(description):
        return description, ()
    refused = D(
        "SST-SNO031",
        model=model,
        step="writing the model's description",
        detail="the relation's comment holds template syntax, which dbt would run; nothing is written for it",
        subject=f"dbt_model:{model}",
    )
    return None, (refused,)
