"""
Suggestion Helpers

Provides fuzzy matching and formatted suggestion text for diagnostics.
Uses stdlib difflib for typo correction — no external dependencies.
"""

import difflib
from typing import List, Optional


def _fuzzy_match(typo: str, candidates: List[str], cutoff: float = 0.6) -> Optional[str]:
    if not candidates:
        return None

    matches = difflib.get_close_matches(
        typo.lower(),
        [c.lower() for c in candidates],
        n=1,
        cutoff=cutoff,
    )

    if matches:
        for candidate in candidates:
            if candidate.lower() == matches[0]:
                return candidate

    return None


def suggest_table(typo: str, available_tables: List[str], cutoff: float = 0.6) -> Optional[str]:
    """
    Suggest the closest matching table name for a typo.

    Args:
        typo: The misspelled table name
        available_tables: List of valid table names
        cutoff: Minimum similarity ratio (0.0-1.0)

    Returns:
        Closest match, or None if no good match found
    """
    return _fuzzy_match(typo, available_tables, cutoff)


def suggest_column(typo: str, available_columns: List[str], cutoff: float = 0.6) -> Optional[str]:
    """
    Suggest the closest matching column name for a typo.

    Args:
        typo: The misspelled column name
        available_columns: List of valid column names
        cutoff: Minimum similarity ratio (0.0-1.0)

    Returns:
        Closest match, or None if no good match found
    """
    return _fuzzy_match(typo, available_columns, cutoff)


def suggest_value(typo: str, valid_values: List[str], cutoff: float = 0.5) -> Optional[str]:
    """
    Suggest the closest matching value from a set of valid options.

    Args:
        typo: The invalid value
        valid_values: List of valid values
        cutoff: Minimum similarity ratio

    Returns:
        Closest match, or None
    """
    return _fuzzy_match(typo, valid_values, cutoff)


def format_available_list(items: List[str], max_show: int = 5) -> str:
    """
    Format a list of available items for display in suggestions.

    Args:
        items: Full list of available items
        max_show: Maximum number to display before truncating

    Returns:
        Formatted string like "orders, customers, products (and 3 more)"
    """
    if not items:
        return "(none)"

    if len(items) <= max_show:
        return ", ".join(items)

    shown = ", ".join(items[:max_show])
    remaining = len(items) - max_show
    return f"{shown} (and {remaining} more)"
