"""SQL values for tests: guarded authored SQL, and the statements test doubles return.

`authored` and `authored_query` run the package's guards, as a loader does, so a model a test
builds holds exactly what a loaded one would. `statement` wraps literal test SQL as `Sql` for a
test double or an expected value; package code never does this, and composes with `sql()`.
"""

from __future__ import annotations

from collections.abc import Iterable

from snowflake_semantic_tools.domain.sql import AuthoredExpression, AuthoredQuery, Sql, guard_expression, guard_query
from snowflake_semantic_tools.domain.sql.core import _seal


def authored(text: str) -> AuthoredExpression:
    """Guard an expression a test authors, as a loader would."""
    return guard_expression(text)


def authored_query(text: str) -> AuthoredQuery:
    """Guard a query a test authors, as a loader would."""
    return guard_query(text)


def statement(text: str) -> Sql:
    """Wrap literal test SQL as `Sql`, for a test double or an expected value."""
    return _seal(text)


def statements(*texts: str) -> tuple[Sql, ...]:
    """Wrap several literal test statements, in order."""
    return tuple(_seal(text) for text in texts)


def texts(values: Iterable[Sql]) -> tuple[str, ...]:
    """Return statements as the text the driver would receive, in order."""
    return tuple(str(value) for value in values)
