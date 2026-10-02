"""Typed SQL: the one way SST builds text it sends to Snowflake.

Every statement is an `Sql` value composed by `sql()` from a static template and parts that
are themselves `Sql`: identifiers, qualified names, literals, keywords from a closed
vocabulary, data types that match Snowflake's grammar, and authored expressions and queries
that have passed a guard. Nothing outside this package can construct an `Sql` from a string,
so a value from a project file or from Snowflake reaches a statement only through a
constructor that quotes it or refuses it.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.sql.authored import (
    AuthoredExpression,
    AuthoredQuery,
    UnsafeSqlError,
    expr,
    guard_expression,
    guard_query,
    query_text,
)
from snowflake_semantic_tools.domain.sql.core import Sql, canonical, join, sql
from snowflake_semantic_tools.domain.sql.datatype import datatype, is_datatype
from snowflake_semantic_tools.domain.sql.names import ident, keyword, privilege, qname, scope, stage_path
from snowflake_semantic_tools.domain.sql.values import (
    boolean,
    bound_literal,
    dollar_quoted,
    literal,
    local_file,
    null,
    number,
)

__all__ = [
    "AuthoredExpression",
    "AuthoredQuery",
    "Sql",
    "UnsafeSqlError",
    "boolean",
    "bound_literal",
    "canonical",
    "datatype",
    "dollar_quoted",
    "expr",
    "guard_expression",
    "guard_query",
    "ident",
    "is_datatype",
    "join",
    "keyword",
    "literal",
    "local_file",
    "null",
    "number",
    "privilege",
    "qname",
    "query_text",
    "scope",
    "sql",
    "stage_path",
]
