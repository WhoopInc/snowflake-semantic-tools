"""Check a semantic-layer name: that it renders unquoted, and whether it collides with reserved names."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.validate.sql import name_problem

# Snowflake's reserved and limited keywords: as an identifier each must be quoted or not used.
RESERVED_WORDS = frozenset(
    [
        "ACCOUNT",
        "ALL",
        "ALTER",
        "AND",
        "ANY",
        "AS",
        "BETWEEN",
        "BY",
        "CASE",
        "CAST",
        "CHECK",
        "COLUMN",
        "CONNECT",
        "CONNECTION",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DATABASE",
        "DELETE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXISTS",
        "FALSE",
        "FOLLOWING",
        "FOR",
        "FROM",
        "FULL",
        "GRANT",
        "GROUP",
        "GSCLUSTER",
        "HAVING",
        "ILIKE",
        "IN",
        "INCREMENT",
        "INNER",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "ISSUE",
        "JOIN",
        "LATERAL",
        "LEFT",
        "LIKE",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "MINUS",
        "NATURAL",
        "NOT",
        "NULL",
        "OF",
        "ON",
        "OR",
        "ORDER",
        "ORGANIZATION",
        "QUALIFY",
        "REGEXP",
        "REVOKE",
        "RIGHT",
        "RLIKE",
        "ROW",
        "ROWS",
        "SAMPLE",
        "SCHEMA",
        "SELECT",
        "SET",
        "SOME",
        "START",
        "TABLE",
        "TABLESAMPLE",
        "THEN",
        "TO",
        "TRIGGER",
        "TRUE",
        "TRY_CAST",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VALUES",
        "VIEW",
        "WHEN",
        "WHENEVER",
        "WHERE",
        "WINDOW",
        "WITH",
    ]
)
# Prefixes SST and Snowflake keep for their own objects, casefolded.
RESERVED_PREFIXES = ("sst_", "sst$", "snowflake_", "snowflake$")


def name_warnings(name: str, *, subject: str, origin: Origin | None = None) -> tuple[Diagnostic, ...]:
    """Report what makes a renderable name risky, in the order below.

    Diagnostics:
        SST-PRS012: the name holds a character outside ASCII.
        SST-PRS031: the name, unquoted, is a Snowflake reserved word.
        SST-PRS100: the name starts with a prefix SST or Snowflake reserves, or is `snowflake`.
    """
    found: list[Diagnostic] = []
    if not name.isascii():
        found.append(D("SST-PRS012", name=name, subject=subject, origin=origin))
    if name.upper() in RESERVED_WORDS:
        found.append(D("SST-PRS031", name=name, subject=subject, origin=origin))
    folded = name.casefold()
    if folded == "snowflake" or folded.startswith(RESERVED_PREFIXES):
        found.append(D("SST-PRS100", name=name, subject=subject, origin=origin))
    return tuple(found)


def identifier_problem(value: str, *, artifact: str, subject: str, origin: Origin | None = None) -> Diagnostic | None:
    """Report a name that does not render as one unquoted identifier; None when it does.

    Diagnostics:
        SST-PRS011: the name is not written quoted, and is a valid identifier only once quoted.
        SST-PRS005: the name is not a valid identifier, quoted or not.
    """
    problem = name_problem(value, artifact=artifact, subject=subject, origin=origin)
    if problem is None or value.startswith('"'):
        return problem
    quoted = '"' + value.replace('"', '""') + '"'
    if name_problem(quoted, artifact=artifact, subject=subject, origin=origin) is None:
        return D("SST-PRS011", name=value, subject=subject, origin=origin)
    return problem
