"""Check a semantic-layer name: that it renders unquoted, and whether it collides with reserved names."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.sql.names import RESERVED_WORDS as RESERVED_WORDS
from snowflake_semantic_tools.domain.validate.sql import name_problem

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
