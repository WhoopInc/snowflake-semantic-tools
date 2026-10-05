"""The Snowflake signature table: which SNO code a driver error is, and the diagnostic that reports it.

`SIGNATURES` is the table, one row per signature; `match_signature` is the one function
that consults it. Matching is most specific first: a row whose `errno` equals the error's
number, then a row whose `sqlstate` equals its SQLSTATE, then a row whose `pattern`
searches its message, each pass in table order. A number the driver gives several failures
matches its row only when the wording does too. An error no row matches is `UNRECOGNISED`,
SST-SNO001, and never a specific code. A row with neither a number nor a SQLSTATE matches
on Snowflake's wording alone, so it is `fragile`, and `fragile_signatures` lists those rows
so the set stays visible and can shrink as numbers are observed.

`session_failure` reads a classified error as the session sees it -- a rejected credential,
a missed deadline, a missing privilege -- so the connector's own errors are classified by
the same table. Snowflake reports an object the role may not see as one that does not
exist, so the reading is a privilege when the object is in the `SNOWFLAKE` database, which
every account has.

`snowflake_diagnostic` reports a classified error, filling the code's placeholders from the
message: the first quoted name for `{value}`, the message without its number and heading
for `{detail}`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from string import Formatter
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D, Diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import ErrorKind


@dataclass(frozen=True, slots=True)
class Signature:
    """One way Snowflake reports a failure, and the SNO code it maps to.

    Attributes:
        errno: The driver error number that identifies it; None when none was observed.
        sqlstate: The SQLSTATE that identifies it; None when it is shared or unobserved.
        pattern: The wording that identifies it when the error carries no matching number.
        kind: The class apply records for the failure.
        retryable: Whether running the same statement again may succeed.
        extract: Where the code's `{detail}` or `{found}` comes from in the message: the
            first group of this pattern; None takes the whole message.
        shared_errno: The driver gives `errno` to other failures too, so an error with it
            matches this row only when `pattern` also searches its message.
    """

    code: str
    errno: int | None = None
    sqlstate: str | None = None
    pattern: re.Pattern[str] | None = None
    kind: ErrorKind = ErrorKind.UNKNOWN
    retryable: bool = False
    extract: re.Pattern[str] | None = None
    shared_errno: bool = False

    @property
    def fragile(self) -> bool:
        """Report whether the row matches on wording alone, with no number or SQLSTATE."""
        return self.errno is None and self.sqlstate is None


def _words(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE | re.DOTALL)


_PRIVILEGE = ErrorKind.PRIVILEGE
_NOT_FOUND = ErrorKind.NOT_FOUND
_SYNTAX = ErrorKind.SYNTAX
_TRANSIENT = ErrorKind.TRANSIENT
_AUTHENTICATION = _words(r"\bincorrect username or password\b|\bauthentication (?:failed|token)")
_DURATION = _words(r"(\d+\s*(?:(?:second|minute|hour)(?:\(s\)|s)?|ms|s)(?![a-z]))")

# The table. Specific rows come before general ones that share their wording, so the
# message pass reads a missing schema as a schema before it reads it as an object.
SIGNATURES: tuple[Signature, ...] = (
    Signature("SST-SNO002", 2002, "42710", _words(r"\balready exists\b")),
    Signature("SST-SNO005", 2043, None, _words(r"\bschema\s+'[^']*'\s+does not exist"), _NOT_FOUND),
    Signature("SST-SNO006", None, None, _words(r"\bdatabase\s+'[^']*'\s+does not exist"), _NOT_FOUND),
    Signature("SST-SNO007", None, None, _words(r"\bwarehouse\s+'[^']*'\s+does not exist"), _NOT_FOUND),
    Signature("SST-SNO008", None, None, _words(r"\bno active warehouse\b"), _NOT_FOUND),
    Signature("SST-SNO003", 2003, "02000", _words(r"\bdoes not exist\b"), _NOT_FOUND),
    Signature("SST-SNO003", None, "42S02", None, _NOT_FOUND),
    Signature("SST-SNO017", None, None, _words(r"\bCREATE AGENT\b.*\b(?:privilege|required)\b"), _PRIVILEGE),
    Signature("SST-SNO018", None, None, _words(r"\bCREATE DATASET\b.*\b(?:privilege|required)\b"), _PRIVILEGE),
    Signature("SST-SNO004", 3001, "42501", _words(r"\binsufficient privileges?\b|\bnot authori[sz]ed\b"), _PRIVILEGE),
    Signature("SST-SNO004", None, "28000", None, _PRIVILEGE),
    Signature("SST-SNO012", 93932, None, _words(r"\bsecure\b.*\bshare\b|\bshare\b.*\bsecure\b"), _PRIVILEGE),
    # 250001 is the driver's number for every failed login, a network failure included.
    Signature("SST-SNO013", 250001, None, _AUTHENTICATION, _PRIVILEGE, shared_errno=True),
    # Snowflake's numbers for a credential it rejected at login: a wrong password, an expired or
    # invalid token, a rejected key pair, and a failed SSO or MFA exchange.
    *(
        Signature("SST-SNO013", number, None, None, _PRIVILEGE)
        for number in (390100, 390144, 390195, 390302, 390303, 390318, 390422)
    ),
    Signature("SST-SNO011", 630, "57014", _words(r"\bstatement or warehouse timeout\b"), extract=_DURATION),
    Signature(
        "SST-SNO015", None, None, _words(r"\bmax_staleness\b"), extract=_words(r"max_staleness\W*'?(\d+(?:\s*[a-z]+)?)")
    ),
    Signature(
        "SST-SNO016",
        None,
        None,
        _words(r"\bunsupported feature\b.*\bsemantic view|\bsemantic views?\b.*\bnot (?:enabled|available)\b"),
    ),
    Signature("SST-SNO019", None, None, _words(r"\bduplicate synonym\b")),
    Signature("SST-SNO020", None, None, _words(r"\bidentifier\b.*\btoo long\b|\bexceeds? the maximum length\b")),
    Signature("SST-SNO023", None, None, _words(r"\bresult (?:set )?(?:is )?too large\b")),
    Signature(
        "SST-SNO024",
        None,
        None,
        _words(r"\bqueued\b.*\b(?:beyond|exceed|timeout)|\bstatement_queued_timeout"),
        _TRANSIENT,
        extract=_DURATION,
    ),
    Signature(
        "SST-SNO022",
        None,
        None,
        _words(r"\block\b.*\b(?:timeout|wait)|\b(?:timeout|wait).*\block\b|\bdeadlock\b|\bconcurrent\b"),
        _TRANSIENT,
        True,
    ),
    Signature("SST-SNO010", None, None, _words(r"\bSQL execution internal error\b|\bincident\s+\d+")),
    Signature("SST-SNO009", 1003, "42000", _words(r"\bsyntax error\b|\bSQL compilation error\b"), _SYNTAX),
    Signature("SST-SNO009", None, "42601", None, _SYNTAX),
    # The SQL-standard connection-exception states: 250002 is the driver closing a connection
    # under a session, as 08003 says; 08006 is a connection that failed mid-statement.
    Signature("SST-SNO014", 250002, "08003", None, _TRANSIENT, True),
    Signature("SST-SNO014", None, "08000", None, _TRANSIENT, True),
    Signature("SST-SNO014", None, "08006", None, _TRANSIENT, True),
    Signature(
        "SST-SNO014",
        None,
        "08001",
        _words(
            r"\btime(?:d)? ?out\b|\bconnection (?:aborted|reset|refused)\b|\bfailed to (?:connect|execute request)\b"
        ),
        _TRANSIENT,
        True,
    ),
)

UNRECOGNISED = Signature("SST-SNO001")

_NUMBER_PREFIX = re.compile(r"^\s*\d{3,6}\s*(?:\([0-9A-Z]{5}\))?\s*:\s*")
_HEADING = _words(r"^\s*(?:SQL compilation error|SQL execution internal error)\s*:\s*")
_QUOTED = re.compile(r"'([^']+)'")


def match_signature(message: str, *, errno: int | None = None, sqlstate: str | None = None) -> Signature:
    """Return the most specific row matching a driver error; `UNRECOGNISED` when none does.

    The number is tried first, then the SQLSTATE, then the wording, each in table order.
    """
    if errno is not None:
        for row in SIGNATURES:
            if row.errno == errno and not (row.shared_errno and not _searches(row, message)):
                return row
    if sqlstate:
        for row in SIGNATURES:
            if row.sqlstate == sqlstate:
                return row
    for row in SIGNATURES:
        if _searches(row, message):
            return row
    return UNRECOGNISED


def _searches(row: Signature, message: str) -> bool:
    return row.pattern is not None and row.pattern.search(message) is not None


class SessionFailure(Enum):
    """What a driver error means to the session that ran into it."""

    AUTHENTICATION = "authentication"
    DEADLINE = "deadline"
    PRIVILEGE = "privilege"
    NOT_VISIBLE = "not_visible"


# The codes of an object the session's role cannot see: it is absent or not granted.
_NOT_VISIBLE = frozenset(("SST-SNO003", "SST-SNO005", "SST-SNO006"))
# The numbers and SQLSTATEs that identify those failures, rather than their wording alone.
_NOT_VISIBLE_ERRNOS = frozenset(row.errno for row in SIGNATURES if row.code in _NOT_VISIBLE and row.errno)
_NOT_VISIBLE_SQLSTATES = frozenset(row.sqlstate for row in SIGNATURES if row.code in _NOT_VISIBLE and row.sqlstate)
# The database Snowflake shares into every account, such as `SNOWFLAKE.ACCOUNT_USAGE`: it is
# never absent, so failing to see it, or anything in it, is a privilege the role lacks.
_SYSTEM_DATABASE = "SNOWFLAKE"


def session_failure(message: str, *, errno: int | None = None, sqlstate: str | None = None) -> SessionFailure | None:
    """Read a driver error, as `match_signature` classifies it, as what it means to the session.

    Returns:
        AUTHENTICATION for a rejected credential (SST-SNO013); NOT_VISIBLE for a database,
        schema or object that does not exist or is not granted; DEADLINE for a transient
        failure or a statement timeout; PRIVILEGE for any other refused privilege, and for a
        not-visible failure its number or SQLSTATE identifies on the `SNOWFLAKE` database or
        anything in it; None for anything else.
    """
    signature = match_signature(message, errno=errno, sqlstate=sqlstate)
    if signature.code == "SST-SNO013":
        return SessionFailure.AUTHENTICATION
    if signature.code in _NOT_VISIBLE:
        identified = errno in _NOT_VISIBLE_ERRNOS or sqlstate in _NOT_VISIBLE_SQLSTATES
        if identified and _names_system_database(message):
            return SessionFailure.PRIVILEGE
        return SessionFailure.NOT_VISIBLE
    if signature.kind is ErrorKind.TRANSIENT or signature.code == "SST-SNO011":
        return SessionFailure.DEADLINE
    if signature.kind is ErrorKind.PRIVILEGE:
        return SessionFailure.PRIVILEGE
    return None


def _names_system_database(message: str) -> bool:
    """Report whether the first name a message quotes is the `SNOWFLAKE` database or is in it."""
    quoted = _QUOTED.search(message)
    return quoted is not None and quoted.group(1).split(".", 1)[0] == _SYSTEM_DATABASE


def fragile_signatures() -> tuple[Signature, ...]:
    """Return the rows that match on wording alone, in table order."""
    return tuple(row for row in SIGNATURES if row.fragile)


def detail_of(message: str) -> str:
    """Return a driver message without its leading error number and its SQL error heading."""
    return _HEADING.sub("", _NUMBER_PREFIX.sub("", message), count=1).strip()


def signature_codes() -> frozenset[str]:
    """Return every code a driver error can map to, SST-SNO001 included."""
    return frozenset((UNRECOGNISED.code, *(row.code for row in SIGNATURES)))


def snowflake_diagnostic(code: str, message: str, *, value: str, subject: str | None = None) -> Diagnostic:
    """Report a driver error under the SNO code it was classified as.

    Args:
        code: A code of `signature_codes`, as `match_signature` chose it.
        value: What `{value}` names when the message quotes no name: the object the failed
            statement addressed.
        subject: The artifact key the failure belongs to; None when there is none.

    Raises:
        KeyError: `code` is not a code of `signature_codes`.

    Diagnostics:
        Any SNO code of `SIGNATURES`, or SST-SNO001 for `UNRECOGNISED`.
    """
    if code not in signature_codes():
        raise KeyError(code)
    extract = next((row.extract for row in SIGNATURES if row.code == code and row.extract is not None), None)
    quoted = _QUOTED.search(message)
    extracted = extract.search(message) if extract is not None else None
    specific = extracted.group(1).strip() if extracted is not None else detail_of(message)
    filled = {"value": quoted.group(1) if quoted is not None else value, "detail": specific, "found": specific}
    fields = {field for _, field, _, _ in Formatter().parse(ERROR_REGISTRY[code].template) if field}
    context: dict[str, Any] = {key: text for key, text in filled.items() if key in fields}
    return D(code, subject=subject, **context)
