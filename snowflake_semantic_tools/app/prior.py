"""Read what a planned change replaces, for `plan --capture-prior`, in a form it can be restored from.

Each kind is read the way it is published:

- A semantic view, Cortex search service, procedure, or function is replaced, so its prior is
  what GET_DDL prints: the CREATE OR REPLACE that re-creates it, with names fully qualified.
  A routine is addressed by its argument types. GET_DDL prints no `COPY GRANTS`, so running
  its text as returned would revoke every grant on the object. A semantic view's text is
  therefore put in the form the server re-executes, which also moves `MAX_STALENESS` after the
  AI clauses (docs/design/snowflake-behavior.md), and a routine's gains `COPY GRANTS` after
  its argument list. A search service takes no `COPY GRANTS`, so its prior is followed by the
  GRANT statements that give back the grants plan read on it. A text of any other shape is
  kept as returned, under a comment that says what to check before running it.
- An agent is never replaced: an update adds a version and keeps every earlier one. GET_DDL
  would print a CREATE OR REPLACE AGENT of one version, which would drop the others and the
  agent's grants, so the prior is instead the version the agent serves now, read from SHOW
  VERSIONS IN AGENT: its specification, and the statements that point callers back at it.
- A stage tool is only ever created when absent, and GET_DDL cannot read a stage, so it has
  no prior to capture.
"""

from __future__ import annotations

import json
import re

from snowflake_semantic_tools.app.grant_sql import grant_statement
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, RenderedArtifact, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions, CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.semantic_view import TRAILING_CLAUSES
from snowflake_semantic_tools.domain.sql import ident, literal, qname, sql
from snowflake_semantic_tools.domain.sql.lexer import LexError, Token, TokenKind, tokenize

# The object types apply never alters once they exist, so there is no prior to read.
_NEVER_ALTERED = frozenset(("STAGE",))
_VERSION_NUMBER = re.compile(r"VERSION\$(\d+)", re.IGNORECASE)
_UNLISTED = AgentVersions()
_VIEW_HEADER = ("CREATE", "OR", "REPLACE", "SEMANTIC", "VIEW")
_REPLACE = ("CREATE", "OR", "REPLACE")
_ROUTINES = frozenset(("PROCEDURE", "FUNCTION"))
# Inserted after a routine's argument list, where SST's own render puts it.
_COPY_GRANTS_CLAUSE = " COPY GRANTS"
# The clauses before the trailing ones, each a keyword and a parenthesised list.
_VIEW_BLOCKS = frozenset(("TABLES", "RELATIONSHIPS", "VARIABLES", "FACTS", "DIMENSIONS", "METRICS"))
# Each trailing clause by its first keyword, with the keyword a two-word one must have next.
_TRAILING_STARTS = {clause.split()[0]: clause for clause in TRAILING_CLAUSES}
# A trailing clause by the keyword it starts with when its optional first one is left out: the
# grammar is `[ [ WITH ] TAG ( ... ) ]`.
_BARE_STARTS = {"TAG": "WITH TAG"}
_NAME_PARTS = frozenset((TokenKind.WORD, TokenKind.QUOTED_IDENTIFIER))
_VALUES = frozenset((TokenKind.STRING, TokenKind.DOLLAR_STRING, TokenKind.NUMBER))


def prior_definition(
    port: CatalogPort,
    object_type: str,
    row: ShowRow,
    desired_artifact: RenderedArtifact | None,
    agent_versions: AgentVersions = _UNLISTED,
    grants: tuple[GrantRow, ...] | None = (),
) -> tuple[str | None, tuple[Diagnostic, ...]]:
    """Read the definition of an object this plan may change; None when it is not one or cannot be read.

    An agent's prior comes from `agent_versions`, the SHOW VERSIONS IN AGENT read the
    observation already made for its update, so the agent is listed once. A search service's
    is followed by a GRANT for each explicit grant in `grants`, the grants observation read on
    it; None, for grants not read, leaves a comment saying to grant them again by hand.

    A read that fails is reported and leaves the definition None; the plan goes on.

    Diagnostics:
        SST-PLN037: the definition could not be read, or the agent lists no version.
    """
    if desired_artifact is None or object_type in _NEVER_ALTERED:
        return None, ()
    if object_type == "AGENT":
        prior = agent_prior(row.qualified_name, agent_versions.versions, desired_artifact.desired_alias)
        if prior is None:
            return None, (D("SST-PLN037", value=row.qualified_name.sql, detail="the agent lists no version"),)
        return prior, ()
    try:
        text = port.get_ddl(object_type, row.qualified_name, desired_artifact.routine_signature)
    except SnowflakePortError as exc:
        return None, (D("SST-PLN037", value=row.qualified_name.sql, detail=str(exc)),)
    # Snowflake's docs do not say GET_DDL rewrites a semantic view; the server's output fails
    # to re-execute as returned (docs/design/snowflake-behavior.md).
    if object_type == "SEMANTIC VIEW":
        return restorable_semantic_view(text), ()
    if object_type in _ROUTINES:
        return restorable_routine(text), ()
    if object_type == "CORTEX SEARCH SERVICE":
        return regranted(text, object_type, row.qualified_name, grants), ()
    return text, ()


def restorable_routine(text: str) -> str:
    """Add `COPY GRANTS` to a procedure's or function's GET_DDL text, so running it keeps the grants.

    It goes right after the argument list, where SST's render puts it. The text is lexed, so a
    parenthesis inside a string or a quoted name never ends the list; text that already says
    COPY GRANTS there comes back unchanged. Text of another shape comes back as returned under
    a comment that says to add it, rather than rewritten on a guess.
    """
    try:
        tokens = tokenize(text)
    except LexError as exc:
        return _ungranted(text, f"it does not lex: {exc}")
    significant = [token for token in tokens if token.kind not in (TokenKind.SPACE, TokenKind.COMMENT)]
    words = [token.text.upper() if token.kind is TokenKind.WORD else "" for token in significant]
    if tuple(words[: len(_REPLACE)]) != _REPLACE:
        return _ungranted(text, "it is not a CREATE OR REPLACE")
    kind = next((index for index, word in enumerate(words) if word in _ROUTINES), None)
    if kind is None or any(not word for word in words[len(_REPLACE) : kind]):
        return _ungranted(text, "it does not create a procedure or function")
    opening = _name_end(significant, kind + 1)
    if opening >= len(significant) or significant[opening].text != "(":
        return _ungranted(text, "its name is not followed by its argument list")
    closing = _closing(significant, opening)
    if closing is None:
        return _ungranted(text, "its argument list does not close")
    if words[closing + 1 : closing + 3] == ["COPY", "GRANTS"]:
        return text
    end = significant[closing].offset + 1
    return "".join((text[:end], _COPY_GRANTS_CLAUSE, text[end:]))


def _closing(tokens: list[Token], opening: int) -> int | None:
    """The index of the parenthesis that closes the one at `opening`; None when none does."""
    depth = 0
    for index in range(opening, len(tokens)):
        depth += {"(": 1, ")": -1}.get(tokens[index].text, 0)
        if depth == 0:
            return index
    return None


def _ungranted(text: str, reason: str) -> str:
    return (
        f"-- SST did not add COPY GRANTS to this definition: {reason}.\n"
        "-- Before running it, add COPY GRANTS after the argument list, which keeps the routine's grants.\n" + text
    )


def regranted(text: str, object_type: str, name: QualifiedName, grants: tuple[GrantRow, ...] | None) -> str:
    """Follow a replaced object's definition with the GRANTs that give back its explicit grants.

    A grant SQL cannot spell, which SHOW GRANTS never prints, is named in a comment instead of
    granted. None, for grants plan did not read, leaves a comment saying to grant them again.
    """
    body = text.rstrip()
    if grants is None:
        return body + "\n-- SST did not read this object's grants; running this revokes them, so grant them again.\n"
    lines: list[str] = []
    for grant in sorted(item for item in grants if item.is_explicit):
        try:
            lines.append(f"{grant_statement(object_type, name, grant)};")
        except ValueError:
            lines.append(f"-- grant {grant.privilege} to {grant.granted_to} {grant.grantee_name} again by hand.")
    if not lines:
        return text
    return (
        body + "\n-- Running the replace above revokes these grants; they give them back:\n" + "\n".join(lines) + "\n"
    )


def restorable_semantic_view(text: str) -> str:
    """Put a semantic view's GET_DDL text in the form the server re-executes, keeping its grants.

    The trailing clauses move into `TRAILING_CLAUSES` order, the order SST renders, and a
    `COPY GRANTS` is appended when absent, where SST's render puts it. Text already in that
    form comes back unchanged. The text is lexed, so a keyword inside a string, a quoted
    identifier, or a parenthesised list never splits a clause.

    Only the shape GET_DDL prints is rewritten: a CREATE OR REPLACE SEMANTIC VIEW whose name is
    followed by keyword clauses, each a parenthesised list or a value. Any other text comes
    back as returned under a comment naming what to check before running it, rather than
    rewritten on a guess.
    """
    try:
        tokens = tokenize(text)
    except LexError as exc:
        return _unnormalized(text, f"it does not lex: {exc}")
    significant = [token for token in tokens if token.kind is not TokenKind.SPACE]
    header = tuple(token.text.upper() for token in significant[: len(_VIEW_HEADER)] if token.kind is TokenKind.WORD)
    if header != _VIEW_HEADER:
        return _unnormalized(text, "it is not a CREATE OR REPLACE SEMANTIC VIEW")
    clauses = _view_clauses(significant, _name_end(significant, len(_VIEW_HEADER)))
    if isinstance(clauses, str):
        return _unnormalized(text, clauses)
    starts, body_end = clauses
    trailing = [(name, start) for name, start in starts if name in TRAILING_CLAUSES]
    names = [name for name, _ in trailing]
    if names == sorted(names, key=TRAILING_CLAUSES.index) and "COPY GRANTS" in names:
        return text
    bounds = [start for _, start in trailing] + [body_end]
    pieces = {name: text[start:end].strip() for (name, start), end in zip(trailing, bounds[1:], strict=True)}
    pieces.setdefault("COPY GRANTS", "COPY GRANTS")
    head = text[: bounds[0]].rstrip()
    tail = "".join(f"\n  {pieces[name]}" for name in TRAILING_CLAUSES if name in pieces)
    return head + tail + text[body_end:]


def _name_end(tokens: list[Token], index: int) -> int:
    """The index past the dotted name that starts at `index`."""
    while index < len(tokens) and tokens[index].kind in _NAME_PARTS:
        index += 1
        if index < len(tokens) and tokens[index].text == ".":
            index += 1
        else:
            break
    return index


def _view_clauses(tokens: list[Token], index: int) -> tuple[list[tuple[str, int]], int] | str:
    """Each top-level clause after the name, by name and offset, and the offset the body ends at.

    The body ends past its last token before a closing `;`. A reason, instead, when the text
    is not the shape GET_DDL prints: a keyword SST does not know, a block after a trailing
    clause, a clause given twice, a comment between clauses, or unbalanced parentheses.
    """
    end = len(tokens)
    if end > index and tokens[-1].text == ";":
        end -= 1
    starts: list[tuple[str, int]] = []
    depth = 0
    while index < end:
        token = tokens[index]
        index += 1
        if token.text in {"(", ")"}:
            depth += 1 if token.text == "(" else -1
            if depth < 0:
                return "its parentheses do not balance"
            continue
        # A value is a string, or a bare number such as `MAX_STALENESS = 300`.
        if depth > 0 or token.kind in _VALUES or token.text == "=":
            continue
        if token.kind is TokenKind.COMMENT:
            return "it has a comment between its clauses"
        clause = _clause_at(tokens, index, end)
        if isinstance(clause, str):
            return clause
        name, index = clause
        if name in _VIEW_BLOCKS and any(started in TRAILING_CLAUSES for started, _ in starts):
            return f"its {name} comes after a trailing clause"
        if any(started == name for started, _ in starts):
            return f"it gives {name} twice"
        starts.append((name, token.offset))
    if depth:
        return "its parentheses do not balance"
    last = tokens[end - 1]
    return starts, last.offset + len(last.text)


def _clause_at(tokens: list[Token], index: int, end: int) -> tuple[str, int] | str:
    """The clause the keyword before `index` starts, and the index past it; a reason if it starts none."""
    token = tokens[index - 1]
    word = token.text.upper() if token.kind is TokenKind.WORD else None
    bare = _BARE_STARTS.get(word or "")
    name = bare or (word if word in _VIEW_BLOCKS else _TRAILING_STARTS.get(word or ""))
    if name is None:
        return f"it has {token.text!r} where SST expects a clause"
    follow = [] if bare else name.split()[1:]
    if follow:
        if index >= end or tokens[index].text.upper() != follow[0]:
            return f"its {word} is not {name}"
        index += 1
    return name, index


def _unnormalized(text: str, reason: str) -> str:
    return (
        f"-- SST did not put this definition in the form the server re-executes: {reason}.\n"
        "-- Before running it, check that it ends with COPY GRANTS, which keeps the view's grants,\n"
        "-- and that MAX_STALENESS comes after the AI clauses.\n" + text
    )


def agent_prior(agent: QualifiedName, versions: tuple[AgentVersion, ...], alias: str | None) -> str | None:
    """Describe the version an agent serves now, and how to point callers back at it after apply.

    The served version is the release `served_version` names: the one carrying `alias`, the
    alias apply moves, else the default version, else the last committed release, else the
    live version; an eval-model version is never named. The text names it, gives the
    ALTER AGENT statements that restore the alias and the default version as they are now,
    then the version's specification as indented JSON, as SST renders one. None when the
    agent lists no version.
    """
    named = _releases(versions)
    aliased = _aliased(named, alias)
    default = next((version for version in named if version.is_default), None)
    last = max(named, key=_version_number, default=None)
    served = served_version(versions, alias)
    if served is None:
        return None
    lines = [f"-- agent {qname(agent)} serves {_described(served, last)}."]
    restore = []
    if aliased is not None and alias is not None:
        restore.append(_restore_alias(agent, aliased, alias))
    if default is not None:
        restore.append(_restore_default(agent, default))
    elif served.name is not None and aliased is None:
        restore.append(_restore_default(agent, served))
    if restore:
        lines.append("-- An update adds a version and keeps this one; to roll back after apply, run:")
        lines.extend(restore)
    else:
        lines.append("-- An update commits the live version before it adds one, so this one is kept.")
    if served.specification is None:
        lines.append(f"-- The role may not read the specification of {_label(served)}.")
    else:
        lines.append(f"-- The specification of {_label(served)}:")
        lines.append(_specification(served.specification))
    return "\n".join(lines) + "\n"


def _described(version: AgentVersion, last: AgentVersion | None) -> str:
    """Name a version with what makes it served: its alias, being the default, or being last."""
    roles = []
    if version.alias:
        roles.append(f"alias {version.alias}")
    if version.is_default:
        roles.append("the default")
    elif version is last:
        roles.append("the last committed")
    return _label(version) + (f" ({', '.join(roles)})" if roles else "")


def _label(version: AgentVersion) -> str:
    return version.name or "the live version"


def _restore_alias(agent: QualifiedName, version: AgentVersion, alias: str) -> str:
    statement = sql(
        "ALTER AGENT {agent} MODIFY VERSION {version} SET ALIAS = {alias};",
        agent=qname(agent),
        version=ident(Identifier.parse(str(version.name))),
        alias=ident(Identifier.parse(alias)),
    )
    return str(statement)


def _restore_default(agent: QualifiedName, version: AgentVersion) -> str:
    statement = sql(
        "ALTER AGENT {agent} SET DEFAULT_VERSION = {version};",
        agent=qname(agent),
        version=literal(str(version.name)),
    )
    return str(statement)


def served_version(versions: tuple[AgentVersion, ...], alias: str | None) -> AgentVersion | None:
    """Return the release an agent serves: the one carrying `alias`, else the default, else the last.

    Only releases count: an eval-model version is never what SST published, even where a hand
    edit made it the default. The live version when there is no committed release; None when
    the agent lists no version.
    """
    named = _releases(versions)
    default = next((version for version in named if version.is_default), None)
    last = max(named, key=_version_number, default=None)
    live = next((version for version in versions if version.name is None), None)
    return _aliased(named, alias) or default or last or live


def _releases(versions: tuple[AgentVersion, ...]) -> tuple[AgentVersion, ...]:
    """Return the committed versions SST released: every one but an eval-model version, in order."""
    return tuple(version for version in versions if version.name is not None and not version.is_eval_model)


def _aliased(named: tuple[AgentVersion, ...], alias: str | None) -> AgentVersion | None:
    return next(
        (version for version in named if alias and version.alias and version.alias.casefold() == alias.casefold()),
        None,
    )


def _version_number(version: AgentVersion) -> int:
    match = _VERSION_NUMBER.fullmatch(version.name or "")
    return int(match.group(1)) if match else -1


def _specification(text: str) -> str:
    """Print a specification as indented JSON, as SST renders one; text that is not JSON as it is."""
    try:
        return json.dumps(json.loads(text), indent=2, ensure_ascii=False)
    except ValueError:
        return text.rstrip("\n")
