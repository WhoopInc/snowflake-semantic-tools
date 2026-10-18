"""A tool's definition: what SST renders for a routine or search service, and what DESCRIBE shows.

Each is one member, named by the tool. A routine compares its signature, return type, language,
handler settings, `EXECUTE AS`, comment and body; a search service its columns, settings, query and
comment. A comment compares with each run of whitespace collapsed, as SST renders it, and without
its ownership marker. What DESCRIBE has not been seen to report -- a routine's external access
integrations and secrets -- is left out of both sides, as is a setting the project leaves to
Snowflake's default, which DESCRIBE reports all the same.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.lifecycle.marker import unmarked_comment
from snowflake_semantic_tools.domain.model.tool import ToolMember
from snowflake_semantic_tools.domain.plan.member_diff import Definition, Member, Value

# The languages whose routines name a runtime version and a handler, as the renderer knows them.
_HANDLER_LANGUAGES = frozenset(("java", "python", "scala"))


def desired_routine(member: ToolMember, *, procedure: bool) -> Definition:
    """Return the definition SST renders for a procedure or function member."""
    kind = "procedure" if procedure else "function"
    signature = ", ".join(f"{Identifier.parse(item.name).sql} {item.type}" for item in member.signature)
    properties: list[tuple[str, Value]] = [
        ("signature", _type_text(f"({signature})")),
        ("returns", _type_text(member.returns or "")),
        ("language", (member.language or "").upper()),
    ]
    if (member.language or "").strip().casefold() in _HANDLER_LANGUAGES:
        properties += [
            ("runtime_version", member.runtime_version or ""),
            ("handler", member.handler or ""),
            ("packages", member.packages),
            ("imports", member.imports),
        ]
    if procedure:
        properties.append(("execute_as", (member.execute_as or "caller").upper()))
    properties += [("comment", _one_line(member.description or "")), ("body", _body(member.body or ""))]
    return Definition((Member(kind, member.name.casefold(), tuple(properties)),))


def live_routine(name: str, described: Mapping[str, str], comment: str | None, *, procedure: bool) -> Definition:
    """Return the definition DESCRIBE PROCEDURE or FUNCTION shows, with the comment SHOW listed."""
    kind = "procedure" if procedure else "function"
    properties: list[tuple[str, Value]] = [
        ("signature", _type_text(described.get("signature", ""))),
        ("returns", _type_text(described.get("returns", ""))),
        ("language", described.get("language", "").upper()),
    ]
    for prop in ("runtime_version", "handler"):
        if prop in described:
            properties.append((prop, described[prop]))
    for prop in ("packages", "imports"):
        if prop in described:
            properties.append((prop, _listed(described[prop])))
    if procedure and "execute as" in described:
        properties.append(("execute_as", described["execute as"].upper()))
    properties += [("comment", _one_line(unmarked_comment(comment))), ("body", _body(described.get("body", "")))]
    return Definition((Member(kind, name.casefold(), tuple(properties)),))


def desired_search(member: ToolMember, query: str) -> Definition:
    """Return the definition SST renders for a search service member, whose query is `query`."""
    columns = dict.fromkeys((*(column.name for column in member.columns), *member.attribute_columns))
    selected = tuple(_column(name) for name in (*columns, member.search_column or "") if name)
    properties: list[tuple[str, Value]] = [
        ("search_column", _column(member.search_column or "")),
        ("attribute_columns", tuple(_column(name) for name in member.attribute_columns)),
        ("columns", tuple(dict.fromkeys(selected))),
        ("warehouse", _column(member.warehouse or "")),
        ("target_lag", member.target_lag or ""),
    ]
    if member.embedding_model:
        properties.append(("embedding_model", member.embedding_model))
    if member.refresh_mode:
        properties.append(("refresh_mode", member.refresh_mode.upper()))
    properties += [("query", query), ("comment", _one_line(member.description or ""))]
    return Definition((Member("search_service", member.name.casefold(), tuple(properties)),))


def live_search(name: str, described: Mapping[str, str]) -> Definition:
    """Return the definition DESCRIBE CORTEX SEARCH SERVICE shows, from its one row."""
    properties: list[tuple[str, Value]] = [
        ("search_column", described.get("search_column", "")),
        ("attribute_columns", _split(described.get("attribute_columns", ""))),
        ("columns", _split(described.get("columns", ""))),
        ("warehouse", described.get("warehouse", "")),
        ("target_lag", described.get("target_lag", "")),
        ("embedding_model", described.get("embedding_model", "")),
        ("refresh_mode", described.get("refresh_mode", "").upper()),
        ("query", described.get("definition", "")),
        ("comment", _one_line(unmarked_comment(described.get("comment")))),
    ]
    return Definition((Member("search_service", name.casefold(), tuple(properties)),))


def search_query(statement: str) -> str:
    """Return the query a rendered `CREATE CORTEX SEARCH SERVICE` statement ends with, after `AS`."""
    return statement.rpartition("\n  AS ")[2].strip()


def _column(name: str) -> str:
    return Identifier.parse(name).folded if name else ""


def _split(text: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in text.split(",") if part.strip())


def _listed(text: str) -> tuple[str, ...]:
    """A list DESCRIBE prints as text, such as `['pandas', 'numpy']` or `[]`."""
    inner = text.strip().removeprefix("[").removesuffix("]")
    return tuple(part.strip().strip("'\"") for part in inner.split(",") if part.strip())


def _type_text(text: str) -> str:
    """A signature or type with its case and spacing made uniform: `(ID VARCHAR)`."""
    collapsed = re.sub(r"\s+", " ", text.upper()).strip()
    return re.sub(r"\s*([(),])\s*", lambda match: ", " if match.group(1) == "," else match.group(1), collapsed)


def _body(text: str) -> str:
    """A routine body without the newlines the renderer wraps it in, or its trailing space."""
    return text.rstrip().strip("\n")


def _one_line(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
