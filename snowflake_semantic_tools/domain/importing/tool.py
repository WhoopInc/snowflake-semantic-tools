"""Map a live search service, procedure or function to the tool member that publishes it.

By default a member references the object (`relations:`), so SST calls it and never changes it.
With `define`, the member defines it, and SST publishes it from then on: a search service over
one dbt model as an `on:` block, a procedure's or function's body as a file beside its YAML,
each three-part name of a dbt model's relation in the body written as `{{ ref() }}`. What a
definition cannot carry -- a search query with a filter or a join, a body that does not lex --
falls back to a reference, and is reported (SST-IMP013).
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.importing.model import ImportedFile, ImportedObject, artifact_name
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.references import ProjectIndex
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.sql.lexer import LexError, TokenKind, tokenize

# The tool member type each listed object type is, as SHOW names it.
TOOL_TYPES: Mapping[str, str] = MappingProxyType(
    {"CORTEX SEARCH SERVICE": "cortex_search_service", "PROCEDURE": "procedure", "FUNCTION": "function"}
)
# The file a routine's body goes in, by its language.
_BODY_SUFFIXES: Mapping[str, str] = MappingProxyType(
    {"sql": "sql", "python": "py", "javascript": "js", "java": "java", "scala": "scala"}
)


@dataclass(frozen=True, slots=True)
class LiveTool:
    """A search service, procedure or function as Snowflake shows it.

    Attributes:
        name: The object.
        member_type: Its tool member type, a `TOOL_TYPES` value.
        comment: Its comment without SST's marker; None when it has none.
        described: What DESCRIBE reports, by lowercase property.
        signature: A routine's arguments, as `(name, type)`; empty for a search service.
    """

    name: QualifiedName
    member_type: str
    comment: str | None
    described: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    signature: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ToolPlacement:
    """Where a tool member's files go, and what it inherits there."""

    root: str
    destination: Destination
    inherited: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


def import_tool(live: LiveTool, index: ProjectIndex, placement: ToolPlacement, *, define: bool) -> ImportedObject:
    """Map a live tool object to its member file, which references it, or defines it under `define`.

    Diagnostics:
        SST-IMP013: what a definition cannot carry; the member references the object instead.
    """
    name = artifact_name(live.name)
    key = f"tool:{name}"
    folder = posixpath.join(placement.root, *placement.destination.folder, file_name(name))
    diagnostics: list[Diagnostic] = []
    body: dict[str, object] = {"name": name, "type": live.member_type}
    if live.comment:
        body["description"] = live.comment
    files: list[ImportedFile] = []
    defined = False
    if define and live.member_type == "cortex_search_service":
        defined = _define_search(live, index, body, diagnostics, key)
    elif define:
        defined = _define_routine(live, index, body, files, folder, name, placement, diagnostics, key)
    if not defined:
        _reference(live, index, body)
    if live.signature:
        body["signature"] = [{"name": arg.casefold(), "type": kind} for arg, kind in live.signature]
    if defined and placement.destination.overridden:
        database, schema, _ = live.name.folded
        config = body.setdefault("config", {})
        if isinstance(config, dict):
            config.update({"database": database, "schema": schema})
    main = ImportedFile(posixpath.join(folder, f"{file_name(name)}.yml"), {"tool": body})
    return ImportedObject("tool", live.name, name, (main, *files), diagnostics=tuple(diagnostics))


def _reference(live: LiveTool, index: ProjectIndex, body: dict[str, object]) -> None:
    """Write what a referencing member says of its object: its search columns, and its relation."""
    if live.member_type == "cortex_search_service":
        search = live.described.get("search_column", "").casefold()
        attributes = {item.casefold() for item in _split(live.described.get("attribute_columns", ""))}
        columns = [item.casefold() for item in _split(live.described.get("columns", ""))]
        if columns:
            body["columns"] = [
                {"column": column, "searchable": column == search, "filterable": column in attributes}
                for column in columns
            ]
    body["relations"] = {index.target_name: live.name.sql}


def _define_search(
    live: LiveTool, index: ProjectIndex, body: dict[str, object], diagnostics: list[Diagnostic], key: str
) -> bool:
    """Define a search service over one dbt model, when its query selects that model's columns alone."""
    query = live.described.get("definition", "")
    selected = _selection(query)
    model = index.model(selected[1]) if selected is not None else None
    if selected is None or model is None:
        diagnostics.append(
            D(
                "SST-IMP013",
                subject=key,
                artifact=key,
                detail="its query is not a SELECT of one dbt model's columns, which `on:` defines; it is referenced",
            )
        )
        return False
    columns, _ = selected
    search = live.described.get("search_column", "").casefold()
    attributes = [item.casefold() for item in _split(live.described.get("attribute_columns", ""))]
    body["on"] = {
        "table": "{{ ref('" + model + "') }}",
        "search_column": "{{ ref('" + model + "', '" + search + "') }}",
        **({"attributes": ["{{ ref('" + model + "', '" + item + "') }}" for item in attributes]} if attributes else {}),
    }
    others = [column for column in columns if column not in (search, *attributes)]
    if others:
        body["columns"] = [{"column": "{{ ref('" + model + "', '" + column + "') }}"} for column in others]
    for setting in ("warehouse", "target_lag", "embedding_model"):
        value = live.described.get(setting, "")
        if value:
            body.setdefault("config", {})
            config = body["config"]
            if isinstance(config, dict):
                config[setting] = value
    if live.described.get("refresh_mode"):
        body["refresh_mode"] = live.described["refresh_mode"].upper()
    return True


def _define_routine(
    live: LiveTool,
    index: ProjectIndex,
    body: dict[str, object],
    files: list[ImportedFile],
    folder: str,
    name: str,
    placement: ToolPlacement,
    diagnostics: list[Diagnostic],
    key: str,
) -> bool:
    """Define a procedure or function: its body as a file, its language, returns, handler and settings."""
    described = live.described
    language = described.get("language", "sql").casefold()
    source = described.get("body", "")
    suffix = _BODY_SUFFIXES.get(language)
    if not source or suffix is None:
        detail = f"its {language or 'unknown'} body cannot be read back whole; it is referenced"
        diagnostics.append(D("SST-IMP013", subject=key, artifact=key, detail=detail))
        return False
    try:
        rewritten = _relations_as_refs(source, index)
    except LexError as exc:
        detail = f"its body does not lex ({exc.reason}); it is referenced"
        diagnostics.append(D("SST-IMP013", subject=key, artifact=key, detail=detail))
        return False
    body_file = f"{file_name(name)}.{suffix}"
    files.append(ImportedFile(posixpath.join(folder, body_file), text=rewritten.rstrip("\n") + "\n"))
    body["body"] = "{{ file('" + body_file + "') }}"
    body["language"] = language
    for setting in ("runtime_version", "handler"):
        if described.get(setting):
            body[setting] = described[setting]
    if described.get("returns"):
        body["returns"] = described["returns"]
    packages = _split(described.get("packages", "").strip("[]").replace("'", ""))
    if packages:
        body["packages"] = list(packages)
    execute_as = described.get("execute as", "").casefold()
    inherited = str(placement.inherited.get("execute_as") or "caller").casefold()
    if live.member_type == "procedure" and execute_as and execute_as != inherited:
        body["config"] = {"execute_as": execute_as}
    return True


def _selection(query: str) -> tuple[tuple[str, ...], QualifiedName] | None:
    """Return a `SELECT a, b FROM DB.S.T` query's columns, casefolded, and its table; None for another."""
    try:
        tokens = [token for token in tokenize(query) if token.kind not in (TokenKind.SPACE, TokenKind.COMMENT)]
    except LexError:
        return None
    words = [token.text for token in tokens]
    upper = [word.upper() for word in words]
    if not upper or upper[0] != "SELECT" or upper.count("FROM") != 1:
        return None
    at = upper.index("FROM")
    columns = [word.casefold() for word in words[1:at] if word != ","]
    rest = words[at + 1 :]
    if len(rest) != 5 or rest[1] != "." or rest[3] != "." or not columns:
        return None
    if any(not word.replace("_", "").isalnum() for word in columns):
        return None
    try:
        return tuple(columns), QualifiedName.parse("".join(rest))
    except ValueError:
        return None


def _relations_as_refs(source: str, index: ProjectIndex) -> str:
    """Write each three-part name of a dbt model's relation in a routine's body as `{{ ref() }}`.

    Raises:
        LexError: the body does not lex.
    """
    tokens = tokenize(source)
    out: list[str] = []
    index_at = 0
    while index_at < len(tokens):
        window = tokens[index_at : index_at + 5]
        if (
            len(window) == 5
            and window[1].text == "."
            and window[3].text == "."
            and all(window[at].kind in (TokenKind.WORD, TokenKind.QUOTED_IDENTIFIER) for at in (0, 2, 4))
            and (index_at == 0 or tokens[index_at - 1].text != ".")
        ):
            try:
                relation = QualifiedName.parse("".join(token.text for token in window))
            except ValueError:
                relation = None
            model = index.model(relation) if relation is not None else None
            if model is not None:
                out.append("{{ ref('" + model + "') }}")
                index_at += 5
                continue
        out.append(tokens[index_at].text)
        index_at += 1
    return "".join(out)


def _split(text: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in text.split(",") if part.strip())
