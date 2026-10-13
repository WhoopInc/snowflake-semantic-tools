"""The semantic checks that read files: each document's text, and the SQL file a verified query loads.

Reading is edge work, so it stays here; every check of what was read is
`domain.validate.semantic`'s.
"""

from __future__ import annotations

import posixpath

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.fields import OUTSIDE_PROJECT, sibling_within
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import malformed_field
from snowflake_semantic_tools.domain.validate.semantic.nodes import load_nodes, member_root, node_origin


def file_texts(documents: RawDocuments) -> dict[str, str]:
    """Return each document's file as UTF-8 text, by its path; one that cannot be read so is left out."""
    texts: dict[str, str] = {}
    for document in documents.documents:
        try:
            texts[document.path] = document.abs_path.read_bytes().decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue
    return texts


def _verified_query_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """Check that each `snowflake_verified_queries:` entry has SQL, and that a file it loads is usable.

    Entries come from files in any folder; one with no name is called `<unnamed>`. `sql:` is
    inline SQL, or one `{{ file('<path>') }}` call that is all of it, resolved against the
    entry's own file, which must stay inside the project without passing through a symbolic
    link.

    Raises:
        OSError: The file `sql:` names exists and cannot be read.

    Diagnostics:
        SST-PRS002: when an entry has no `sql`.
        SST-LOD004, SST-REF033, SST-REF003: when a template in `sql` does not parse.
        SST-REF003: when `sql` calls `file()` and that call is not all of it.
        SST-REF027: when the file resolves outside the project; it is not read.
        SST-PRT009: when the file is, or is reached through, a symbolic link; it is not read.
        SST-REF014: when the path names no file.
        SST-LOD019: when the file holds only whitespace.
        SST-LOD006: when the file is not UTF-8.
    """
    diagnostics: list[Diagnostic] = []
    by_path = documents.by_path
    for document, index, node in load_nodes(documents, member_root("verified_query")):
        name = str(node.get("name") or "<unnamed>")
        subject = artifact_key("verified_query", name)
        origin = node_origin(document, member_root("verified_query"), index)
        if node.get("sql") is None:
            diagnostics.append(D("SST-PRS002", artifact=subject, field="sql", subject=subject, origin=origin))
            continue
        requested = _sql_file(str(node["sql"]), origin, subject, diagnostics)
        if requested is None:
            continue
        sql_path, refusal = sibling_within(by_path[document.path].abs_path, document.path, requested)
        if refusal == OUTSIDE_PROJECT:
            diagnostics.append(D("SST-REF027", path=requested, subject=subject, origin=origin))
        elif refusal is not None:
            path = _sibling(document.path, requested)
            diagnostics.append(D("SST-PRT009", path=path, detail=refusal, subject=subject, origin=origin))
        elif not sql_path.is_file():
            diagnostics.append(D("SST-REF014", path=requested, subject=subject, origin=origin))
        elif not (content := sql_path.read_bytes()).strip():
            diagnostics.append(D("SST-LOD019", path=requested, file=document.path, subject=subject, origin=origin))
        else:
            diagnostics.extend(_utf8_diagnostics(content, document.path, requested, subject, origin))
    return tuple(diagnostics)


def _sql_file(sql: str, origin: Origin, subject: str, diagnostics: list[Diagnostic]) -> str | None:
    """Return the path a verified query's `sql:` loads, or None for inline SQL or a misused `file()`.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: when a template in `sql` does not parse; the
            expression checks report it, so nothing is added here.
        SST-REF003: when `sql` calls `file()` and that call is not all of it.
    """
    try:
        calls = scan_template_calls(sql)
    except TemplateSyntaxError:
        return None
    files = [call for call in calls if call.function == "file"]
    if not files:
        return None
    if len(calls) != 1 or calls[0].raw != sql.strip() or len(files[0].args) != 1:
        diagnostics.append(malformed_field(sql, origin, subject=subject))
        return None
    return str(files[0].args[0])


def _utf8_diagnostics(
    content: bytes, document_path: str, sql_file: str, subject: str, origin: Origin
) -> tuple[Diagnostic, ...]:
    """Report SST-LOD006 when `content` is not UTF-8, naming the file as its document's sibling path."""
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as exc:
        return (
            D("SST-LOD006", file=_sibling(document_path, sql_file), offset=exc.start, subject=subject, origin=origin),
        )
    return ()


def _sibling(document_path: str, sql_file: str) -> str:
    """Name a file a document names as the project-relative path it is, `..` taken away."""
    return posixpath.normpath(posixpath.join(posixpath.dirname(document_path), sql_file))
