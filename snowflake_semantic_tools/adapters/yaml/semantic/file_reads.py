"""The semantic checks that read files: each document's text, and the SQL file a verified query names.

Reading is edge work, so it stays here; every check of what was read is
`domain.validate.semantic`'s.
"""

from __future__ import annotations

import posixpath

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
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
    """Check that each `snowflake_verified_queries:` entry has exactly one SQL source, and a usable one.

    Entries come from files in any folder; one with no name is called `<unnamed>`. A
    `sql_file:` is resolved against the entry's own file.

    Raises:
        OSError: A `sql_file:` exists and cannot be read.

    Diagnostics:
        SST-VAL412: when an entry declares both `sql` and `sql_file`, or neither.
        SST-LOD018: when the `sql_file:` is not a file.
        SST-LOD019: when the `sql_file:` holds no bytes.
        SST-LOD006: when the `sql_file:` is not UTF-8.
    """
    diagnostics: list[Diagnostic] = []
    by_path = documents.by_path
    for document, index, node in load_nodes(documents, member_root("verified_query")):
        name = str(node.get("name") or "<unnamed>")
        subject = artifact_key("verified_query", name)
        origin = node_origin(document, member_root("verified_query"), index)
        has_sql = node.get("sql") is not None
        has_sql_file = node.get("sql_file") is not None
        if has_sql == has_sql_file:
            diagnostics.append(
                D(
                    "SST-VAL412",
                    member=name,
                    detail=("sql and sql_file are both present" if has_sql else "neither sql nor sql_file is present"),
                    subject=subject,
                    origin=origin,
                )
            )
            continue
        if has_sql_file:
            sql_path = by_path[document.path].abs_path.parent / str(node["sql_file"])
            if not sql_path.is_file():
                diagnostics.append(
                    D(
                        "SST-LOD018",
                        file=document.path,
                        path=str(node["sql_file"]),
                        subject=subject,
                        origin=origin,
                    )
                )
            elif not (content := sql_path.read_bytes()):
                diagnostics.append(
                    D(
                        "SST-LOD019",
                        path=str(node["sql_file"]),
                        file=document.path,
                        subject=subject,
                        origin=origin,
                    )
                )
            else:
                diagnostics.extend(_utf8_diagnostics(content, document.path, str(node["sql_file"]), subject, origin))
    return tuple(diagnostics)


def _utf8_diagnostics(
    content: bytes, document_path: str, sql_file: str, subject: str, origin: Origin
) -> tuple[Diagnostic, ...]:
    """Report SST-LOD006 when `content` is not UTF-8, naming the file as its document's sibling path."""
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as exc:
        file = posixpath.normpath(posixpath.join(posixpath.dirname(document_path), sql_file))
        return (D("SST-LOD006", file=file, offset=exc.start, subject=subject, origin=origin),)
    return ()
