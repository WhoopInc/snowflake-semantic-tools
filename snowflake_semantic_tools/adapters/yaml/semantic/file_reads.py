"""Each document's file text, and the shape checks of a verified query's `sql:`.

Reading is edge work, so it stays here; every check of what was read is
`domain.validate.semantic`'s.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
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
    """Check that each verified query has SQL, and that a `file()` in it is all of it.

    Entries come from files in any folder; one with no name is called `<unnamed>`. Nothing is
    read here: `load_verified_queries` reads the file a `sql:` names, once, and reports it.

    An entry with no name is SST-PRS107's to report, as for every member type.

    Diagnostics:
        SST-PRS002: when an entry has no `question`, or one that is blank; or has no `sql`.
        SST-REF003: when `sql` calls `file()` and that call is not all of it, or names other
            than one path.
    """
    diagnostics: list[Diagnostic] = []
    for document, index, node in load_nodes(documents, member_root("verified_query")):
        name = str(node.get("name") or "<unnamed>")
        subject = artifact_key("verified_query", name)
        origin = node_origin(document, member_root("verified_query"), index)
        # The reader skips an entry without a question, so it would publish nowhere unreported.
        if not str(node.get("question") or "").strip():
            diagnostics.append(D("SST-PRS002", artifact=subject, field="question", subject=subject, origin=origin))
        if node.get("sql") is None:
            diagnostics.append(D("SST-PRS002", artifact=subject, field="sql", subject=subject, origin=origin))
            continue
        sql = str(node["sql"])
        try:
            calls = scan_template_calls(sql)
        except TemplateSyntaxError:
            # The expression checks report a template that does not parse.
            continue
        files = [call for call in calls if call.function == "file"]
        if files and (len(calls) != 1 or calls[0].raw != sql.strip() or len(files[0].args) != 1):
            diagnostics.append(malformed_field(sql, origin, subject=subject))
    return tuple(diagnostics)
