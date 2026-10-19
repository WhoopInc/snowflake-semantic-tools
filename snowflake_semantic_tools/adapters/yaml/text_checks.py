"""What every file SST reads must be before it is text, and what `parse_yaml_bytes` checks in it.

`read_authored_text` is the one rule for an authored file's bytes, YAML, a `file()` sidecar or
a `SKILL.md` alike: within the size limit, no byte-order mark, UTF-8, and no NUL. A mark is
refused, never removed, since removing it would change what a checksum or a published body
holds. `unstorable_diagnostics` refuses a loaded value holding what Snowflake cannot store.

For YAML, the refusals raise `ProjectError` with their diagnostics: those above, and tabs used
for indentation. The formatting findings are returned instead, since the file still loads:
CRLF line endings, which are normalised in memory and never written back, and trailing
whitespace.
"""

from __future__ import annotations

import re
from collections.abc import Callable

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.text import TreePath, unstorable, unstorable_strings

# A semantic-model file this large is a generated dump, not something a person reviews.
MAX_FILE_BYTES = 4 * 1024 * 1024
_BOM = b"\xef\xbb\xbf"
# Leading spaces and `- ` sequence entries: whatever precedes a line's first key or scalar.
_ENTRY_PREFIX = re.compile(r" *(?:- +)*")


def _refuse(*diagnostics: Diagnostic) -> ProjectError:
    return ProjectError("; ".join(diagnostic.message for diagnostic in diagnostics), diagnostics=diagnostics)


def bom_refusal(raw: bytes, path: str, *, subject: str | None = None) -> Diagnostic | None:
    """Return the refusal of a file that begins with a byte-order mark; None when it does not.

    Diagnostics:
        SST-LOD017: the file begins with a UTF-8 byte-order mark.
    """
    return D("SST-LOD017", origin=Origin(path), file=path, subject=subject) if raw.startswith(_BOM) else None


def read_authored_text(raw: bytes, path: str, *, subject: str | None = None) -> str | Diagnostic:
    """Return an authored file's bytes as text, or the one diagnostic that refuses them.

    Args:
        path: The file as diagnostics name it, relative to the project root.

    Diagnostics:
        SST-LOD007: the file is larger than `MAX_FILE_BYTES`.
        SST-LOD017: the file begins with a UTF-8 byte-order mark.
        SST-LOD006: the bytes are not UTF-8, at the first offending byte.
        SST-LOD022: the text holds a NUL character, at its line.
    """
    if len(raw) > MAX_FILE_BYTES:
        expected = f"{MAX_FILE_BYTES}-byte"
        return D("SST-LOD007", origin=Origin(path), file=path, size=len(raw), expected=expected, subject=subject)
    refusal = bom_refusal(raw, path, subject=subject)
    if refusal is not None:
        return refusal
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return D("SST-LOD006", origin=Origin(path), file=path, offset=exc.start, subject=subject)
    # Strict UTF-8 decoding never yields a surrogate, so a NUL is all `unstorable` can find here.
    problem = unstorable(text)
    if problem is not None:
        line = text.count("\n", 0, text.index("\x00")) + 1
        return D("SST-LOD022", origin=Origin(path, line), file=path, field="text", found=problem, subject=subject)
    return text


def decoded_text(raw: bytes, path: str) -> str:
    """Return a YAML file's bytes as text, refusing what SST will not read.

    Raises:
        ProjectError: As `read_authored_text` refuses the bytes.

    Diagnostics:
        Every code `read_authored_text` lists.
    """
    text = read_authored_text(raw, path)
    if isinstance(text, Diagnostic):
        raise _refuse(text)
    return text


def unstorable_diagnostics(
    tree: object, path: str, line_of: Callable[[TreePath], int | None], *, subject: str | None = None
) -> tuple[Diagnostic, ...]:
    """Report each string in a loaded tree that Snowflake cannot store, at the line it is written on.

    Args:
        line_of: The line a tree path is written on; None when it is not known.

    Diagnostics:
        SST-LOD022: a string holds a NUL or a lone surrogate, once per string.
    """
    return tuple(
        D(
            "SST-LOD022",
            origin=Origin(path, line_of(at)),
            file=path,
            field=".".join(str(part) for part in at) or "document",
            found=problem,
            subject=subject,
        )
        for at, problem in unstorable_strings(tree)
    )


def normalised_text(text: str, path: str) -> tuple[str, tuple[Diagnostic, ...]]:
    """Return `text` with CRLF line endings made LF and trailing whitespace dropped, and what was found.

    Both are dropped in memory so that neither changes a published value: YAML already drops
    trailing whitespace in a plain or quoted scalar, but keeps it in a `|` block scalar, so
    without this a stray space in a description or a verified query's SQL would change the
    rendered DDL and its fingerprint. The file itself is never rewritten; `sst format` does that.

    Diagnostics:
        SST-LOD012: lines end in CRLF, or carry trailing whitespace; once for each.
        SST-LOD202: the CRLF line endings were normalised in memory, with how many.
    """
    findings: list[Diagnostic] = []
    crlf = text.count("\r\n")
    if crlf:
        text = text.replace("\r\n", "\n")
        findings.append(D("SST-LOD012", origin=Origin(path), file=path, detail=f"{crlf} line(s) end in CRLF"))
        findings.append(D("SST-LOD202", origin=Origin(path), file=path, count=crlf))
    lines = text.split("\n")
    trailing = [number for number, line in enumerate(lines, start=1) if line != line.rstrip(" \t")]
    if trailing:
        text = "\n".join(line.rstrip(" \t") for line in lines)
        findings.append(
            D(
                "SST-LOD012",
                origin=Origin(path, trailing[0]),
                file=path,
                detail=f"trailing whitespace on {len(trailing)} line(s), first at line {trailing[0]}",
            )
        )
    return text, tuple(findings)


def block_scalar_indent(line: str) -> int | None:
    """Return the column a block scalar opened on `line` is indented past, or None if it opens none."""
    entries = _ENTRY_PREFIX.match(line)
    prefix = entries.end() if entries else 0
    # Measured from the owning key or entry, not the line's first `-`, so the rest of a
    # list item after its `- key: |` block is still executable text.
    if re.search(r":\s*[>|][+-]?\s*(?:#.*)?$", line):
        return prefix
    if line[:prefix].strip() and re.fullmatch(r"[>|][+-]?\s*(?:#.*)?", line[prefix:]):
        return line.rindex("-", 0, prefix)
    return None


def executable_lines(text: str) -> list[bool]:
    """Say, line by line, whether YAML reads the line as structure rather than as text.

    A whole-line comment and the body of a block scalar are text: YAML keeps them verbatim,
    so they may hold tabs or `{{` freely.
    """
    executable: list[bool] = []
    block_indent: int | None = None
    for line in text.splitlines():
        stripped = line.lstrip()
        if block_indent is not None:
            if stripped.strip() and len(line) - len(stripped) <= block_indent:
                block_indent = None
            else:
                executable.append(False)
                continue
        if stripped.startswith("#"):
            executable.append(False)
            continue
        opened = block_scalar_indent(line)
        if opened is not None:
            block_indent = opened
        executable.append(True)
    return executable


def refuse_tab_indentation(text: str, path: str, executable: list[bool]) -> None:
    """Refuse a structural line whose indentation holds a tab, which YAML forbids.

    Raises:
        ProjectError: One SST-LOD010 per such line, in line order.
    """
    found = tuple(
        D("SST-LOD010", origin=Origin(path, number), file=path, line=number)
        for number, (line, structural) in enumerate(zip(text.splitlines(), executable, strict=True), start=1)
        if structural and line.strip() and "\t" in line[: len(line) - len(line.lstrip(" \t"))]
    )
    if found:
        raise _refuse(*found)


def unquoted_colon(text: str, path: str, line: int, col: int) -> Diagnostic | None:
    """Report the unquoted `: ` YAML stopped at, when it follows a key on the same line.

    `mapping values are not allowed here` at the first colon of a line is an indentation
    problem; at a later one it is prose such as `description: Note: ...`, which needs quotes.

    Args:
        line: 1-based, as YAML's mark gives it plus one.
        col: 0-based column of the colon.
    """
    lines = text.splitlines()
    if not 0 < line <= len(lines):
        return None
    source = lines[line - 1]
    entries = _ENTRY_PREFIX.match(source)
    start = entries.end() if entries else 0
    first = source.find(":", start)
    if first < 0 or first >= col:
        return None
    key = source[start:first].strip()
    return D("SST-LOD009", origin=Origin(path, line), file=path, line=line, key=key)
