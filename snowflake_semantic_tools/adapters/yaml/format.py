"""SST's canonical YAML form: what `sst format` writes and what SST-VAL009 checks a file against.

The canonical form is a re-layout that never changes what the file means:

- line feeds only, no trailing whitespace, and exactly one newline at the end of a non-empty file;
- dbt's own indentation: mappings by two spaces, a list item's dash two spaces in;
- `name` first in every mapping that is a list item and carries no comment, the rest in the
  order written;
- every multi-line string that is not already a block scalar as a literal block scalar, `|-` (or
  `|` when it ends in a newline); a block scalar keeps the style it was written in, folded or
  literal, so the formatter never rewrites how an author chose to write a value.

Comments, quoting, and the order of everything else survive the round trip. `canonical_yaml`
checks the result against the original before returning it: both must load to the same value,
or the file is refused rather than rewritten. With `sanitize`, two repairs that do change values
are made first -- apostrophes leave `synonyms` and `sample_values` entries, and Jinja delimiters
in a `description` are broken apart -- and the check compares against the repaired value.
"""

from __future__ import annotations

import io
import re

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError, YAMLError
from ruamel.yaml.scalarstring import (
    DoubleQuotedScalarString,
    FoldedScalarString,
    LiteralScalarString,
    ScalarString,
    SingleQuotedScalarString,
)

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D, Origin

# dbt's layout, which enrich also writes new files in.
_INDENT = {"mapping": 2, "sequence": 4, "offset": 2}
# Wide enough that no line is ever folded where the file did not fold it.
_WIDTH = 4096
_FIRST_KEY = "name"
_APOSTROPHE_LISTS = frozenset(("synonyms", "sample_values"))
_APOSTROPHES = re.compile("['\u2019]")
_JINJA = re.compile(r"\{([{%#])|([}%#])\}")


def _layout() -> YAML:
    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    yaml.width = _WIDTH
    yaml.indent(**_INDENT)
    # An explicit `null` stays written; the round-trip dumper would leave the value empty.
    yaml.representer.add_representer(type(None), _null)
    return yaml


def _null(representer: object, value: None) -> object:
    return representer.represent_scalar("tag:yaml.org,2002:null", "null")  # type: ignore[attr-defined]


def _plain() -> YAML:
    return YAML(typ="safe", pure=True)


def canonical_yaml(text: str, path: str, *, sanitize: bool = False) -> str:
    """Return `text` in canonical form; formatting the result again returns it unchanged.

    Args:
        path: How diagnostics name the file.
        sanitize: Also make the two value-changing repairs the module docstring names.

    Raises:
        ProjectError: the text is not YAML (SST-LOD001), or its canonical form would load to a
            different value, so it is left as it is (SST-INT003).

    Diagnostics:
        SST-LOD001: the text does not parse; raised.
        SST-INT003: the canonical form would change the file's value; raised.
    """
    source = _without_trailing_space(text.replace("\r\n", "\n").replace("\r", "\n"))
    if not source.strip():
        return ""
    yaml = _layout()
    yaml.explicit_start = source.lstrip().startswith("---")
    try:
        documents = list(yaml.load_all(source))
        expected = list(_plain().load_all(source))
    except YAMLError as exc:
        raise _unparseable(path, exc) from exc
    for document in documents:
        _canonicalise(document, sanitize=sanitize, parent_key=None)
    if sanitize:
        expected = [_sanitised_value(document) for document in documents]
    stream = io.StringIO()
    if len(documents) == 1:
        yaml.dump(documents[0], stream)
    else:
        yaml.dump_all(documents, stream)
    result = _without_trailing_space(stream.getvalue()).rstrip("\n") + "\n"
    if list(_plain().load_all(result)) != expected:
        found = f"a different value for {path}, which was left as it is"
        diagnostic = D("SST-INT003", subject="format", value="sst format", found=found, expected="the same value")
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return result


def _without_trailing_space(text: str) -> str:
    """Strip trailing whitespace from each line, unless that changes what the text loads to.

    Trailing space is content only inside a block scalar, so stripping it elsewhere is a no-op
    on the value; where the value differs, the text is returned as it was.
    """
    stripped = "\n".join(line.rstrip() for line in text.split("\n"))
    if stripped == text:
        return text
    try:
        same = list(_plain().load_all(stripped)) == list(_plain().load_all(text))
    except YAMLError:
        same = False
    return stripped if same else text


def _unparseable(path: str, exc: YAMLError) -> ProjectError:
    mark = exc.problem_mark if isinstance(exc, MarkedYAMLError) else None
    line, col = (mark.line + 1, mark.column + 1) if mark is not None else (1, 1)
    problem = exc.problem if isinstance(exc, MarkedYAMLError) and exc.problem else str(exc)
    diagnostic = D("SST-LOD001", origin=Origin(path, line, col), file=path, line=line, col=col, detail=problem)
    return ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _canonicalise(node: object, *, sanitize: bool, parent_key: str | None) -> None:
    """Rewrite one node and everything under it into canonical form, in place."""
    if isinstance(node, CommentedMap):
        for key in list(node):
            node[key] = _canonical_value(node[key], sanitize=sanitize, key=str(key))
            _canonicalise(node[key], sanitize=sanitize, parent_key=str(key))
    elif isinstance(node, CommentedSeq):
        for index, item in enumerate(node):
            if _reorderable(item):
                item.move_to_end(_FIRST_KEY, last=False)
            node[index] = _canonical_item(item, sanitize=sanitize, parent_key=parent_key)
            _canonicalise(node[index], sanitize=sanitize, parent_key=None)


def _reorderable(item: object) -> bool:
    """Report whether `name` can move first: a comment between keys would follow the wrong one."""
    return isinstance(item, CommentedMap) and _FIRST_KEY in item and not item.ca.items and not item.ca.comment


def _canonical_value(value: object, *, sanitize: bool, key: str) -> object:
    if sanitize and key == "description" and isinstance(value, str):
        value = _restyled(value, _JINJA.sub(_unjinja, value))
    return _literal(value)


def _canonical_item(item: object, *, sanitize: bool, parent_key: str | None) -> object:
    if sanitize and parent_key in _APOSTROPHE_LISTS and isinstance(item, str):
        item = _restyled(item, _APOSTROPHES.sub("", item))
    return _literal(item)


def _unjinja(match: re.Match[str]) -> str:
    opening, closing = match.group(1), match.group(2)
    return f"{{ {opening}" if opening else f"{closing} }}"


def _restyled(original: str, repaired: str) -> str:
    """Return `repaired` in the scalar style `original` was written in, so only its text changes."""
    if repaired == original or not isinstance(original, ScalarString):
        return repaired
    return type(original)(repaired)


def _literal(value: object) -> object:
    """Return a multi-line string as a literal block scalar; any other value as it is.

    A string with whitespace at the end of a line, or a tab in a line's indentation, is
    double-quoted instead, unless it is one quoted line: in a block or a plain scalar that
    whitespace would be content at the end of a line, so the file could never be canonical.
    Otherwise a block scalar keeps the style it was written in, folded or literal.
    """
    if not isinstance(value, str):
        return value
    lines = value.split("\n")
    awkward = any(line != line.rstrip() or "\t" in line[: len(line) - len(line.lstrip())] for line in lines)
    quoted = isinstance(value, SingleQuotedScalarString | DoubleQuotedScalarString)
    if awkward and ("\n" in value or not quoted):
        return DoubleQuotedScalarString(value)
    if "\n" not in value.rstrip("\n") or isinstance(value, LiteralScalarString | FoldedScalarString):
        return value
    return LiteralScalarString(str(value))


def _sanitised_value(node: object) -> object:
    """Load the plain value of a node the round-trip loader built, for the semantic check."""
    stream = io.StringIO()
    _layout().dump(node, stream)
    return _plain().load(stream.getvalue())
