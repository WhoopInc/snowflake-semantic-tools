"""SST's canonical YAML form: what `sst format` writes and `sst format --check` checks a file against.

The canonical form is a re-layout that never changes what the file means:

- line feeds only, no trailing whitespace, and exactly one newline at the end of a non-empty file;
- dbt's own indentation: mappings by two spaces, a list item's dash two spaces in -- or, in a file
  that already writes its dashes at the parent key's column, as `dbt` scaffolds them, there;
- `name` first in every mapping that is a list item, or the value of a singular root such as
  `metric:`, and carries no comment, the rest in the order written: a single-item file is laid
  out as its list item would be;
- every multi-line string that is not already a block scalar as a literal block scalar, `|-` (or
  `|` when it ends in a newline); a block scalar keeps the style it was written in, folded or
  literal, so the formatter never rewrites how an author chose to write a value; a string
  holding a control character such as a carriage return stays double-quoted;
- a multi-line prose value (`PROSE_KEYS`) whose lines end in whitespace, which no block scalar
  can hold, has that whitespace and its final newline dropped and is written as `|-`, rather
  than as one escaped double-quoted line: a whitespace-only repair, which the value check
  allows for, and which Snowflake and dbt docs never show;
- with `wrap_width` (`format.wrap_width`), a single-line string that would run past it is a
  folded block, `>-`, broken at single spaces so it loads back as the same one line; and with
  `unwrap_prose` (`format.unwrap_prose`), a prose value's soft wraps are joined first, the third
  value-changing repair -- both in `adapters.yaml.styles`, which never touches SQL, expressions,
  or instructions;
- with `spaced` (`format.blank_line_between_items`), a blank line before every item but the
  first of a list of named items -- each a mapping holding `name` or `question`, as models,
  columns, metrics and eval questions are -- so a long list reads one item at a time. A list of
  anything else, such as a column's `data_tests` or a question's `ground_truth_invocations`, is
  left as written, and a blank line already there is kept whether or not `spaced` is set.

A file that is not YAML only because a `{{ ... }}` template is written unquoted, which
`sst validate` reads, has each template swapped for a placeholder before parsing (the
validate loader's own masking, `adapters.yaml.parse`) and restored as written after; a result
whose templates would no longer read back as the same scalars is refused (SST-INT003)
rather than written.

Comments, quoting, and the order of everything else survive the round trip. `canonical_yaml`
checks the result against the original before returning it: both must load to the same value,
and formatting the result again must return it unchanged, or the file is refused rather than
rewritten. With `sanitize`, two repairs that do change values are made first -- apostrophes
leave `synonyms` entries, and Jinja delimiters in a `description` are broken apart -- and the
check compares against the repaired value. A sample value keeps its apostrophes: it must match
the warehouse's text, and the DDL escapes the quote.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, replace

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
from ruamel.yaml.util import load_yaml_guess_indent

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.roundtrip import check_depth
from snowflake_semantic_tools.adapters.yaml.documents import SINGULAR_ROOTS
from snowflake_semantic_tools.adapters.yaml.parse import _neutralize_templates
from snowflake_semantic_tools.adapters.yaml.styles import PROSE_KEYS, YamlStyle, styled
from snowflake_semantic_tools.domain.diagnostics import D, Origin

# dbt's layout, which enrich also writes new files in.
_INDENT = (2, 4, 2)
# Wide enough that ruamel never folds a line itself; `styles` decides every fold.
_WIDTH = 1_000_000
_FIRST_KEY = "name"
_APOSTROPHE_LISTS = frozenset(("synonyms",))
_APOSTROPHES = re.compile("['\u2019]")
_JINJA = re.compile(r"\{([{%#])|([}%#])\}")
# A control character a block or plain scalar cannot hold: everything below space but tab and LF.
_CONTROL = re.compile("[\x00-\x08\x0b-\x1f\x7f]")
# The keys that make a list item a named item, which `spaced` separates with a blank line.
_ITEM_KEYS = ("name", "question")


@dataclass(frozen=True, slots=True)
class _Options:
    """What one formatting pass does beyond the layout: the repairs and the string styles."""

    sanitize: bool
    spaced: bool
    wrap_width: int | None
    unwrap_prose: bool
    indent: tuple[int, int, int] = _INDENT


def _layout(indent: tuple[int, int, int] = _INDENT) -> YAML:
    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    yaml.width = _WIDTH
    mapping, sequence, offset = indent
    yaml.indent(mapping=mapping, sequence=sequence, offset=offset)
    # An explicit `null` stays written; the round-trip dumper would leave the value empty.
    yaml.representer.add_representer(type(None), _null)
    return yaml


def _null(representer: object, value: None) -> object:
    return representer.represent_scalar("tag:yaml.org,2002:null", "null")  # type: ignore[attr-defined]


def _plain() -> YAML:
    return YAML(typ="safe", pure=True)


def canonical_yaml(
    text: str,
    path: str,
    *,
    sanitize: bool = False,
    spaced: bool = False,
    wrap_width: int | None = None,
    unwrap_prose: bool = False,
) -> str:
    """Return `text` in canonical form; formatting the result again returns it unchanged.

    Args:
        path: How diagnostics name the file.
        sanitize: Also make the two value-changing repairs the module docstring names.
        spaced: Also put a blank line between named list items, as `spaced_items` does.
        wrap_width: Fold a single-line string longer than this into `>-` (`styles.styled`).
        unwrap_prose: Also join the soft wraps of prose values, the third value-changing repair.

    Raises:
        ProjectError: the text is not YAML (SST-LOD001), or its canonical form would load to a
            different value or is not a fixed point, so it is left as it is (SST-INT003).

    Diagnostics:
        SST-LOD001: the text does not parse, or nests collections deeper than
            `adapters.roundtrip.check_depth` allows; raised.
        SST-INT003: the canonical form would change the file's value, or formatting it again,
            with the same `sanitize`, would change it; raised.
    """
    options = _Options(sanitize, spaced, wrap_width, unwrap_prose)
    try:
        return _laid_out(text, path, options)
    except ProjectError as exc:
        if "{{" not in text or [item.code for item in exc.diagnostics] != ["SST-LOD001"]:
            raise
        unparsed = exc
    masked, templates = _masked(text, path)
    if not templates:
        raise unparsed
    result = _restored_templates(_laid_out(masked, path, options), templates)
    again, kept = _masked(result, path)
    if kept != templates or _laid_out(again, path, options) != again:
        found = f"a form of {path} whose templates would not read back as written, so it was left as it is"
        diagnostic = D("SST-INT003", subject="format", value="sst format", found=found, expected="the same value")
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return result


def _masked(text: str, path: str) -> tuple[str, dict[str, str]]:
    """Return `text` with each template validate reads in a scalar swapped for a placeholder.

    A template in a comment or a block scalar is text to YAML, so it is left in place.

    Raises:
        ProjectError: a template is unterminated or nested (SST-LOD004), as validate refuses it.
    """
    if "{{" not in text:
        return text, {}
    masked, sources = _neutralize_templates(text.replace("\r\n", "\n"), path)
    return masked, {placeholder: source.raw for placeholder, source in sources.items()}


def _restored_templates(text: str, templates: dict[str, str]) -> str:
    for placeholder, raw in templates.items():
        text = text.replace(placeholder, raw)
    return text


def _laid_out(text: str, path: str, options: _Options) -> str:
    own = _file_indent(text)
    if own is not None and own != _INDENT:
        # The file's own list indentation is kept, unless that layout is not a fixed point.
        try:
            return _fixed_point(text, path, replace(options, indent=own))
        except ProjectError:
            pass
    return _fixed_point(text, path, options)


def _fixed_point(text: str, path: str, options: _Options) -> str:
    result = _canonical(text, path, options)
    if result and _canonical(result, path, options) != result:
        found = f"a form of {path} that a second format would change, so it was left as it is"
        diagnostic = D("SST-INT003", subject="format", value="sst format", found=found, expected="a fixed point")
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return result


def _file_indent(text: str) -> tuple[int, int, int] | None:
    """Return the indent of a file whose list dashes sit at their parent key's column, as dbt writes them.

    That is the one layout kept besides SST's own (dashes two spaces in); any other indentation,
    or a file with no block list, is None and is laid out the canonical way.
    """
    try:
        _, sequence, offset = load_yaml_guess_indent(text.replace("\r\n", "\n"))
    except (YAMLError, ValueError, TypeError, RecursionError):
        # Text too deep to guess is refused by `_canonical`'s depth check, with its own code.
        return None
    if sequence is None:
        return None
    offset = offset or 0
    mapping = sequence - offset if sequence > offset else sequence
    return (2, 2, 0) if (mapping, sequence, offset) == (2, 2, 0) and re.search(r"(?m)^- ", text) else None


def _canonical(text: str, path: str, options: _Options) -> str:
    """Return one formatting pass over `text`, refusing one that changes its value."""
    source = text.replace("\r\n", "\n").replace("\r", "\n")
    try:
        check_depth(source)
    except YAMLError as exc:
        raise _unparseable(path, exc) from exc
    source = _without_trailing_space(source)
    if not source.strip():
        return ""
    yaml = _layout(options.indent)
    yaml.explicit_start = source.lstrip().startswith("---")
    try:
        documents = list(yaml.load_all(source))
        expected = [_trimmed_tree(document) for document in _plain().load_all(source)]
    except YAMLError as exc:
        raise _unparseable(path, exc) from exc
    for document in documents:
        _name_first_in_roots(document)
        _canonicalise(document, options, parent_key=None, depth=0)
    if options.sanitize or options.unwrap_prose:
        repaired = [_sanitized_value(document, options.indent) for document in documents]
        if options.unwrap_prose and not options.sanitize and _prose_blind(repaired) != _prose_blind(expected):
            found = f"a change to {path} beyond the whitespace of prose values, so it was left as it is"
            diagnostic = D("SST-INT003", subject="format", value="sst format", found=found, expected="the same value")
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        expected = repaired
    stream = io.StringIO()
    if len(documents) == 1:
        yaml.dump(documents[0], stream)
    else:
        yaml.dump_all(documents, stream)
    result = _without_trailing_space(stream.getvalue()).rstrip("\n") + "\n"
    if options.spaced:
        result = spaced_items(result)
    try:
        same = list(_plain().load_all(result)) == expected
    except YAMLError:
        same = False
    if not same:
        found = f"a different value for {path}, which was left as it is"
        diagnostic = D("SST-INT003", subject="format", value="sst format", found=found, expected="the same value")
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return result


def styled_yaml(text: str, path: str, style: YamlStyle) -> str:
    """Return `text` in canonical form with the project's `format:` style applied."""
    return canonical_yaml(text, path, spaced=style.spaced, wrap_width=style.wrap_width, unwrap_prose=style.unwrap_prose)


def keep_canonical(original: str | None, edited: str, path: str, style: YamlStyle) -> str:
    """Return an edited file's text, kept in canonical form when the file was already in it.

    A file `sst format` with `style` would leave as it is is formatted again after the edit, so
    only what the edit added changes and the file stays canonical; any other file, or one whose
    edited form cannot be formatted, is returned as edited, its other lines as the author left
    them. A new file (no `original`) is formatted.
    """
    if style == YamlStyle():
        return edited
    try:
        if original is not None and original.strip() and styled_yaml(original, path, style) != original:
            return edited
        return styled_yaml(edited, path, style)
    except ProjectError:
        return edited


def spaced_items(text: str) -> str:
    """Return `text` with a blank line before every named list item but the first.

    A named item is a mapping holding `name` or `question`, in a block list whose items all are.
    The blank line goes above the comments written over the item, and is not added where one
    already is. Only blank lines are added, so the value is unchanged; where it would not be --
    a `|+` block scalar keeps a trailing blank line as content -- `text` is returned as it was,
    as it is when it does not parse.
    """
    try:
        documents = list(_layout().load_all(text))
    except YAMLError:
        return text
    starts: set[int] = set()
    for document in documents:
        _named_item_lines(document, starts)
    if not starts:
        return text
    lines = text.split("\n")
    blank_before: set[int] = set()
    for start in starts:
        line = _above_comments(lines, start)
        if line > 0 and lines[line - 1].strip():
            blank_before.add(line)
    if not blank_before:
        return text
    spaced = "\n".join(
        piece for index, line in enumerate(lines) for piece in (("", line) if index in blank_before else (line,))
    )
    try:
        same = list(_plain().load_all(spaced)) == list(_plain().load_all(text))
    except YAMLError:
        same = False
    return spaced if same else text


def _named_item_lines(node: object, starts: set[int]) -> None:
    """Add the 0-based line of every named list item but the first under `node` to `starts`."""
    if isinstance(node, CommentedMap):
        for value in node.values():
            _named_item_lines(value, starts)
    elif isinstance(node, CommentedSeq):
        named = (
            len(node) > 1
            and not node.fa.flow_style()
            and all(isinstance(item, CommentedMap) and any(key in item for key in _ITEM_KEYS) for item in node)
        )
        for index, item in enumerate(node):
            if named and index > 0:
                starts.add(node.lc.item(index)[0])
            _named_item_lines(item, starts)


def _above_comments(lines: list[str], start: int) -> int:
    """Return the line a blank line goes before: above the comments at the item's dash indent."""
    dash = len(lines[start]) - len(lines[start].lstrip())
    line = start
    while line > 0:
        above = lines[line - 1]
        if not above.lstrip().startswith("#") or len(above) - len(above.lstrip()) != dash:
            break
        line -= 1
    return line


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


def _canonicalise(node: object, options: _Options, *, parent_key: str | None, depth: int) -> None:
    """Rewrite one node and everything under it into canonical form, in place.

    `depth` counts the block levels above `node`, for the indent of a value that has no
    position (one an import or enrich just built).
    """
    step = options.indent[0]
    # A flow collection (`[a, b]`, `{k: v}`) holds no block scalar, so nothing in one is restyled.
    flow = isinstance(node, CommentedMap | CommentedSeq) and bool(node.fa.flow_style())
    if isinstance(node, CommentedMap):
        for key in list(node):
            column = _column(node, key, fallback=depth * step)
            value = _canonical_value(node[key], sanitize=options.sanitize, key=str(key))
            inline = column + len(str(key)) + 2
            node[key] = value if flow else _styled(value, str(key), options, indent=column + step, inline=inline)
            _canonicalise(node[key], options, parent_key=str(key), depth=depth + 1)
    elif isinstance(node, CommentedSeq):
        for index, item in enumerate(node):
            if _reorderable(item):
                item.move_to_end(_FIRST_KEY, last=False)
            column = _column(node, index, fallback=depth * step)
            value = _canonical_item(item, sanitize=options.sanitize, parent_key=parent_key)
            node[index] = (
                value if flow else _styled(value, parent_key, options, indent=column + step, inline=column + 2)
            )
            _canonicalise(node[index], options, parent_key=None, depth=depth + 1)


def _column(node: CommentedMap | CommentedSeq, key: object, *, fallback: int) -> int:
    """Return the column a key (or list item) starts at in the source; `fallback` when unknown."""
    try:
        position = node.lc.key(key) if isinstance(node, CommentedMap) else node.lc.item(key)
    except (KeyError, IndexError, TypeError, AttributeError):
        return fallback
    return int(position[1]) if position else fallback


def _styled(value: object, key: str | None, options: _Options, *, indent: int, inline: int) -> object:
    """Apply the string style rules, then `_literal`'s quoting for what a block cannot hold."""
    if options.wrap_width is None and not options.unwrap_prose:
        return value
    if (
        not isinstance(value, str)
        or _CONTROL.search(value)
        or isinstance(value, DoubleQuotedScalarString)
        and "\n" in value
    ):
        return value
    return styled(value, key, width=options.wrap_width, unwrap=options.unwrap_prose, indent=indent, inline=inline)


def _prose_blind(value: object, key: str | None = None) -> object:
    """Return `value` with each prose string's whitespace runs collapsed, for the unwrap check."""
    if isinstance(value, dict):
        return {k: _prose_blind(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_prose_blind(v, key) for v in value]
    if isinstance(value, str) and key in PROSE_KEYS:
        return " ".join(value.split())
    return value


def _name_first_in_roots(document: object) -> None:
    """Move `name` first in each singular root's mapping, as `_canonicalise` does for a list item."""
    if not isinstance(document, CommentedMap):
        return
    for key in document:
        if str(key) in SINGULAR_ROOTS and _reorderable(document[key]):
            document[key].move_to_end(_FIRST_KEY, last=False)


def _reorderable(item: object) -> bool:
    """Report whether `name` can move first: a comment between keys would follow the wrong one."""
    return isinstance(item, CommentedMap) and _FIRST_KEY in item and not item.ca.items and not item.ca.comment


def _canonical_value(value: object, *, sanitize: bool, key: str) -> object:
    if sanitize and key == "description" and isinstance(value, str):
        value = _restyled(value, _JINJA.sub(_unjinja, value))
    trimmed = _trimmed_prose(value, key)
    return LiteralScalarString(trimmed) if trimmed is not value else _literal(value)


def _trimmed_prose(value: object, key: str | None) -> object:
    """Return a multi-line prose value without the whitespace that ends its lines, or `value`.

    Only a value a block scalar could otherwise hold is trimmed: one with no control character
    and no tab in a line's indentation. Its final newline goes too, so it is written as `|-`.
    `value` itself is returned when nothing applies.
    """
    if key not in PROSE_KEYS or not isinstance(value, str) or _CONTROL.search(value):
        return value
    lines = value.split("\n")
    if len(value.rstrip("\n").split("\n")) < 2 or all(line == line.rstrip() for line in lines):
        return value
    if any("\t" in line[: len(line) - len(line.lstrip())] for line in lines):
        return value
    return "\n".join(line.rstrip() for line in lines).rstrip("\n")


def _trimmed_tree(value: object, key: str | None = None) -> object:
    """Return a plain loaded value with each prose string trimmed as `_trimmed_prose` trims it."""
    if isinstance(value, dict):
        return {k: _trimmed_tree(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        # A list item is never trimmed, as `_canonical_item` leaves it.
        return [_trimmed_tree(v) for v in value]
    trimmed = _trimmed_prose(value, key)
    return str(trimmed) if trimmed is not value else value


def _canonical_item(item: object, *, sanitize: bool, parent_key: str | None) -> object:
    if sanitize and parent_key in _APOSTROPHE_LISTS and isinstance(item, str):
        item = _restyled(item, _APOSTROPHES.sub("", item))
    return _literal(item)


def _unjinja(match: re.Match[str]) -> str:
    opening, closing = match.group(1), match.group(2)
    return f"{{ {opening}" if opening else f"{closing} }}"


def _restyled(original: str, repaired: str) -> str:
    """Return `repaired` in the scalar style `original` was written in, so only its text changes.

    An unchanged value is `original` itself: a plain copy of it would lose its quotes, and then
    `_literal` could quote it another way, which a second pass would not.
    """
    if repaired == original:
        return original
    if not isinstance(original, ScalarString):
        return repaired
    return type(original)(repaired)


def _literal(value: object) -> object:
    """Return a multi-line string as a literal block scalar; any other value as it is.

    A string with whitespace at the end of a line, or a tab in a line's indentation, is
    double-quoted instead, unless it is one quoted line: in a block or a plain scalar that
    whitespace would be content at the end of a line, so the file could never be canonical.
    A string holding a control character other than a line feed or tab, such as a carriage
    return, is double-quoted too: a block scalar cannot hold one, and only escapes keep it.
    Otherwise a block scalar keeps the style it was written in, folded or literal.
    """
    if not isinstance(value, str):
        return value
    lines = value.split("\n")
    awkward = any(line != line.rstrip() or "\t" in line[: len(line) - len(line.lstrip())] for line in lines)
    if _CONTROL.search(value):
        return value if isinstance(value, DoubleQuotedScalarString) else DoubleQuotedScalarString(value)
    quoted = isinstance(value, SingleQuotedScalarString | DoubleQuotedScalarString)
    if awkward and ("\n" in value or not quoted):
        return DoubleQuotedScalarString(value)
    if "\n" not in value.rstrip("\n") or isinstance(value, LiteralScalarString | FoldedScalarString):
        return value
    return LiteralScalarString(str(value))


def _sanitized_value(node: object, indent: tuple[int, int, int] = _INDENT) -> object:
    """Load the plain value of a node the round-trip loader built, for the semantic check."""
    stream = io.StringIO()
    _layout(indent).dump(node, stream)
    return _plain().load(stream.getvalue())
