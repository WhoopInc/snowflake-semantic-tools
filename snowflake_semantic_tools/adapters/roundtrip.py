"""Edit a YAML file in place: load it into editable nodes, change some, and write it back.

Comments, key order, quoting, flow and block styles, and the file's own indentation survive,
so a file comes back unchanged wherever it was not edited. A file whose layout ruamel cannot
reproduce exactly -- unusual indentation, say -- comes back reformatted, which `reformats`
reports before anything is written.
"""

from __future__ import annotations

import io
from dataclasses import dataclass

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.error import YAMLError
from ruamel.yaml.scalarstring import SingleQuotedScalarString
from ruamel.yaml.util import load_yaml_guess_indent

from snowflake_semantic_tools.adapters.errors import ProjectError

__all__ = [
    "CommentedMap",
    "CommentedSeq",
    "EditableYaml",
    "WrittenFile",
    "block_list",
    "insert_key",
    "load_editable",
    "new_editable",
    "scalar",
]

# Wide enough that no line is ever folded where the file did not fold it.
_WIDTH = 4096

# The layout of a file enrich creates: dbt's own, a list item indented under its key.
_NEW_FILE_INDENT = (2, 4, 2)

# dbt reads YAML 1.1, where a plain `no` is false and `1:20` is 80; a value is checked
# against a 1.1 loader before it is written plain.
_YAML_1_1 = YAML(typ="safe", pure=True)
_YAML_1_1.version = (1, 1)


def _yaml(indent: tuple[int, int, int], *, explicit_start: bool) -> YAML:
    mapping, sequence, offset = indent
    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    yaml.width = _WIDTH
    yaml.indent(mapping=mapping, sequence=sequence, offset=offset)
    yaml.explicit_start = explicit_start
    return yaml


@dataclass(frozen=True, slots=True)
class WrittenFile:
    """A file's text after enrich's edits.

    Attributes:
        text: The whole file as it is written.
        reformatted: Writing it changes lines enrich did not edit (SST-PRS125).
    """

    text: str
    reformatted: bool


@dataclass
class EditableYaml:
    """One YAML document loaded for editing, with the layout it is written back in.

    Attributes:
        root: The document's top node; edits change it in place.
        original: The text the document was loaded from; empty for a new document.
    """

    root: object
    original: str
    _yaml: YAML

    def dump(self) -> str:
        """Return the document as text, in the layout it was loaded with, ending as the file did.

        The dumper always ends a document with one newline; a file that ended without one, or
        with blank lines, keeps its own ending.
        """
        stream = io.StringIO()
        self._yaml.dump(self.root, stream)
        text = stream.getvalue()
        if not self.original:
            return text
        ending = self.original[len(self.original.rstrip("\n")) :]
        return text.rstrip("\n") + ending

    def reformats(self) -> bool:
        """Report whether writing the document back unedited would change its text."""
        return bool(self.original) and _reloaded(self.original) != self.original


def _guess_indent(text: str, path: str) -> tuple[int, int, int]:
    """Return the mapping indent, sequence indent and dash offset the text is written with."""
    try:
        _, sequence, offset = load_yaml_guess_indent(text)
    except YAMLError as exc:
        raise ProjectError(f"{path}: cannot parse YAML to edit it: {exc}") from exc
    sequence = sequence or 2
    offset = offset or 0
    mapping = sequence - offset if sequence > offset else sequence
    return mapping, sequence, offset


def load_editable(text: str, path: str) -> EditableYaml:
    """Load one YAML document for editing, keeping its layout.

    Raises:
        ProjectError: The text is not YAML.
    """
    yaml = _yaml(_guess_indent(text, path), explicit_start=text.lstrip().startswith("---"))
    try:
        root = yaml.load(text)
    except YAMLError as exc:
        raise ProjectError(f"{path}: cannot parse YAML to edit it: {exc}") from exc
    return EditableYaml(root, text, yaml)


def new_editable(root: CommentedMap) -> EditableYaml:
    """Start a document enrich creates, laid out as dbt's own files are."""
    return EditableYaml(root, "", _yaml(_NEW_FILE_INDENT, explicit_start=False))


def _reloaded(text: str) -> str:
    return load_editable(text, "<round trip>").dump()


def scalar(text: str) -> str:
    """Return text as a scalar every YAML loader reads back as that text, quoting it when needed.

    Text that a YAML 1.1 loader -- dbt's -- would read as something else, such as `no`, `1:20` or
    `012`, is written single-quoted; anything else is left for the dumper to style.
    """
    try:
        plain = _YAML_1_1.load(text) == text
    except YAMLError:
        plain = False
    return text if plain else SingleQuotedScalarString(text)


def block_list(values: tuple[str, ...]) -> CommentedSeq:
    """Return text values as a list written one item per line, each quoted as `scalar` decides."""
    sequence = CommentedSeq(scalar(value) for value in values)
    sequence.fa.set_block_style()
    return sequence


def insert_key(mapping: CommentedMap, key: str, value: object, order: tuple[str, ...]) -> None:
    """Set a key: in place when it exists, else before the first later key of `order`.

    A key `order` does not name, or with no later key present, is added at the end.
    """
    if key in mapping:
        mapping[key] = value
        return
    later = order[order.index(key) + 1 :] if key in order else ()
    for position, existing in enumerate(mapping):
        if existing in later:
            mapping.insert(position, key, value)
            return
    mapping[key] = value
