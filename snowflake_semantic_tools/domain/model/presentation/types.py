"""The types of the presentation tables: how an editor names, groups and lays out each key.

The authored-file tables say what a key means; these say how a person sees it. An editor reads
them from the JSON Schemas, as `x-studio`, so it never derives a label from a key's spelling or
guesses which field titles a list's items. Every key of every file kind, and of `sst_config.yml`,
has one; `tests/unit/domain/test_presentation.py` refuses a key without one and an entry for no key.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Widget(Enum):
    """The control an editor shows a value in, where its kind alone does not decide it.

    `PROSE` is free text a person reads, `SQL` and `MARKDOWN` are code shown as such, `REFERENCE`
    names another object through a template call, and `CODE` is any other verbatim text, such as a
    JSON Schema or a stage path.
    """

    TEXT = "text"
    PROSE = "prose"
    SQL = "sql"
    MARKDOWN = "markdown"
    REFERENCE = "reference"
    CODE = "code"


class ListLayout(Enum):
    """How an editor lays out a list of mappings: a table of short entries, cards, or chips."""

    TABLE = "table"
    CARDS = "cards"
    CHIPS = "chips"


@dataclass(frozen=True, slots=True)
class Group:
    """One section of a mapping's keys, in the order an editor shows them.

    Attributes:
        id: What a key's `Ui.group` names it by.
        label: The section's heading.
        blurb: One line on what the keys in it decide.
    """

    id: str
    label: str
    blurb: str


@dataclass(frozen=True, slots=True)
class Ui:
    """How one key is shown.

    Attributes:
        label: What a person calls the key, in sentence case, never the key's own spelling.
        group: The `Group.id` of the section of its mapping it sits in; None in a mapping with no
            sections.
        advanced: Shown only once asked for, behind its section's further settings.
        widget: The control its value is shown in, when its kind does not decide it.
    """

    label: str
    group: str | None = None
    advanced: bool = False
    widget: Widget | None = None


@dataclass(frozen=True, slots=True)
class ListUi:
    """How a list of mappings is shown, and how one of its items is named.

    Attributes:
        noun: One item, in lower case: what an "Add" button adds.
        title: The key whose value titles an item.
        layout: How the items are laid out.
        subtitle: The key whose value is an item's second line; None for none.
        columns: For a `TABLE`, the keys shown as its columns, after the title.
    """

    noun: str
    title: str
    layout: ListLayout
    subtitle: str | None = None
    columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class KindUi:
    """How a file kind is named, and which of its keys describe a declaration.

    Attributes:
        label: One declaration of the kind, in sentence case.
        plural: More than one.
        groups: The sections of the kind's own keys, in order.
        description: The key whose value says what a declaration is; None when it has none.
        owner: The key path, dotted, whose value names who owns a declaration; None when it has none.
        key_settings: The keys a summary of a declaration shows, in order.
        flatten: Mappings that only hold others, such as an agent's `spec`, whose keys are shown
            as the kind's own, in its sections, with no heading of their own.
    """

    label: str
    plural: str
    groups: tuple[Group, ...]
    description: str | None = None
    owner: str | None = None
    key_settings: tuple[str, ...] = ()
    flatten: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Section:
    """One section of `sst_config.yml`, as an editor's tabs show it.

    Attributes:
        id: What a top-level key's section names it by.
        label: The tab's name.
        blurb: One line on what the section decides.
    """

    id: str
    label: str
    blurb: str
