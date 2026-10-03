"""The semantic members as authored, before any reference resolves, and the files they come from.

A document is one semantic-model file as its parse found it: a tree of nodes, where each node
starts, and its top-level keys. The checks in `domain.validate.semantic` read documents and
the member records below through `AuthoredDocument` and `AuthoredDocuments`, which the YAML
adapter's parsed files satisfy, so no check depends on how a file was read.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol, TypeAlias

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls

NodePath: TypeAlias = tuple[str | int, ...]


@dataclass(frozen=True, slots=True)
class SourcePosition:
    """Where a YAML node starts in its file, as a 1-based line and a 1-based column."""

    line: int
    col: int


class AuthoredDocument(Protocol):
    """One semantic-model file as parsed: its tree, where its nodes start, and how it was found."""

    @property
    def path(self) -> str:
        """Return the file relative to the project directory, in POSIX form, as diagnostics name it."""
        ...

    @property
    def tree(self) -> Mapping[str, Any]:
        """Return the root mapping, with string keys at the top level."""
        ...

    @property
    def line_index(self) -> Mapping[NodePath, SourcePosition]:
        """Return where the value at each node path starts."""
        ...

    @property
    def root_keys(self) -> tuple[str, ...]:
        """Return the tree's top-level keys, in file order."""
        ...

    @property
    def hint_root(self) -> str | None:
        """Return the type folder the file was found in; None outside every type's folder."""
        ...

    def position(self, path: NodePath) -> SourcePosition | None:
        """Return where the value at a node path starts; None when the file records no such path."""
        ...


class AuthoredDocuments(Protocol):
    """Every semantic-model file that parsed, in discovery order."""

    @property
    def documents(self) -> Sequence[AuthoredDocument]:
        """Return the files that parsed, in discovery order."""
        ...


@dataclass(frozen=True, slots=True)
class NonAdditiveDef:
    """One `non_additive_dimensions` entry as authored: a dimension, its table, and its sort."""

    dimension: str
    table: str | None = None
    descending: bool | None = None
    nulls_first: bool | None = None

    @property
    def names(self) -> tuple[str, ...]:
        """Return the names the entry's sort key is written from: its table, if any, then its dimension."""
        return (self.table, self.dimension) if self.table else (self.dimension,)

    @property
    def key(self) -> tuple[str, bool | None, bool | None]:
        """Return the entry's `NON ADDITIVE BY` sort key as text, with its sort as written.

        The expression is the dimension uppercased, as `TABLE.DIMENSION` when the entry names a
        table; the loader guards it before a model holds it.
        """
        return ".".join(name.upper() for name in self.names), self.descending, self.nulls_first


SORT_DIRECTIONS: Mapping[str, bool] = MappingProxyType({"ascending": False, "descending": True})
NULL_ORDERS: Mapping[str, bool] = MappingProxyType({"first": True, "last": False})


@dataclass(frozen=True, slots=True)
class WindowOrderDef:
    """One `window.order_by` entry: a `{{ ref() }}` or `{{ metric() }}` call and its sort."""

    ref: str
    descending: bool | None = None
    nulls_first: bool | None = None


@dataclass(frozen=True, slots=True)
class WindowDef:
    """A metric's `window:` block as authored, with its references unresolved."""

    partition_by: tuple[str, ...] = ()
    partition_excluding: tuple[str, ...] = ()
    order_by: tuple[WindowOrderDef, ...] = ()
    # The canonical frame clause; None when absent or not a frame (SST-PRS124).
    frame: str | None = None
    has_frame: bool = False

    def references(self) -> tuple[tuple[str, str], ...]:
        """Every entry, with the field label a diagnostic names it by."""
        return (
            *((f"partition_by[{index}]", text) for index, text in enumerate(self.partition_by)),
            *((f"partition_by_excluding[{index}]", text) for index, text in enumerate(self.partition_excluding)),
            *((f"order_by[{index}]", entry.ref) for index, entry in enumerate(self.order_by)),
        )


@dataclass(frozen=True, slots=True)
class MetricDef:
    """A metric as authored, with its `ref()`s still unresolved.

    Attributes:
        name, expr: As written; `expr` keeps its templates.
        description: On one line and stripped; None when absent or blank.
        synonyms: Each as text, a lone scalar as one synonym; `()` when absent.
        tables: The models `tables:` names, casefolded, in order; `()` when it is absent or
            cannot be read.
        derived: Whether the metric is derived, built from other metrics, rather than table-scoped.
        using_relationships: The relationship names, uppercased, each as written or as the one
            `relationship()` call it is written as.
        non_additive: The `non_additive_dimensions` entries that name a dimension.
        access_modifier: As written; `public_access` when absent.
        has_tables_key: Whether the entry has a `tables:` key at all, which `tables` cannot tell.
        origin: Where the entry starts; None only for a record not read from a file.
        template_calls: The calls in `expr`, as read; `()` when it has none or one is malformed.
        poisoned: Whether `tables:` cannot be read, which keeps the metric out of every view.
        window: The `window:` block; None when absent or not a mapping.
        relationship_refs: Those of `using_relationships` written as a `relationship()` call.
    """

    name: str
    expr: str
    description: str | None
    synonyms: tuple[str, ...]
    tables: tuple[str, ...] = ()
    derived: bool = False
    using_relationships: tuple[str, ...] = ()
    non_additive: tuple[NonAdditiveDef, ...] = ()
    access_modifier: str = "public_access"
    has_tables_key: bool = False
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
    window: WindowDef | None = None
    relationship_refs: tuple[str, ...] = ()

    @property
    def calls(self) -> tuple[TemplateCall, ...]:
        """List the template calls in `expr`, in source order; empty when a template is malformed.

        The calls read with the metric are used when there are any; otherwise `expr` is scanned
        again, so a record built without them still finds its calls.
        """
        if self.template_calls:
            return self.template_calls
        try:
            return scan_template_calls(self.expr)
        except TemplateSyntaxError:
            return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        """Which models this metric's expr refs, in first-seen order."""
        seen: list[str] = []
        for call in self.calls:
            if call.function == "ref" and call.args and call.args[0].casefold() not in seen:
                seen.append(call.args[0].casefold())
        return tuple(seen)

    @property
    def referenced_metrics(self) -> tuple[str, ...]:
        """Name the metrics `expr` calls `metric()` on, casefolded, each once in first-seen order.

        A `metric()` call with other than one argument names none.
        """
        return tuple(
            dict.fromkeys(
                call.args[0].casefold() for call in self.calls if call.function == "metric" and len(call.args) == 1
            )
        )


@dataclass(frozen=True, slots=True)
class FilterDef:
    """A filter as authored, with its `ref()`s still unresolved.

    Attributes:
        name, expr: As written; `expr` keeps its templates.
        description: On one line and stripped; None when absent or blank.
        tables: The models `tables:` names, casefolded, in order; `()` when it is absent or
            cannot be read.
        entity_level: Whether `labels:` holds `filter`, in any case: such a filter is built as a
            filter column of its table, and any other filter as prose in the view's instructions.
        origin, template_calls, poisoned: As `MetricDef` reads them.
        labeled: Whether the entry has a `labels:` key at all, even an empty one.
    """

    name: str
    expr: str
    description: str | None
    tables: tuple[str, ...]
    entity_level: bool
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
    labeled: bool = False


@dataclass(frozen=True, slots=True)
class InstructionDef:
    """A custom instruction as authored, which views attach by name.

    Attributes:
        name: As written.
        ai_sql_generation, ai_question_categorization: Stripped; None when absent or blank.
        origin: Where the entry starts; None only for a record not read from a file.
        poisoned: Never set by the reader: an instruction declares no tables to be malformed.
        renamed: Whether the entry spells a channel the 0.3 way, which the reader does not read.
    """

    name: str
    ai_sql_generation: str | None
    ai_question_categorization: str | None
    origin: Origin | None = None
    poisoned: bool = False
    renamed: bool = False


@dataclass(frozen=True, slots=True)
class VerifiedQueryDef:
    """A verified query as authored, with its SQL's templates still unresolved.

    Attributes:
        name, question: As written.
        sql: From `sql:` or the `sql_file:` it names, without its leading blank and `--` lines
            or trailing whitespace.
        tables: The models `tables:` names, casefolded, in order; `()` when it is absent or
            cannot be read.
        verified_at: In seconds since the epoch: an integer as written, or a `YYYY-MM-DD`
            date's midnight UTC; None when absent.
        verified_by: Stripped; None when absent or blank.
        onboarding_question: `use_as_onboarding_question` as a truth value; None when absent.
        origin, template_calls, poisoned: As `MetricDef` reads them, with `sql` for `expr`.
    """

    name: str
    question: str
    sql: str
    tables: tuple[str, ...]
    verified_at: int | None
    verified_by: str | None
    onboarding_question: bool | None
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
