"""The rules enrich derives metadata by: data types, column types, sample values, and synonyms.

Each rule is a pure function of what the warehouse and the model YAML say, so a run's output
depends on nothing else, and two runs over the same data write the same YAML.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.validate.column_metadata import (
    base_type,
    is_numeric,
    is_sentinel,
    printable,
    synonym_problem,
)

# The type INFORMATION_SCHEMA reports for each spelling a model YAML may hold. A type not
# listed is its own family, compared by its base type.
_TYPE_FAMILIES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "TEXT": ("TEXT", "VARCHAR", "STRING", "CHAR", "CHARACTER", "NCHAR", "NVARCHAR", "NVARCHAR2", "CHAR VARYING"),
        "NUMBER": ("NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"),
        "FLOAT": ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"),
        "BOOLEAN": ("BOOLEAN", "BOOL"),
        "TIMESTAMP_NTZ": ("TIMESTAMP_NTZ", "TIMESTAMPNTZ", "DATETIME", "TIMESTAMP"),
        "TIMESTAMP_LTZ": ("TIMESTAMP_LTZ", "TIMESTAMPLTZ"),
        "TIMESTAMP_TZ": ("TIMESTAMP_TZ", "TIMESTAMPTZ"),
        "BINARY": ("BINARY", "VARBINARY"),
    }
)
_FAMILY_OF: Mapping[str, str] = MappingProxyType(
    {spelling: family for family, spellings in _TYPE_FAMILIES.items() for spelling in spellings}
)

# The data types enrich samples: the scalar ones, whose values read as text in YAML.
SAMPLED_TYPES = frozenset(
    ("TEXT", "NUMBER", "FLOAT", "BOOLEAN", "DATE", "TIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ")
)

# Longer values are free text, not samples an analyst would filter on.
MAX_SAMPLE_LENGTH = 500
MAX_SYNONYM_LENGTH = 100

# Text a template engine would read: a value or synonym holding it is never written. dbt renders
# the YAML enrich writes as Jinja, so a value holding one would run as code at the next parse.
_TEMPLATE_MARKERS = ("{{", "{%", "{#")


def has_template_syntax(text: str) -> bool:
    """Report whether `text` opens a Jinja expression, statement or comment."""
    return any(marker in text for marker in _TEMPLATE_MARKERS)


def semantic_data_type(data_type: str) -> str:
    """Return the type enrich writes for a data type: its family, as INFORMATION_SCHEMA names it.

    `varchar(16)` is `TEXT` and `bigint` is `NUMBER`; a type with no family, such as `VARIANT`,
    is its base type upper-cased.
    """
    base = base_type(data_type)
    return _FAMILY_OF.get(base, base)


def same_data_type(left: str, right: str) -> bool:
    """Report whether two data types name the same family, whatever their spelling or parameters."""
    return semantic_data_type(left) == semantic_data_type(right)


def derive_column_type(data_type: str, *, key: bool) -> str:
    """Return the column type a column derives: `fact` when numeric and not a key, else `dimension`.

    A key -- declared on the model or named by a key or relationship test -- is a dimension
    whatever its type, so a numeric identifier is not summed as a measure.
    """
    return "fact" if is_numeric(data_type) and not key else "dimension"


def is_sampled_type(data_type: str) -> bool:
    """Report whether enrich samples a column of this data type."""
    return semantic_data_type(data_type) in SAMPLED_TYPES


def usable_sample(value: str) -> bool:
    """Report whether a sampled value may be written: short, single-line, and not a placeholder.

    A value is refused when it is blank, a missing-value sentinel such as `nan`, longer than
    `MAX_SAMPLE_LENGTH`, holds a control character or a line break, or holds template syntax.
    """
    if not value.strip() or is_sentinel(value.strip()) or len(value) > MAX_SAMPLE_LENGTH:
        return False
    if has_template_syntax(value):
        return False
    return all(character == "\t" or character.isprintable() for character in value)


@dataclass(frozen=True, slots=True)
class SampleDecision:
    """What one column's sampled values say: the values to write, and whether they are all of them.

    Attributes:
        values: The usable values, most frequent first: all of them when `complete`, else at
            most the display limit.
        complete: The column holds no other non-null value, and every value it holds is usable.
    """

    values: tuple[str, ...]
    complete: bool


def decide_samples(fetched: Sequence[str], *, distinct_limit: int, display_limit: int) -> SampleDecision:
    """Decide what to write from a column's distinct values, fetched most frequent first.

    The query fetches at most `distinct_limit + 1` values, so more than `distinct_limit` means
    the column has more. A refused value makes the set incomplete: writing the rest as an enum
    would claim a value the column holds does not exist.
    """
    distinct = tuple(dict.fromkeys(fetched))
    usable = tuple(value for value in distinct if usable_sample(value))
    complete = len(distinct) <= distinct_limit and len(usable) == len(distinct)
    return SampleDecision(usable if complete else usable[:display_limit], complete)


def _folded_name(name: str) -> tuple[str, str]:
    folded = name.casefold()
    return folded, folded.replace("_", " ")


def proposed_synonym(candidate: object) -> tuple[str, str | None]:
    """Read one proposed synonym: its text, and why it can never be written.

    The text is stripped and its runs of spaces collapsed. It is refused, whatever else the
    column or table holds, when it holds a quote or a control character (a newline or tab
    inside it among them: those are checked before spaces are collapsed), is longer than
    `MAX_SYNONYM_LENGTH`, or holds template syntax.

    Returns:
        `(text, reason)`, the reason worded as SST-PRS030 words it; `("", None)` for a proposal
        that is not text, and a reason of None for one that may be written.
    """
    if not isinstance(candidate, str):
        return "", None
    stripped = candidate.strip()
    text = " ".join(stripped.split())
    problem = synonym_problem(stripped)
    if problem is None and len(text) > MAX_SYNONYM_LENGTH:
        problem = f"more than {MAX_SYNONYM_LENGTH} characters"
    if problem is None and has_template_syntax(text):
        problem = "template syntax"
    return text, problem


def clean_synonyms(candidates: Iterable[object], *, name: str, taken: Collection[str], limit: int) -> tuple[str, ...]:
    """Return the usable synonyms of a column or table, in the order the model proposed them.

    A synonym is dropped when it is not text, is empty, `proposed_synonym` refuses it, repeats
    `name` (with or without underscores), repeats an earlier synonym, or is in `taken`: the
    casefolded names and synonyms of the other columns or tables it must stay apart from. At
    most `limit` are kept.
    """
    seen = {*taken, *_folded_name(name)}
    kept: list[str] = []
    for candidate in candidates:
        if len(kept) == limit:
            break
        text, problem = proposed_synonym(candidate)
        if not text or problem is not None or text.casefold() in seen:
            continue
        seen.add(text.casefold())
        kept.append(text)
    return tuple(kept)


def rejected_synonyms(
    candidates: Iterable[object], *, artifact: str, subject: str, limit: int
) -> tuple[Diagnostic, ...]:
    """Report the proposed synonyms `proposed_synonym` refuses, so none is dropped unseen.

    A proposal that merely repeats a name or another synonym is not reported: dropping it is
    the cleaning working. At most `limit` are reported, so a runaway answer cannot flood the
    report; the synonym is shown as proposed, stripped, escaped and cut short.

    Diagnostics:
        SST-PRS030: a proposed synonym holds a quote, a control character or template syntax,
            or is too long.
    """
    found: list[Diagnostic] = []
    for candidate in candidates:
        if len(found) == limit:
            break
        text, problem = proposed_synonym(candidate)
        if text and problem is not None:
            # Shown as proposed, not collapsed, so the reviewer sees the newline that refused it.
            shown = printable(str(candidate).strip())
            found.append(D("SST-PRS030", artifact=artifact, value=shown, detail=problem, subject=subject))
    return tuple(found)


def templated_text(update_name: str, values: Iterable[tuple[str, object]]) -> tuple[str, ...]:
    """Return each text enrich would write for one column that holds template syntax.

    The column's name and every text value, a list value's items among them, are checked:
    whatever rule produced them, none reaches a file that dbt renders as Jinja.
    """
    texts = [update_name]
    for _, value in values:
        texts.extend(item for item in (value if isinstance(value, tuple) else (value,)) if isinstance(item, str))
    return tuple(text for text in texts if has_template_syntax(text))


def taken_names(names: Iterable[str], synonyms: Iterable[str]) -> frozenset[str]:
    """Return names and synonyms as a synonym must avoid them: casefolded, underscores read as spaces."""
    folded = {form for name in names for form in _folded_name(name)}
    return frozenset(folded | {synonym.casefold() for synonym in synonyms})
