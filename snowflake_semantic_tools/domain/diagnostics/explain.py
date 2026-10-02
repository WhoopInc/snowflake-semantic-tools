"""Everything the registry knows about one code, for `sst explain`.

`explain` answers for three kinds of code, case-insensitively: a registered 1.0 code, from its
registry entry; an SST 0.3 code, from its alias, naming the 1.0 codes it now answers to; and a
retired 1.0 number, from its tombstone. Any other text is unknown, and `explain` returns None.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from snowflake_semantic_tools.domain.diagnostics.core import ERROR_REFERENCE_URL, ErrorSpec
from snowflake_semantic_tools.domain.diagnostics.specs.aliases import ALIASES, TOMBSTONES, Alias
from snowflake_semantic_tools.domain.diagnostics.specs.retired import RETIRED_CODES


@dataclass(frozen=True, slots=True)
class AliasRow:
    """One 0.3 code as an explanation lists it: its title, what became of it, and its 1.0 codes."""

    code: str
    title: str
    disposition: str
    targets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Explanation:
    """What `sst explain` reports about one code.

    Attributes:
        kind: `code` for a registered code, `alias` for a 0.3 code, `retired` for a burned number.
        severity, phase, message_template, suggestion_template: From the registry entry; None
            for an alias or a retired number, which has none.
        non_demotable: Whether no setting or baseline may lower the code.
        origin: The 0.3 codes that resolve to this code.
        aliases: For a registered or retired code, the 0.3 codes that resolve to it; for an
            alias, the alias itself, whose `targets` are the 1.0 codes it was split into.
        superseded_by: The code a retired number's condition moved to; None when it moved nowhere.
        retired_in: The release that retired the number; None for any other kind.
    """

    code: str
    kind: str
    title: str
    help_url: str
    severity: str | None = None
    phase: str | None = None
    message_template: str | None = None
    suggestion_template: str | None = None
    non_demotable: bool = False
    deprecated_in: str | None = None
    origin: tuple[str, ...] = ()
    aliases: tuple[AliasRow, ...] = ()
    superseded_by: str | None = None
    retired_in: str | None = None

    @property
    def retired(self) -> bool:
        """Report whether the code is a retired number, which is never raised again."""
        return self.kind == "retired"


def explain(text: str) -> Explanation | None:
    """Return what the registry knows about the code `text` names, in any case; None for no code.

    A registered code wins over an alias of the same spelling, and an alias over a tombstone.
    """
    code = text.strip().upper()
    spec = ERROR_REGISTRY.get(code)
    if spec is not None:
        return _registered(spec)
    alias = ALIASES.get(code)
    if alias is not None:
        return Explanation(
            code,
            "alias",
            alias.title,
            _help_url(alias.targets[0]) if alias.targets else _help_url(code),
            aliases=(_row(alias),),
        )
    tombstone = TOMBSTONES.get(code)
    if tombstone is None:
        return None
    return Explanation(
        code,
        "retired",
        tombstone.title,
        _help_url(code),
        origin=_origin(code),
        aliases=_aliases_of(code),
        superseded_by=tombstone.superseded_by,
        retired_in=RETIRED_CODES.get(code),
    )


def _registered(spec: ErrorSpec) -> Explanation:
    return Explanation(
        spec.code,
        "code",
        spec.title,
        spec.help_url,
        severity=spec.severity.name.lower(),
        phase=spec.phase,
        message_template=spec.template,
        suggestion_template=spec.suggestion,
        non_demotable=not spec.demotable,
        deprecated_in=spec.deprecated_in,
        superseded_by=spec.superseded_by,
        origin=_origin(spec.code),
        aliases=_aliases_of(spec.code),
    )


def _origin(code: str) -> tuple[str, ...]:
    return tuple(alias.code for alias in ALIASES.values() if code in alias.targets)


def _aliases_of(code: str) -> tuple[AliasRow, ...]:
    return tuple(_row(alias) for alias in ALIASES.values() if code in alias.targets)


def _row(alias: Alias) -> AliasRow:
    return AliasRow(alias.code, alias.title, alias.disposition, alias.targets)


def _help_url(code: str) -> str:
    return f"{ERROR_REFERENCE_URL}#{code.casefold()}"
