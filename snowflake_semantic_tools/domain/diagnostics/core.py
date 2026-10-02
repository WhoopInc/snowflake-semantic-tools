"""The parts every diagnostic code is made of: severity, source origin, and registry entry.

`spec` builds one registry entry; each module under `specs/` calls it once per code. This
module imports only the standard library, so the spec modules can load it while the
package `__init__`, which builds the registry from them, is still being imported.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum
from string import Formatter
from types import MappingProxyType


class Severity(IntEnum):
    """How much a diagnostic blocks: an error blocks the command, info never does.

    A warning does not block unless strict mode promotes it to an error.
    """

    INFO = 10
    WARNING = 20
    ERROR = 30


@dataclass(frozen=True, slots=True)
class Origin:
    """Where in the project a diagnostic points.

    Attributes:
        line: 1-based; None when the problem concerns the whole file.
        col: 1-based; None when only the line is known.
        dbt_node: The dbt node's unique id when the location is dbt model metadata; None otherwise.
    """

    file: str
    line: int | None = None
    col: int | None = None
    dbt_node: str | None = None


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    """The registry entry for one code: its severity, message template, and fix.

    Build one with `spec`, which derives `subsystem`, `phase`, and `help_url` from the code.

    Attributes:
        title: The short description the error reference shows under the code's heading.
        template: A `str.format` template; `D` reports SST-INT901 when a placeholder is not supplied.
        suggestion: How to fix the problem; None when there is nothing to suggest.
        subsystem: The three letters after ``SST-``, which place the code in the error reference.
        phase: The subsystem in lowercase, reported as each diagnostic's phase.
        demotable: False for an error that no setting may downgrade ("always an error").
        internal_detail: Whether its diagnostics carry raw text from SST or Snowflake beyond the
            template; only INT and SNO codes may.
        deprecated_in: The release that deprecated the code; None while it is current.
        superseded_by: The code that replaces a deprecated one; required with `deprecated_in`.
    """

    code: str
    severity: Severity
    title: str
    template: str
    suggestion: str | None
    subsystem: str
    phase: str
    help_url: str
    demotable: bool = True
    internal_detail: bool = False
    deprecated_in: str | None = None
    superseded_by: str | None = None

    @property
    def always_error(self) -> bool:
        """Report whether the code is an error that no setting may downgrade."""
        return self.severity is Severity.ERROR and not self.demotable


class RegistryIntegrityError(RuntimeError):
    """A registry SST is built from is itself invalid: the diagnostic codes, or the artifact types.

    Raised while the package is imported or a registry is built, never for a user's project.
    There is no diagnostic channel yet when it is raised, so it carries its REG code and the
    code's formatted message itself; `str()` is ``<code>: <message>``.

    Attributes:
        code: The SST-REG code naming what is wrong.
        context: The values the code's message template was formatted with, read-only.
    """

    def __init__(self, code: str, message: str, context: Mapping[str, object]) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.context: Mapping[str, object] = MappingProxyType(dict(context))


# Every code has a heading in the generated error reference (`sst docs`); the
# fragment is the code in lowercase, which is the anchor GitHub derives from it.
ERROR_REFERENCE_URL = "https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/reference/error-codes.md"


def spec(
    code: str,
    severity: Severity,
    title: str,
    template: str,
    suggestion: str | None,
    *,
    demotable: bool = True,
    internal_detail: bool = False,
    deprecated_in: str | None = None,
    superseded_by: str | None = None,
) -> ErrorSpec:
    """Build the registry entry for one code, deriving its subsystem, phase, and help URL.

    Args:
        demotable: False for an error that no setting may downgrade.
        internal_detail, deprecated_in, superseded_by: As `ErrorSpec` documents them.
    """
    return ErrorSpec(
        code=code,
        severity=severity,
        title=title,
        template=template,
        suggestion=suggestion,
        subsystem=code.split("-")[1][:3],
        phase=code.split("-")[1][:3].casefold(),
        help_url=f"{ERROR_REFERENCE_URL}#{code.casefold()}",
        demotable=demotable,
        internal_detail=internal_detail,
        deprecated_in=deprecated_in,
        superseded_by=superseded_by,
    )


def _placeholders(template: str) -> frozenset[str]:
    return frozenset(field for _, field, _, _ in Formatter().parse(template) if field)
