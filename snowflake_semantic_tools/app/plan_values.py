"""What preparing a plan decides and returns: its scope, candidates, observation and outcome.

`plan` holds `PreparePlan`, which builds these; callers that only read a plan's outcome
import them from here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.observation_age import duration
from snowflake_semantic_tools.app.partial import PartialSplit
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import ChangeSet
from snowflake_semantic_tools.domain.plan.recorded import RecordedObservation
from snowflake_semantic_tools.domain.plan.selectors import Selection, SelectionScope
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.state import Manifest, State

# How long an observation stays current. A plan that takes longer to observe and decide
# reports SST-PLN018, because what it observed first may already have changed.
OBSERVATION_TTL_MS = 15 * 60 * 1000


@dataclass(frozen=True, slots=True)
class PlanScope:
    """Which artifacts a plan covers and which it may prune, as the selectors resolved.

    `prune_types` and `prune_keys` serve twice: an artifact either names is covered, and a
    prune acts only on what they name. When both are None, every artifact is covered.

    Attributes:
        selected: The selectors as given, which the saved plan records.
        prune_types, prune_keys: The artifact types and keys selected, less what `--exclude`
            names; None when the selectors name none.
        excluded_types, excluded_keys: What `--exclude` leaves out; None leaves nothing out.
        impact: Whether `state:modified` narrows the plan to what changed since the previous
            manifest.
        overwrite: `--overwrite`: update each covered object changed out of band (SST-PLN014)
            instead of blocking it.
    """

    selected: tuple[str, ...]
    prune_types: frozenset[str] | None
    prune_keys: frozenset[str] | None
    excluded_types: frozenset[str] | None
    excluded_keys: frozenset[str] | None
    include_prune: bool
    impact: bool = False
    overwrite: bool = False

    def covers(self, item: CompiledArtifact) -> bool:
        """Report whether the plan covers an artifact: selected by type or key, and not excluded."""
        return self.selection.covers(item.artifact_type, item.artifact_key)

    @property
    def by_state(self) -> bool:
        """Report whether every selector is a `state:` one.

        Such a selection chooses nothing when nothing is in that state, which is no miss.
        """
        return bool(self.selected) and all(value.casefold().startswith("state:") for value in self.selected)

    @property
    def selection(self) -> SelectionScope:
        """Return what `--select` and `--exclude` choose, as every command applies it."""
        named = self.prune_types is not None or self.prune_keys is not None
        return SelectionScope(
            Selection(self.prune_types, self.prune_keys) if named else None,
            Selection(self.excluded_types, self.excluded_keys),
        )


@dataclass(frozen=True, slots=True)
class PlanCandidates:
    """What a plan may publish, decided offline, before Snowflake is reached.

    Attributes:
        full: Everything that compiled; the lifecycle handlers and the observed schemas read
            all of it, whatever the selection.
        source: What may publish: with `--partial`, the compile split's healthy part, else `full`.
        selected: `source` narrowed to the scope, still carrying `source`'s diagnostics.
        split: The compile split, when `--partial` split the compile result.
        manifest: The manifest `source` publishes, which is the one `sst compile` wrote.
        strict, connected: How validation runs, with the flags applied over `validation:`.
        notices: What deciding the scope reported, which the plan reports first.
        covers_all: False when `state:modified` narrowed the plan to what changed.
    """

    full: CompileResult
    source: CompileResult
    selected: CompileResult
    split: PartialSplit | None
    manifest: Manifest
    scope: PlanScope
    partial: bool
    strict: bool
    connected: bool
    notices: tuple[Diagnostic, ...] = ()
    covers_all: bool = True


@dataclass(frozen=True, slots=True)
class CachedObservation:
    """The recorded observation a plan was made from, and how old it was when the plan used it.

    Attributes:
        fetched_at: When the observation was taken.
        age_seconds: How long before the plan that was; None when the time does not parse.
    """

    fetched_at: str
    age_seconds: float | None

    @property
    def age(self) -> str:
        """The age as hours and minutes, such as `2h 05m`, or `unknown`."""
        return duration(self.age_seconds) if self.age_seconds is not None else "unknown"


@dataclass(frozen=True, slots=True)
class PlanReady:
    """A plan ready to save or apply, with what it was made from.

    Attributes:
        result: The selection as validated. With `--partial`, narrowed to what stayed healthy,
            and carrying validation's diagnostics followed by one SST-PLN032 per artifact
            left out; otherwise the selection with the compile's diagnostics.
        state: The authoritative state the plan was made from.
        changeset: The plan, with what reading state reported ahead of its own diagnostics.
        lifecycle_handlers: The composite artifact types' handlers, by type, which apply reuses.
        recorded: What the plan read of the live target, for a later plan to reuse; None when
            a read failed, or the plan was made from a record.
        cached: The record the plan was made from instead of reading the target; None when
            it read the live target.
        promoted: How many warnings strict mode made errors, validation's and the plan's own.
    """

    result: CompileResult
    manifest: Manifest
    state: State
    changeset: ChangeSet
    lifecycle_handlers: Mapping[str, CompositeLifecycleHandler]
    recorded: RecordedObservation | None = None
    cached: CachedObservation | None = None
    promoted: int = 0

    @property
    def restamps_state(self) -> bool:
        """Report whether applying the plan would change state though it executes nothing.

        A report-only prune executes nothing, but apply records the object it observed when
        state has no entry for it; until it has, applying the plan changes the state table.
        """
        return any(change.key not in self.state.applied for change in self.changeset.report_only)


@dataclass(frozen=True, slots=True)
class PlanRefused:
    """Why nothing was planned: errors a run reports, or a project that cannot be planned at all.

    Attributes:
        diagnostics: The errors that stopped the plan, compile's or validation's; with a
            `reason`, what reports it.
        reason: Set when the project cannot be planned at all -- the selectors matched nothing
            (SST-DIS010, one per selector), or the compile is stale (SST-MAN009) -- which the CLI
            reports as a configuration error rather than as findings.
        promoted: How many warnings strict mode made errors before the plan was refused.
    """

    diagnostics: DiagnosticBag = DiagnosticBag()
    reason: str | None = None
    promoted: int = 0
