"""Whether a bare `sst apply` can apply the plan `sst plan` just saved instead of planning again.

`sst plan` writes `sst_target/plan.json`. A bare `sst apply`, with no `--plan`, used to plan the
whole project again, reading every published object, even when that plan was minutes old.
Now it reuses the saved plan when all of these hold:

- apply was given no flag that shapes the plan (`--select`, `--exclude`, `--prune`,
  `--partial`, `--overwrite`, `--temporary`, `--state`, `--replan`), and the saved plan was made
  with none;
- the saved plan is for the target apply resolves, and from the manifest `sst compile` wrote,
  which the project still compiles to;
- it observed Snowflake no more than `apply.reuse_plan_minutes` ago (30 by default; 0 never
  reuses).

Reusing it, apply plans again only the changes the saved plan writes, and applies them only
when each is the change it saved: same action, object, observed marker, fingerprint and
statements. Otherwise it plans the whole project, as it always did, and says why. A change the
saved plan left alone is not read again: an object changed out of band in that window is found
by the next plan.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.cli.settings import project_config
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.cli.wiring.plan import PlanRequest
from snowflake_semantic_tools.cli.wiring.project import project_inputs, target_dir
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_KEYS, config_block, config_int
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeSet
from snowflake_semantic_tools.domain.state import SavedChange, SavedPlan

REUSE_MINUTES = int(CONFIG_KEYS["apply.reuse_plan_minutes"].default or 0)
_PASSING = frozenset((Action.NOOP.value, Action.BLOCKED.value))


@dataclasses.dataclass(frozen=True)
class Reuse:
    """A saved plan a bare apply may apply, the file it was read from, and how old it is."""

    saved: SavedPlan
    path: Path
    minutes: int

    @property
    def writes(self) -> tuple[SavedChange, ...]:
        """Return the changes the saved plan writes, in its order."""
        return tuple(change for change in self.saved.changes if change.action not in _PASSING)

    @property
    def selectors(self) -> tuple[str, ...]:
        """Return a `<type>:<name>` selector for each change the saved plan writes."""
        return tuple(change.key for change in self.writes)


def saved_plan_path(project_dir: Path) -> Path:
    """Return where `sst plan` saves the plan a bare apply may reuse."""
    return target_dir(project_dir) / "plan.json"


def reusable_plan(request: PlanRequest, *, flagged: bool, now: datetime | None = None) -> Reuse | str:
    """Return the saved plan a bare apply may reuse, or why it may not, as the module says.

    Args:
        flagged: True when apply was given a flag that shapes the plan.

    Raises:
        ProjectError: the target cannot be resolved, or no compiled manifest exists (SST-MAN001).
    """
    if flagged:
        return "a flag shapes the plan"
    limit = config_int(config_block(project_config(request.paths).get("apply")).get("reuse_plan_minutes"))
    limit = REUSE_MINUTES if limit is None else limit
    if limit <= 0:
        return "apply.reuse_plan_minutes is 0"
    path = saved_plan_path(request.project_dir)
    try:
        saved = PlanFileStore(path).read() if path.is_file() else None
    except ProjectError:
        return "the saved plan cannot be read"
    if saved is None:
        return "no plan saved by sst plan"
    if saved.selected or saved.excluded or saved.include_prune or saved.partial or saved.overwrite:
        return "the saved plan was made with a selection or flags"
    target = project_inputs(request.paths, request.target_name, request.manifest_path).target().identity
    if saved.target.name != target.name:
        return f"the saved plan is for target '{saved.target.name}'"
    if saved.manifest_id != compiled_manifest(request.project_dir).manifest_id:
        return "the project was compiled since the plan was saved"
    minutes = _age_minutes(saved.observation_at, now or datetime.now(UTC))
    if minutes is None:
        return "the saved plan records no observation time"
    if minutes > limit:
        return f"the saved plan is {minutes}m old, past apply.reuse_plan_minutes ({limit}m)"
    return Reuse(saved, path, minutes)


def drifted(reuse: Reuse, fresh: ChangeSet) -> str | None:
    """Name the first saved write a fresh plan of those writes no longer makes; None when it makes each.

    Each write is compared by action, object, observed marker, fingerprint and statements; the
    fresh plan must write nothing else.
    """
    planned = {change.key: SavedChange.from_change(change) for change in fresh.changes}
    for change in reuse.writes:
        now = planned.pop(change.key, None)
        if now is None or _shape(now) != _shape(change):
            return f"{change.key} changed in Snowflake since sst plan"
    extra = sorted(key for key, change in planned.items() if change.action not in _PASSING)
    return f"{extra[0]} now needs a change the saved plan does not make" if extra else None


def _shape(change: SavedChange) -> tuple[object, ...]:
    return (change.action, change.target, change.previous_marker, change.fingerprint, change.statement_hashes)


def _age_minutes(observed_at: str, now: datetime) -> int | None:
    try:
        when = datetime.fromisoformat(observed_at)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0, int((now - when).total_seconds() // 60))
