"""`sst diff`: compare two states of the project's artifacts and explain the difference.

A state is `local`, the manifest `sst compile` wrote; a path ending in `.json`, a saved plan,
as what it would leave published; or any other name, a dbt target, as what that target holds
of SST's artifacts now. `--from` defaults to `local` and `--to` to the resolved target, so
`sst diff` alone compares the working tree with Snowflake; `--from dev --to prod` compares two
targets with no local state at all. Diff only reads: it validates nothing, produces no plan, and
works on a project that does not validate. Exit 2 when the states differ, 1 when one cannot be
read.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.diff import ArtifactStates, live_states, manifest_states, plan_states
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.options import no_detailed_exitcode_option, selection_options, target_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, project_path
from snowflake_semantic_tools.cli.wiring.compile import selection_scope
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.cli.wiring.project import connect
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.plan.diff import MODIFIED, NEW, ORPHANED, ArtifactState, Difference, compare_states
from snowflake_semantic_tools.domain.plan.selectors import Selectable

LOCAL = "local"


class _Unreadable(Exception):
    """A state that could not be read, with what says why."""

    def __init__(self, diagnostics: tuple[Diagnostic, ...]) -> None:
        super().__init__("a state could not be read")
        self.diagnostics = diagnostics


@click.command()
@click.option("--from", "from_ref", default=LOCAL)
@click.option("--to", "to_ref")
@selection_options()
@target_option()
@click.option("--full", is_flag=True)
@click.option("--names-only", is_flag=True)
@no_detailed_exitcode_option()
@command_body("diff")
def diff(
    paths: ProjectPaths,
    from_ref: str,
    to_ref: str | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None,
    full: bool,
    names_only: bool,
    no_detailed_exitcode: bool,
) -> CommandResult:
    """Compare two states -- local, a dbt target, or a saved plan -- and list what differs.

    Exit 0 when they agree, 2 when they differ, 1 when a state cannot be read, and 5 when a
    target cannot be reached.

    Diagnostics:
        SST-MAN001: `local` was asked for and `sst compile` has written no manifest.
        SST-PRT009: a saved plan named by path does not exist, or cannot be read.
        SST-MAN022: a target's state table exists and cannot be read.
    """
    to_ref = to_ref or load_profile_target(paths, target_name).target_name
    try:
        before = _states(paths, from_ref)
        after = _states(paths, to_ref)
    except _Unreadable as exc:
        return CommandResult(ERROR, DiagnosticBag(exc.diagnostics), {"from": from_ref, "to": to_ref})
    differences = _selected(paths, compare_states(before, after), (before, after), selected, excluded)
    exit_code = CHANGES if differences and not no_detailed_exitcode else OK
    data = {
        "from": _reference(from_ref),
        "to": _reference(to_ref),
        "differences": [_difference(item, full=full) for item in differences],
        "counts": {status: sum(item.status == status for item in differences) for status in (NEW, MODIFIED, ORPHANED)},
    }
    return CommandResult(
        exit_code,
        data=data,
        human=lambda: _print(differences, from_ref, to_ref, full=full, names_only=names_only),
    )


def _kind(reference: str) -> str:
    """Name what kind of state a reference is: `local`, `plan`, or `target`."""
    if reference == LOCAL:
        return "local"
    return "plan" if reference.endswith(".json") else "target"


def _states(paths: ProjectPaths, reference: str) -> ArtifactStates:
    """Read the state `reference` names.

    Raises:
        _Unreadable: the manifest, the saved plan, or the target's state could not be read.
        SnowflakePortError: a target could not be reached.
        ProjectError: a target is not declared in `profiles.yml`.
    """
    kind = _kind(reference)
    if kind == "target":
        profile, port = connect(paths, reference)
        try:
            held, problems = live_states(port, profile.state_table, profile.target_name)
        finally:
            port.close()
        if held is None:
            raise _Unreadable(problems)
        return held
    try:
        if kind == "local":
            return manifest_states(compiled_manifest(paths.project_dir))
        plan = PlanFileStore(project_path(paths, Path(reference))).read()
    except ProjectError as exc:
        raise _Unreadable(tuple(exc.diagnostics)) from exc
    except (OSError, ValueError) as exc:
        raise _Unreadable((D("SST-PRT009", subject="cli", path=reference, detail=str(exc)),)) from exc
    if plan is None:
        raise _Unreadable((D("SST-PRT009", subject="cli", path=reference, detail="there is no saved plan"),))
    return plan_states(plan)


def _selected(
    paths: ProjectPaths,
    differences: tuple[Difference, ...],
    states: tuple[ArtifactStates, ArtifactStates],
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
) -> tuple[Difference, ...]:
    """Keep the differences the selectors choose, resolved against every artifact either state holds."""
    if not (selected or excluded):
        return differences
    scope = selection_scope(selected, excluded, _universe(paths, states))
    return tuple(item for item in differences if scope.covers(split_artifact_key(item.key)[0], item.key))


def _universe(paths: ProjectPaths, states: tuple[ArtifactStates, ArtifactStates]) -> tuple[Selectable, ...]:
    """Every artifact either state holds, with its source files when the compiled manifest has them."""
    try:
        sources = {key: entry.source_files for key, entry in compiled_manifest(paths.project_dir).artifacts.items()}
    except ProjectError:
        sources = {}
    keys = sorted({key for state in states for key in state})
    return tuple(
        Selectable(
            key,
            split_artifact_key(key)[0],
            split_artifact_key(key)[1].casefold(),
            _fingerprint(states, key),
            sources.get(key, ()),
        )
        for key in keys
    )


def _fingerprint(states: tuple[ArtifactStates, ArtifactStates], key: str) -> str:
    found = next((state[key] for state in states if key in state), None)
    return (found.fingerprint or "") if found is not None else ""


def _reference(reference: str) -> dict[str, str]:
    return {"ref": reference, "kind": _kind(reference)}


def _side(state: ArtifactState | None) -> dict[str, object] | None:
    if state is None:
        return None
    return {"fingerprint": state.fingerprint, "target": state.target, "manifest_id": state.manifest_id}


def _difference(item: Difference, *, full: bool) -> dict[str, object]:
    entry: dict[str, object] = {
        "key": item.key,
        "artifact_type": item.artifact_type,
        "name": item.name,
        "status": item.status,
        "from": _side(item.before),
        "to": _side(item.after),
    }
    if full:
        entry["properties"] = list(item.properties)
    return entry


def _print(differences: tuple[Difference, ...], from_ref: str, to_ref: str, *, full: bool, names_only: bool) -> None:
    """Print each difference, or only its name, then a summary line."""
    for item in differences:
        if names_only:
            click.echo(item.name)
            continue
        click.echo(f"{item.status:<9} {item.key}")
        if full:
            for field in item.properties:
                click.echo(f"    {field}: {getattr(item.before, field)} -> {getattr(item.after, field)}")
    if names_only:
        return
    if differences:
        click.echo(f"{len(differences)} artifact(s) differ between {from_ref} and {to_ref}")
    else:
        click.echo(f"{from_ref} and {to_ref} agree")
