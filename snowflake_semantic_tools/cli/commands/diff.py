"""`sst diff`: compare two states of the project's artifacts and explain the difference.

A state is `local`, the manifest `sst compile` wrote; a path ending in `.json`, a saved plan,
as what it would leave published; or any other name, a dbt target, as what that target holds
of SST's artifacts now. `--from` defaults to `local` and `--to` to the resolved target, so
`sst diff` alone compares the last compile with Snowflake; `--from dev --to prod` compares two
targets with no local state at all. Diff only reads: it validates nothing, produces no plan, and
works on a project that does not validate. Exit 2 when the states differ, 1 when one cannot be
read, 4 when a target is not declared, and 5 when one cannot be reached.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.local import PlanFileStore
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.snowflake.connector.pool import ConnectorPool
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.diff import (
    ArtifactStates,
    compiled_definitions,
    live_definitions,
    live_states,
    manifest_states,
    name_map,
    plan_states,
)
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.options import (
    no_detailed_exitcode_option,
    selection_options,
    target_option,
    threads_option,
)
from snowflake_semantic_tools.cli.output import render_policy
from snowflake_semantic_tools.cli.plan_output import member_json, write_plan_diff
from snowflake_semantic_tools.cli.render.report import member_diff_lines
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, project_path
from snowflake_semantic_tools.cli.settings import threads_setting
from snowflake_semantic_tools.cli.wiring.compile import compile_result, selection_scope
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest
from snowflake_semantic_tools.cli.wiring.plan import refuse_together
from snowflake_semantic_tools.cli.wiring.project import connect, open_connector
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.config_schema import config_block
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.plan.diff import (
    MODIFIED,
    NEW,
    ORPHANED,
    ArtifactState,
    Difference,
    compare_states,
    member_differences,
)
from snowflake_semantic_tools.domain.plan.member_diff import Definition, printable, summary, unified_text
from snowflake_semantic_tools.domain.plan.selectors import Selectable

LOCAL = "local"


@dataclasses.dataclass(frozen=True, slots=True)
class _Side:
    """One state as diff reads it: by fingerprint, and member by member where it could be read.

    Attributes:
        definitions: Each artifact's definition, physical names labelled; empty for a saved plan,
            and under `--no-diff`.
        unread: Why an artifact's definition was not read, by key.
    """

    states: ArtifactStates
    definitions: Mapping[str, Definition] = dataclasses.field(default_factory=dict)
    unread: Mapping[str, str] = dataclasses.field(default_factory=dict)


class _Unreadable(Exception):
    """A state that could not be read, with what says why."""

    def __init__(self, diagnostics: tuple[Diagnostic, ...]) -> None:
        super().__init__("a state could not be read")
        self.diagnostics = diagnostics


def _refuse_invocation(no_diff: bool, diff_out: Path | None) -> None:
    """Refuse `--diff-out` with `--no-diff`, which writes no member diff.

    Raises:
        SstUsageError: carrying SST-PRT104.
    """
    if diff_out is not None and no_diff:
        refuse_together("--diff-out", "--no-diff")


@click.command(cls=SstCommand)
@click.option("--from", "from_ref", default=LOCAL)
@click.option("--to", "to_ref")
@selection_options()
@target_option()
@click.option("--full", is_flag=True)
@click.option("--names-only", is_flag=True)
@click.option("--no-diff", is_flag=True)
@click.option("--diff-out", type=click.Path(dir_okay=False, path_type=Path))
@threads_option()
@no_detailed_exitcode_option()
@command_body("diff", refusals=_refuse_invocation)
def diff(
    paths: ProjectPaths,
    manifest_path: Path | None,
    from_ref: str,
    to_ref: str | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None,
    full: bool,
    names_only: bool,
    no_diff: bool,
    diff_out: Path | None,
    threads: int | None,
    no_detailed_exitcode: bool,
) -> CommandResult:
    """Compare two states -- local, a dbt target, or a saved plan -- and list what differs.

    A state is `local`, the manifest `sst compile` last wrote; a path ending in `.json`, read
    from the project directory, a saved plan, as what it would leave published; or any other
    name, a `profiles.yml` target, as what it holds of SST's artifacts now: each one its state
    table records whose object still carries SST's ownership marker. --from defaults to
    `local` and --to to the resolved target, so `sst diff` alone compares the last compile with
    Snowflake, and `--from dev --to prod` compares two targets. An artifact is `new` when only
    --to holds it, `orphaned` when only --from does, and `modified` when their fingerprints
    differ. Each modified artifact is then compared member by member, as `sst plan` compares an
    update: `local` as it compiles for the target it is compared with, a target as its objects
    hold it, on up to --threads sessions; --full shows each member's diff whole, and --diff-out
    writes them all as a unified diff. Comparing two targets, each one's physical names are first
    replaced by what they stand for -- an artifact's object by its key, a dbt model's relation by
    `ref('<model>')` -- and an artifact whose members then agree is not a difference, so two
    targets that publish the same project to different databases agree. A saved plan, an
    artifact whose definition cannot be read, and every artifact under --no-diff are compared by
    fingerprint alone. --select and --exclude keep the differences of the artifacts they choose.
    Diff only reads: it validates nothing, writes no plan, and works on a project that does not
    validate; only a target connects.

    Exit 0 when they agree, 2 when they differ, 1 when a state cannot be read, 4 when a target
    is not declared in `profiles.yml`, and 5 when a target cannot be reached.

    Diagnostics:
        SST-MAN001: `local` was asked for and `sst compile` has written no manifest.
        SST-PRT009: a saved plan named by path does not exist, or cannot be read.
        SST-MAN022: a target's state table exists and cannot be read.
    """
    to_ref = to_ref or load_profile_target(paths, target_name).target_name
    reading = _Reading(not no_diff, threads_setting(paths, threads), _compiled_for(from_ref, to_ref), manifest_path)
    try:
        before = _side(paths, from_ref, reading)
        after = _side(paths, to_ref, reading)
    except _Unreadable as exc:
        return CommandResult(ERROR, DiagnosticBag(exc.diagnostics), {"from": from_ref, "to": to_ref})
    across = _kind(from_ref) == _kind(to_ref) == "target" and from_ref != to_ref
    compared = member_differences(
        compare_states(before.states, after.states), before.definitions, after.definitions, across_targets=across
    )
    differences = _selected(paths, compared, (before.states, after.states), selected, excluded)
    unread = {**before.unread, **after.unread}
    exit_code = CHANGES if differences and not no_detailed_exitcode else OK
    data: dict[str, object] = {
        "from": _reference(from_ref),
        "to": _reference(to_ref),
        "differences": [_difference(item, full=full, unread=unread) for item in differences],
        "counts": {status: sum(item.status == status for item in differences) for status in (NEW, MODIFIED, ORPHANED)},
    }
    if diff_out is not None:
        sections = ((item.key, "", item.member_diff or ()) for item in differences)
        text = unified_text(sections, before_side=from_ref, after_side=to_ref) or "# no member differences\n"
        data["diff_path"] = str(write_plan_diff(paths.project_dir, project_path(paths, diff_out), text))
    return CommandResult(
        exit_code,
        data=data,
        human=lambda: _print(differences, from_ref, to_ref, full=full, names_only=names_only, unread=unread),
    )


@dataclasses.dataclass(frozen=True, slots=True)
class _Reading:
    """How diff reads each side: member by member or not, on how many sessions, `local` for which target.

    Attributes:
        manifest_path: The dbt manifest `--manifest` names, which each compile reads.
    """

    members: bool
    threads: int
    local_target: str | None
    manifest_path: Path | None = None


def _compiled_for(from_ref: str, to_ref: str) -> str | None:
    """The target `local` compiles for: the target it is compared with, else the profile's default."""
    return next((ref for ref in (to_ref, from_ref) if _kind(ref) == "target"), None)


def _kind(reference: str) -> str:
    """Name what kind of state a reference is: `local`, `plan`, or `target`."""
    if reference == LOCAL:
        return "local"
    return "plan" if reference.endswith(".json") else "target"


def _side(paths: ProjectPaths, reference: str, reading: _Reading) -> _Side:
    """Read the state `reference` names, and member by member unless `reading` says not to.

    A compile that fails leaves its side without definitions, compared by fingerprint alone.

    Raises:
        _Unreadable: the manifest, the saved plan, or the target's state could not be read.
        SnowflakePortError: a target could not be reached.
        ProjectError: a target is not declared in `profiles.yml`.
    """
    kind = _kind(reference)
    if kind == "target":
        return _target_side(paths, reference, reading)
    states = _states(paths, reference)
    if kind == "plan" or not reading.members:
        return _Side(states)
    result = _compiled(paths, reading.local_target, reading.manifest_path)
    if result is None:
        return _Side(states)
    return _Side(states, compiled_definitions(result, name_map(result)))


def _target_side(paths: ProjectPaths, reference: str, reading: _Reading) -> _Side:
    """What a target holds of SST's artifacts, by fingerprint and, unless --no-diff, member by member."""
    profile, port = connect(paths, reference)
    try:
        held, problems = live_states(port, profile.state_table, profile.target_name)
        if held is None:
            raise _Unreadable(problems)
        result = _compiled(paths, reference, reading.manifest_path) if reading.members else None
        if result is None:
            return _Side(held)
        apply = config_block(resolved_config(paths, reference).tree.get("apply"))
        stage = str(config_block(apply.get("eval_config_stage")).get("stage") or DEFAULT_EVAL_CONFIG_STAGE)
        params = profile.connection_params
        with ConnectorPool(reading.threads, lambda: open_connector(params)) as pool:
            definitions, unread = live_definitions(
                Fanout(port, pool, reading.threads),
                profile.state_table,
                profile.target_name,
                result,
                names=name_map(result),
                eval_stage=stage,
            )
        return _Side(held, definitions, unread)
    finally:
        port.close()


def _compiled(paths: ProjectPaths, target: str | None, manifest_path: Path | None) -> CompileResult | None:
    """The project as it compiles for `target`, in memory; None when it does not compile."""
    try:
        return compile_result(paths, target, manifest_path)
    except ProjectError:
        return None


def _states(paths: ProjectPaths, reference: str) -> ArtifactStates:
    """Read the state a `local` or saved plan reference names.

    Raises:
        _Unreadable: the manifest or the saved plan could not be read.
    """
    kind = _kind(reference)
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


def _recorded(state: ArtifactState | None) -> dict[str, object] | None:
    if state is None:
        return None
    return {"fingerprint": state.fingerprint, "target": state.target, "manifest_id": state.manifest_id}


def _difference(item: Difference, *, full: bool, unread: Mapping[str, str]) -> dict[str, object]:
    entry: dict[str, object] = {
        "key": item.key,
        "artifact_type": item.artifact_type,
        "name": item.name,
        "status": item.status,
        "from": _recorded(item.before),
        "to": _recorded(item.after),
        "diff_status": _diff_status(item, unread),
        "diff": [member_json(member) for member in item.member_diff or ()],
    }
    if item.key in unread:
        entry["diff_unavailable"] = unread[item.key]
    if full:
        entry["properties"] = list(item.properties)
    return entry


def _diff_status(item: Difference, unread: Mapping[str, str]) -> str:
    if item.member_diff is not None:
        return "compared"
    return "unavailable" if item.key in unread else "not_compared"


def _print(
    differences: tuple[Difference, ...],
    from_ref: str,
    to_ref: str,
    *,
    full: bool,
    names_only: bool,
    unread: Mapping[str, str],
) -> None:
    """Print each difference, or only its name, with what differs in its members, then a summary line."""
    policy = render_policy()
    style = Style(policy.terminal, stdout=True)
    color = True if policy.terminal.color_out else None
    for item in differences:
        if names_only:
            click.echo(item.name)
            continue
        line = f"{item.status:<9} {item.key}"
        if item.member_diff is not None:
            line += "  " + (", ".join(summary(item.member_diff)) or "no member-level difference")
        elif item.key in unread:
            line += f"  (not compared member by member: {printable(unread[item.key])})"
        click.echo(line)
        if item.member_diff:
            for text in member_diff_lines(item.member_diff, style, full=full, more="--full shows all"):
                click.echo(text, color=color)
        if full:
            for field in item.properties:
                click.echo(f"    {field}: {getattr(item.before, field)} -> {getattr(item.after, field)}")
    if names_only:
        return
    if differences:
        click.echo(f"{len(differences)} artifact(s) differ between {from_ref} and {to_ref}")
    else:
        click.echo(f"{from_ref} and {to_ref} agree")
