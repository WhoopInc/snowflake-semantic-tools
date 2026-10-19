"""The exit-code contract: every code each command documents, from a real invocation.

Exit 0 is OK, 1 an error, 2 a difference between declared and actual state, 3 a refused command
line, 4 a project that could not be resolved, 5 Snowflake unreachable or refusing (a credential, a
privilege, a deadline), and 130 an interrupt.
Every case below is one real run of `sst`; the table at the bottom says which codes each command
may return, and every case's code must be one of its command's.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import click
import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.exit_codes import EXIT_CODE_DOCS, INTERRUPTED
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics.signatures import SIGNATURES
from snowflake_semantic_tools.domain.model.lifecycle import ErrorKind, OwnershipMarker
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import (
    applied_port,
    common,
    compile_project,
    invoke_with_port,
    on_a_terminal,
    skills_only_project,
)
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.reference_project import REPO_ROOT, break_menu_view, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

# Which exit codes each command may return, besides 130: any command exits 130 when interrupted.
_DECLARED = {
    "init": {0, 1, 3, 4},
    "debug": {0, 1, 3, 4, 5},
    "enrich": {0, 1, 2, 3, 4, 5},
    "compile": {0, 1, 3, 4},
    "validate": {0, 1, 3, 4, 5},
    "plan": {0, 1, 2, 3, 4, 5},
    "apply": {0, 1, 3, 4, 5},
    "list": {0, 1, 3, 4},
    "lineage": {0, 1, 3, 4},
    "test": {0, 1, 3, 4, 5},
    "docs": {0, 1, 2, 3, 4},
    "clean": {0, 1, 3, 4},
    "migrate": {0, 1, 2, 3, 4},
    "explain": {0, 3},
    "drop": {0, 1, 3, 4, 5},
    "diff": {0, 1, 2, 3, 4, 5},
    "baseline": {0, 1, 3, 4},
    "format": {0, 1, 2, 3, 4},
    "agent-kit": {0, 1, 2, 3, 4},
    "state": {0, 1, 3, 4, 5},
    "ask": {0, 1, 3, 4, 5},
}
EXIT_CODES = {command: codes | {INTERRUPTED} for command, codes in _DECLARED.items()}
DROP = ("DB.S.V", "--type", "semantic_view", "--target", "dev", "--yes")
PROFILES_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project" / "profiles.yml"
Scenario = Callable[[Path, pytest.MonkeyPatch], Result]


def _run(*args: str) -> Result:
    return CliRunner().invoke(cli, list(args))


def _unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(params: object) -> FakeSnowflake:
        raise SnowflakePortError("unreachable", diagnostic=None)

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", refuse)


def _interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupt(params: object) -> FakeSnowflake:
        raise KeyboardInterrupt

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", interrupt)


def _dbt_project(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "dbt_project.yml").write_text("name: p\nprofile: p\n", encoding="utf-8")
    return root


def _yaml_project(root: Path, text: str) -> Path:
    (root / "sst_config.yml").write_text("project:\n  semantic_models_dir: semantic_models\n", encoding="utf-8")
    (root / "semantic_models").mkdir()
    (root / "semantic_models" / "views.yml").write_text(text, encoding="utf-8")
    return root


def _agent_kit_in_the_way(tmp: Path) -> Result:
    (tmp / ".claude" / "skills" / "sst-core").mkdir(parents=True)
    (tmp / ".claude" / "skills" / "sst-core" / "SKILL.md").write_text("mine\n", encoding="utf-8")
    return _run("agent-kit", "install", "--project-dir", str(tmp))


def _agent_kit_outdated(tmp: Path) -> Result:
    installed = _run("agent-kit", "install", "--agent", "cortex", "--project-dir", str(tmp))
    assert installed.exit_code == 0, installed.output
    lock = tmp / ".sst" / "agent-kit.json"
    document = json.loads(lock.read_text(encoding="utf-8"))
    document["agents"]["cortex"]["skills"]["sst-core"]["SKILL.md"] = "sha256:an-older-kit"
    lock.write_text(json.dumps(document), encoding="utf-8")
    return _run("agent-kit", "status", "--check", "--project-dir", str(tmp))


def _agent_kit_overlapping(tmp: Path) -> Result:
    (tmp / "sst_config.yml").write_text("project:\n  skills_dir: .cortex/skills\n", encoding="utf-8")
    return _run("agent-kit", "install", "--agent", "cortex", "--project-dir", str(tmp))


def _broken(tmp: Path) -> Path:
    project = project_copy(tmp)
    break_menu_view(project)
    return project


def _connected(tmp: Path, monkeypatch: pytest.MonkeyPatch, *args: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    _unreachable(monkeypatch)
    return _run(args[0], *common(project), *args[1:])


SCENARIOS: dict[tuple[str, int], Scenario] = {
    ("init", 0): lambda tmp, _: _run("init", "--project-dir", str(_dbt_project(tmp / "p")), "--skip-prompts"),
    ("init", 1): lambda tmp, _: _run("init", "--project-dir", str(_dbt_project(tmp / "p")), "--check-only"),
    ("init", 3): lambda tmp, _: _run("init", "-s"),
    ("init", 4): lambda tmp, _: _run("init", "--project-dir", str(tmp / "not-dbt")),
    ("agent-kit", 0): lambda tmp, _: _run("agent-kit", "install", "--agent", "claude", "--project-dir", str(tmp)),
    ("agent-kit", 1): lambda tmp, _: _agent_kit_in_the_way(tmp),
    ("agent-kit", 2): lambda tmp, _: _agent_kit_outdated(tmp),
    ("agent-kit", 3): lambda tmp, _: _run("agent-kit", "install", "--only", "sst-nope", "--project-dir", str(tmp)),
    ("agent-kit", 4): lambda tmp, _: _agent_kit_overlapping(tmp),
    ("debug", 1): lambda tmp, _: _debug_invalid(tmp),
    ("debug", 0): lambda tmp, _: _run("debug", "--project-dir", str(project_copy(tmp)), "--no-connect"),
    ("debug", 3): lambda tmp, _: _run("debug", "--select", "x"),
    ("debug", 4): lambda tmp, _: _run("debug", "--project-dir", str(tmp)),
    ("debug", 5): lambda tmp, mp: _debug_unreachable(tmp, mp),
    ("enrich", 3): lambda tmp, _: _run("enrich", "--select", "a,b"),
    ("enrich", 4): lambda tmp, _: _run("enrich", "--project-dir", str(tmp)),
    ("compile", 0): lambda tmp, _: _run("compile", *common(project_copy(tmp))),
    ("compile", 1): lambda tmp, _: _run("compile", *common(_broken(tmp))),
    ("compile", 3): lambda tmp, _: _run("compile", "--select", "a,b"),
    ("compile", 4): lambda tmp, _: _run("compile", "--project-dir", str(tmp)),
    ("validate", 0): lambda tmp, _: _run("validate", *common(project_copy(tmp))),
    ("validate", 1): lambda tmp, _: _run("validate", *common(project_copy(tmp)), "--strict"),
    ("validate", 3): lambda tmp, _: _run("validate", "--output", "csv"),
    ("validate", 4): lambda tmp, _: _run("validate", "--project-dir", str(tmp)),
    ("validate", 5): lambda tmp, mp: _connected(tmp, mp, "validate", "--snowflake-syntax-check"),
    ("plan", 0): lambda tmp, mp: _plan(tmp, mp, "--no-detailed-exitcode"),
    ("plan", 1): lambda tmp, mp: _plan_broken(tmp, mp),
    ("plan", 2): lambda tmp, mp: _plan(tmp, mp),
    ("plan", 3): lambda tmp, _: _run("plan", "--plan-out", "p.json", "--no-plan-out"),
    ("plan", 4): lambda tmp, _: _run("plan", *common(project_copy(tmp))),
    ("plan", 5): lambda tmp, mp: _connected(tmp, mp, "plan"),
    ("plan", 130): lambda tmp, mp: _interrupted(tmp, mp, "plan"),
    ("apply", 0): lambda tmp, mp: _apply_nothing(tmp, mp),
    ("apply", 1): lambda tmp, mp: _apply_broken(tmp, mp),
    ("apply", 3): lambda tmp, _: _run("apply", "--prune"),
    ("apply", 4): lambda tmp, _: _run("apply", *common(project_copy(tmp)), "--yes"),
    ("apply", 5): lambda tmp, mp: _connected(tmp, mp, "apply", "--yes"),
    ("apply", 130): lambda tmp, mp: _interrupted(tmp, mp, "apply", "--yes"),
    ("list", 0): lambda tmp, _: _listed(tmp),
    ("list", 3): lambda tmp, _: _run("list", "not-a-type"),
    ("list", 4): lambda tmp, _: _run("list", "--project-dir", str(project_copy(tmp))),
    ("lineage", 0): lambda tmp, _: _traced(tmp, "model:orders"),
    ("lineage", 1): lambda tmp, _: _traced(tmp, "model:orders", "--no-manifest", broken=True),
    ("lineage", 3): lambda tmp, _: _traced(tmp, "model:nope"),
    ("lineage", 4): lambda tmp, _: _run("lineage", "--project-dir", str(project_copy(tmp)), "model:orders"),
    ("test", 0): lambda tmp, mp: _test_smoke(tmp, mp, applied=True),
    ("test", 1): lambda tmp, mp: _test_smoke(tmp, mp, applied=False),
    ("test", 3): lambda tmp, _: _run("test", "--suite", "every"),
    ("test", 4): lambda tmp, _: _run("test", *common(project_copy(tmp))),
    ("test", 5): lambda tmp, mp: _connected(tmp, mp, "test", "--suite", "smoke"),
    ("ask", 0): lambda tmp, mp: _ask(tmp, mp),
    ("ask", 1): lambda tmp, mp: _ask(tmp, mp, reply=AgentReply(error="refused")),
    ("ask", 3): lambda tmp, _: _run("ask", "jaffle_analytics_agent", *common(project_copy(tmp))),
    ("ask", 4): lambda tmp, mp: _ask(tmp, mp, agent="finance"),
    ("ask", 5): lambda tmp, mp: _connected(tmp, mp, "ask", "jaffle_analytics_agent", "q"),
    ("docs", 0): lambda tmp, _: _run("docs", "--project-dir", str(REPO_ROOT), "--check"),
    ("docs", 1): lambda tmp, _: _docs_unwritable(tmp),
    ("docs", 2): lambda tmp, _: _run("docs", "--project-dir", str(tmp), "--check"),
    ("docs", 3): lambda tmp, _: _run("docs", "--only", "everything"),
    ("clean", 0): lambda tmp, _: _run("clean", "--project-dir", str(project_copy(tmp))),
    ("clean", 1): lambda tmp, mp: _clean_refused(tmp, mp),
    ("clean", 3): lambda tmp, _: _run("clean", "--force"),
    ("clean", 4): lambda tmp, _: _run("clean", "--project-dir", str(tmp)),
    ("baseline", 0): lambda tmp, _: _run("baseline", "show", *common(project_copy(tmp))),
    ("baseline", 1): lambda tmp, _: _run("baseline", "add", "SST-VAL116", *common(project_copy(tmp))),
    ("baseline", 3): lambda tmp, _: _run("baseline", "renew"),
    ("baseline", 4): lambda tmp, _: _run("baseline", "add", "SST-CFG018", "--project-dir", str(tmp)),
    ("diff", 0): lambda tmp, mp: _diff(tmp, mp, "--from", "dev", "--to", "prod"),
    ("diff", 1): lambda tmp, _: _run("diff", *common(project_copy(tmp)), "--to", "saved.json"),
    ("diff", 2): lambda tmp, mp: _diff(tmp, mp),
    ("diff", 3): lambda tmp, _: _run("diff", "--select", "a,b"),
    ("diff", 4): lambda tmp, _: _run("diff", "--project-dir", str(tmp)),
    ("diff", 5): lambda tmp, mp: _connected(tmp, mp, "diff"),
    ("drop", 0): lambda tmp, mp: _drop(tmp, mp, exists=True),
    ("drop", 1): lambda tmp, mp: _drop(tmp, mp, exists=False),
    ("drop", 3): lambda tmp, _: _run("drop", "DB.S.V", "--type", "semantic_view", "--target", "dev"),
    ("drop", 4): lambda tmp, _: _run("drop", *DROP, "--project-dir", str(tmp)),
    ("drop", 5): lambda tmp, mp: _drop_without_snowflake(tmp, mp, _unreachable),
    ("state", 0): lambda tmp, mp: _state_convert(tmp, mp, "hybrid"),
    ("state", 1): lambda tmp, mp: _state_convert(tmp, mp, "standard"),
    # Off a terminal, a command that would ask refuses without --yes.
    ("state", 3): lambda tmp, _: _run("state", "recover", *common(project_copy(tmp))),
    ("state", 4): lambda tmp, _: _run("state", "recover", "--yes", "--project-dir", str(tmp / "not-dbt")),
    ("state", 5): lambda tmp, mp: _state_unreachable(tmp, mp),
    ("drop", 130): lambda tmp, mp: _drop_without_snowflake(tmp, mp, _interrupt),
    ("explain", 0): lambda tmp, _: _run("explain", "SST-VAL009", "--project-dir", str(tmp)),
    ("explain", 3): lambda tmp, _: _run("explain", "SST-NOPE01"),
    ("format", 0): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a: 1\n"))),
    ("format", 1): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a: [1,\n"))),
    ("format", 2): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a:   1\n")), "--check"),
    ("format", 3): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a: 1\n")), "nothing.yml"),
    ("format", 4): lambda tmp, _: _run("format", "--project-dir", str(tmp)),
    ("migrate", 0): lambda tmp, _: _run("migrate", "refs", "--project-dir", str(project_copy(tmp))),
    ("migrate", 3): lambda tmp, _: _run("migrate", "refs", "--bogus"),
    ("migrate", 4): lambda tmp, _: _run("migrate", "refs", "--project-dir", str(tmp)),
}

# Flags that change how a command exits, each by a real run: (command, code, scenario) by id.
FLAG_SCENARIOS: dict[str, tuple[str, int, Scenario]] = {
    "plan-no-grants": ("plan", 2, lambda tmp, mp: _plan(tmp, mp, "--no-grants")),
    "plan-grants": ("plan", 2, lambda tmp, mp: _plan(tmp, mp, "--grants")),
    "plan-capture-prior": ("plan", 2, lambda tmp, mp: _plan(tmp, mp, "--capture-prior")),
    "plan-use-cached-state": ("plan", 2, lambda tmp, mp: _plan_cached(tmp, mp, recorded=True)),
    "plan-use-cached-state-unrecorded": ("plan", 4, lambda tmp, mp: _plan_cached(tmp, mp, recorded=False)),
    "plan-use-cached-state-without-state": ("plan", 3, lambda tmp, _: _run("plan", "--use-cached-state")),
    "plan-full": ("plan", 2, lambda tmp, mp: _plan(tmp, mp, "--full")),
    "plan-names-only": ("plan", 2, lambda tmp, mp: _plan(tmp, mp, "--names-only")),
    "plan-names-only-in-sync": ("plan", 0, lambda tmp, mp: _plan(tmp, mp, "--names-only", "--no-detailed-exitcode")),
    "plan-no-validate": ("plan", 2, lambda tmp, mp: _plan(tmp, mp, "--no-validate")),
    "plan-no-validate-broken": ("plan", 1, lambda tmp, mp: _plan_broken(tmp, mp, "--no-validate")),
    "plan-no-validate-syntax-check": (
        "plan",
        3,
        lambda tmp, _: _run("plan", "--no-validate", "--snowflake-syntax-check"),
    ),
    "apply-no-validate": ("apply", 0, lambda tmp, mp: _apply_nothing(tmp, mp, "--no-validate")),
    "apply-no-validate-broken": ("apply", 1, lambda tmp, mp: _apply_broken(tmp, mp, "--no-validate")),
    "apply-no-validate-syntax-check": (
        "apply",
        3,
        lambda tmp, _: _run("apply", "--yes", "--no-validate", "--snowflake-syntax-check"),
    ),
    "test-select": ("test", 0, lambda tmp, mp: _test_smoke(tmp, mp, "--select", "jaffle_minimal", applied=True)),
    "test-exclude": ("test", 0, lambda tmp, mp: _test_smoke(tmp, mp, "--exclude", "type:agent", applied=True)),
    "test-select-matches-nothing": ("test", 4, lambda tmp, _: _test_compiled(tmp, "--select", "nothing")),
    "test-select-no-suite-applies": ("test", 4, lambda tmp, _: _test_compiled(tmp, "--select", "type:tool")),
    # An unpublished eval run needs no compiled manifest, and fails when the agent picks the wrong tools.
    "test-unpublished": ("test", 1, lambda tmp, mp: _test_unpublished(tmp, mp)),
    "test-unpublished-capture": (
        "test",
        3,
        lambda tmp, _: _run("test", "--unpublished", "--capture-baseline", "--reason", "why"),
    ),
    "enrich-not-production": ("enrich", 3, lambda tmp, _: _run("enrich", *common(project_copy(tmp)))),
    "enrich-allow-non-prod": ("enrich", 0, lambda tmp, mp: _enrich_allowed(tmp, mp)),
}


class _EnrichSession(ScriptedEnrich):
    """A scripted warehouse that answers what connecting asks of a session."""

    def current_role(self) -> str:
        return "ANALYST"

    def current_account_locator(self) -> str:
        return "ACCOUNT"

    def close(self) -> None:
        return None


def _enrich_allowed(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    port = _EnrichSession(columns={"SST_REF_DEV.JAFFLE.ORDER_ITEMS": [("ORDER_ITEM_ID", "TEXT")]})
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    return _run("enrich", *common(project_copy(tmp)), "--select", "order_items", "--allow-non-prod")


def _test_compiled(tmp: Path, *flags: str) -> Result:
    """Run `sst test`, naming no suite, on a compiled copy of the reference project."""
    project = project_copy(tmp)
    compile_project(project)
    return _run("test", *common(project), *flags)


def _test_smoke(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str, applied: bool) -> Result:
    """Run the smoke suite on a compiled copy, every artifact applied or none."""
    project = project_copy(tmp)
    compile_project(project)
    port = applied_port(project) if applied else FakeSnowflake(state={})
    return invoke_with_port(
        monkeypatch, port, ["test", *common(project), "--target", "dev", "--suite", "smoke", *flags]
    )


def _test_unpublished(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    """Run the eval suite unpublished on an uncompiled copy, every question answered with no tool."""
    args = ["test", *common(project_copy(tmp)), "--target", "dev", "--suite", "evals", "--unpublished"]
    return invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)


def _debug_invalid(tmp: Path) -> Result:
    project = project_copy(tmp)
    config = project / "sst_config.yml"
    block = "diagnostics:\n  severity_overrides:\n"
    config.write_text(config.read_text(encoding="utf-8").replace(block, f"{block}    SST-REG001: warning\n", 1))
    return _run("debug", "--project-dir", str(project), "--no-connect")


def _debug_unreachable(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    _unreachable(monkeypatch)
    return _run("debug", "--project-dir", str(project))


def _plan(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = project_copy(tmp)
    return invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), *flags])


def _plan_cached(tmp: Path, monkeypatch: pytest.MonkeyPatch, *, recorded: bool) -> Result:
    """Plan one view from the observation a live plan recorded, or from a `--state` that holds none, offline."""
    project = project_copy(tmp)
    compile_project(project)
    view = ("--select", "jaffle_minimal", "--no-plan-out")
    state = target_dir(project)
    if recorded:
        invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), *view])
    else:
        state = tmp / "previous"
        state.mkdir()
    _unreachable(monkeypatch)
    return _run("plan", *common(project), "--use-cached-state", "--state", str(state), *view)


def _diff(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    return invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["diff", *common(project), *flags])


def _state_convert(tmp: Path, monkeypatch: pytest.MonkeyPatch, to_kind: str) -> Result:
    project = project_copy(tmp)
    return invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "convert", *common(project), "--to", to_kind, "--yes"]
    )


def _state_unreachable(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    _unreachable(monkeypatch)
    return _run("state", "convert", *common(project), "--to", "hybrid", "--yes")


def _drop_project(tmp: Path) -> Path:
    project = _dbt_project(tmp / "p")
    (project / "profiles.yml").write_text(PROFILES_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    (project / "dbt_project.yml").write_text("name: p\nprofile: sst_reference_impl\n", encoding="utf-8")
    return project


def _drop(tmp: Path, monkeypatch: pytest.MonkeyPatch, *, exists: bool) -> Result:
    marker = OwnershipMarker("a" * 64, "b" * 64)
    port = FakeSnowflake(existing=("DB.S.V",) if exists else (), markers={"DB.S.V": marker})
    return invoke_with_port(monkeypatch, port, ["drop", *DROP, "--project-dir", str(_drop_project(tmp))])


def _drop_without_snowflake(
    tmp: Path, monkeypatch: pytest.MonkeyPatch, broken: Callable[[pytest.MonkeyPatch], None]
) -> Result:
    project = _drop_project(tmp)
    broken(monkeypatch)
    return _run("drop", *DROP, "--project-dir", str(project))


def _plan_broken(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    break_menu_view(project)
    return invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), *flags])


def _apply_nothing(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = skills_only_project(tmp / "skills")
    assert _run("compile", "--project-dir", str(project)).exit_code == 0
    return invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["apply", "--project-dir", str(project), "--yes", *flags]
    )


def _apply_broken(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    break_menu_view(project)
    return invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["apply", *common(project), "--yes", *flags])


def _ask(
    tmp: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    agent: str = "jaffle_analytics_agent",
    reply: AgentReply | None = None,
) -> Result:
    port = FakeSnowflake(state={})
    port.agent_replies = {"q": reply} if reply is not None else None
    return invoke_with_port(monkeypatch, port, ["ask", agent, "q", *common(project_copy(tmp)), "--target", "dev"])


def _interrupted(tmp: Path, monkeypatch: pytest.MonkeyPatch, *args: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    _interrupt(monkeypatch)
    return _run(args[0], *common(project), *args[1:])


def _listed(tmp: Path) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    return _run("list", "--project-dir", str(project), "semantic-views", "--long", "--exclude", "jaffle_menu")


def _traced(tmp: Path, *args: str, broken: bool = False) -> Result:
    """Trace one node of the reference project, compiled, or with `broken` one view that fails to."""
    project = project_copy(tmp)
    compile_project(project)
    if broken:
        break_menu_view(project)
    return _run("lineage", *common(project), *args)


def _docs_unwritable(tmp: Path) -> Result:
    (tmp / "docs").write_text("a file where the docs directory goes", encoding="utf-8")
    return _run("docs", "--project-dir", str(tmp))


def _clean_refused(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    (project / "sst_target").mkdir(parents=True)

    def refuse(path: Path) -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr("snowflake_semantic_tools.adapters.paths.shutil.rmtree", refuse)
    return _run("clean", "--project-dir", str(project))


@pytest.mark.parametrize(("command", "code"), sorted(SCENARIOS), ids=[f"{c}-{n}" for c, n in sorted(SCENARIOS)])
def test_each_documented_exit_code_is_returned_by_a_real_invocation(
    command: str, code: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = SCENARIOS[(command, code)](tmp_path, monkeypatch)
    assert result.exit_code == code, result.output
    assert code in EXIT_CODES[command]


@pytest.mark.parametrize("scenario", sorted(FLAG_SCENARIOS))
def test_each_flag_exits_with_a_code_its_command_documents(
    scenario: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    command, code, run = FLAG_SCENARIOS[scenario]
    result = run(tmp_path, monkeypatch)
    assert result.exit_code == code, result.output
    assert code in EXIT_CODES[command]


def test_every_command_is_in_the_exit_code_table() -> None:
    assert set(EXIT_CODES) == set(cli.commands)


# A command line each body is reached by: a group's subcommand, and a command's required arguments.
_INVOCATIONS = {
    "explain": ("SST-VAL009",),
    "drop": DROP,
    "convert": ("--to", "hybrid"),
    "ask": ("agent", "q"),
    "lineage": ("model:orders",),
}


def _interruptible(name: str, command: click.Command) -> list[list[str]]:
    if isinstance(command, click.Group):
        return [[name, *line] for sub, child in sorted(command.commands.items()) for line in _interruptible(sub, child)]
    return [[name, *_INVOCATIONS.get(name, ())]]


@pytest.mark.parametrize("command", sorted(cli.commands))
@pytest.mark.parametrize(
    "raised", [KeyboardInterrupt, EOFError, click.exceptions.Abort], ids=["ctrl-c", "end-of-input", "abort"]
)
def test_every_command_exits_130_when_its_body_is_interrupted(
    command: str, raised: type[BaseException], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupt(self: object) -> None:
        raise raised

    monkeypatch.setattr("snowflake_semantic_tools.cli.runner._Run.execute", interrupt)
    for line in _interruptible(command, cli.commands[command]):
        result = _run(*line, "--project-dir", str(tmp_path))
        assert result.exit_code == INTERRUPTED, (line, result.output)
    assert INTERRUPTED in EXIT_CODES[command]


# The table's other 130 triggers, met as a user meets them: the confirmation `apply` asks is
# declined, or input ends before it is answered. SIGTERM and SIGHUP reach a real `sst` process
# in test_signals.py.
@pytest.mark.parametrize(("trigger", "answer"), [("a declined prompt", "n\n"), ("end of input", "")])
def test_a_declined_or_unanswered_confirmation_exits_130_as_the_table_documents(
    trigger: str, answer: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert trigger in {code: text for code, _, text in EXIT_CODE_DOCS}[INTERRUPTED]
    on_a_terminal(monkeypatch)
    args = ["apply", *common(project_copy(tmp_path)), "--target", "dev"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args, input=answer)
    assert result.exit_code == INTERRUPTED, result.output
    assert "Apply this plan?" in result.output and "SST-INT001" not in result.output


def test_a_refused_privilege_exits_5_under_a_privilege_code_as_the_table_documents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    def refuse(params: object) -> FakeSnowflake:
        raise SnowflakePortError("Insufficient privileges to operate on schema", sqlstate="42501")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", refuse)
    result = _run("plan", *common(project), "--output", "json")
    assert result.exit_code == 5, result.output
    codes = [item["code"] for item in json.loads(result.output)["diagnostics"]]
    privileges = {row.code for row in SIGNATURES if row.kind is ErrorKind.PRIVILEGE}
    assert len(codes) == 1 and codes[0] in privileges
    assert "privilege" in {code: text for code, _, text in EXIT_CODE_DOCS}[5]
